'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const net = require('node:net');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');

const { loadConfig } = require('../src/config');
const { FrameDecoder, encodeFrame, validateOperation } = require('../src/protocol');
const { prepareSocketPath, unlinkIfSameSocket } = require('../src/socket-path');
const { VerifierServer } = require('../src/server');
const { RandomXEngine } = require('../src/engine');
const { SeedManager } = require('../src/seed-manager');
const { VerifyScheduler } = require('../src/scheduler');
const { MockEngine, mockHash } = require('./mock-engine');

const SEED_A = '11'.repeat(32);
const SEED_B = '22'.repeat(32);
const SEED_C = '33'.repeat(32);
const SEED_D = '44'.repeat(32);
const BLOB = 'a1b2c3d4';

test('frame decoder accepts fragmented/coalesced frames and enforces 16 KiB', () => {
    const decoder = new FrameDecoder(16 * 1024, 64 * 1024);
    const first = encodeFrame({ v: 1, id: 1, op: 'ping' }, 16 * 1024);
    const second = encodeFrame({ v: 1, id: 2, op: 'stats' }, 16 * 1024);
    assert.equal(decoder.hasProcessableFrame(), false);
    decoder.append(first.subarray(0, 3));
    assert.equal(decoder.hasProcessableFrame(), false);
    assert.equal(decoder.next(), null);
    decoder.append(Buffer.concat([first.subarray(3), second]));
    assert.equal(decoder.hasProcessableFrame(), true);
    assert.deepEqual(decoder.next(), { v: 1, id: 1, op: 'ping' });
    assert.equal(decoder.hasProcessableFrame(), true);
    assert.deepEqual(decoder.next(), { v: 1, id: 2, op: 'stats' });
    assert.equal(decoder.hasProcessableFrame(), false);
    assert.equal(decoder.next(), null);

    const bad = Buffer.alloc(4);
    bad.writeUInt32BE(16 * 1024 + 1);
    decoder.append(bad);
    assert.equal(decoder.hasProcessableFrame(), true);
    assert.throws(() => decoder.next(), /frame length/);
});

test('frame decoder rejects malformed UTF-8 and JSON', () => {
    const malformedUtf8 = Buffer.from([0xff]);
    const utf8Frame = Buffer.alloc(5);
    utf8Frame.writeUInt32BE(1, 0);
    malformedUtf8.copy(utf8Frame, 4);
    const utf8Decoder = new FrameDecoder(16384, 65536);
    utf8Decoder.append(utf8Frame);
    assert.throws(() => utf8Decoder.next(), { code: 'INVALID_UTF8' });

    const jsonBody = Buffer.from('{', 'utf8');
    const jsonFrame = Buffer.alloc(4 + jsonBody.length);
    jsonFrame.writeUInt32BE(jsonBody.length, 0);
    jsonBody.copy(jsonFrame, 4);
    const jsonDecoder = new FrameDecoder(16384, 65536);
    jsonDecoder.append(jsonFrame);
    assert.throws(() => jsonDecoder.next(), { code: 'INVALID_JSON' });
});

test('optional protocol fields reject explicit null instead of bypassing exact values', () => {
    assert.throws(
        () => validateOperation({ v: 1, id: 1, op: 'hello', client: null }, 4096),
        { code: 'INVALID_CLIENT' }
    );
    assert.throws(
        () => validateOperation({ v: 1, id: 2, op: 'prepare_seed', seed_hash: SEED_A, mode: null }, 4096),
        { code: 'INVALID_MODE' }
    );
    assert.throws(
        () => validateOperation({
            v: 1,
            id: 3,
            op: 'prepare_seed',
            seed_hash: SEED_A,
            allow_light_fallback: null
        }, 4096),
        { code: 'LIGHT_FALLBACK_FORBIDDEN' }
    );
});

test('socket preparation refuses an existing non-socket', async () => {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'xrv-path-'));
    const socketPath = path.join(directory, 'verifier.sock');
    await fs.writeFile(socketPath, 'do not replace');
    await assert.rejects(prepareSocketPath(socketPath), /refusing to replace non-socket/);
    await fs.rm(directory, { recursive: true, force: true });
});

test('inode cleanup never unlinks a replacement regular file', async () => {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'xrv-clean-'));
    const socketPath = path.join(directory, 'verifier.sock');
    await fs.writeFile(socketPath, 'replacement');
    const stat = await fs.lstat(socketPath);
    assert.equal(await unlinkIfSameSocket(socketPath, { dev: stat.dev, ino: stat.ino }), false);
    assert.equal(await fs.readFile(socketPath, 'utf8'), 'replacement');
    await fs.rm(directory, { recursive: true, force: true });
});

test('engine adapter uses only prepared raw hashAsync contexts', async () => {
    const seed = Buffer.from(SEED_A, 'hex');
    const blob = Buffer.from(BLOB, 'hex');
    let present = false;
    let hashAsyncCalls = 0;
    let options = null;
    const pool = {
        warmSeedAsync: async () => { present = true; },
        has: () => present,
        hashAsync: async () => { hashAsyncCalls += 1; return Buffer.alloc(32, 7); },
        release: () => { present = false; return true; },
        releaseAll: () => { present = false; },
        getSnapshot: () => ({ size: present ? 1 : 0 })
    };
    const fakeModule = {
        createPoolSeedPool: (value) => { options = value; return pool; },
        verifyShare: () => { throw new Error('must not be called'); },
        getStats: () => ({}),
        getHardwareInfo: () => ({})
    };
    const engine = new RandomXEngine(loadConfig({ VERIFIER_SOCKET_PATH: '/run/x.sock' }), fakeModule);
    await assert.rejects(engine.hash(seed, blob), { code: 'SEED_NOT_READY' });
    await engine.prepare(seed);
    assert.deepEqual(await engine.hash(seed, blob), Buffer.alloc(32, 7));
    assert.equal(hashAsyncCalls, 1);
    assert.equal(options.mode, 'fast');
    assert.equal(options.vmPoolSize, 4);
    assert.equal(options.threads, 4);
    assert.equal(options.maxSeeds, 3);
    assert.equal(options.enableHugePages, false);
    engine.close();
});

test('seed manager serializes warmups, enforces capacity, and prevents cold hashing', async () => {
    const engine = new MockEngine();
    engine.prepareDelayMs = 5;
    const seeds = new SeedManager(engine, 3);
    const preparing = [SEED_A, SEED_B, SEED_C].map((seed) => seeds.prepare(seed));
    await assert.rejects(seeds.prepare(SEED_D), { code: 'SEED_CAPACITY' });
    await Promise.all(preparing);
    assert.equal(engine.maxActivePrepares, 1);
    assert.equal(engine.prepareCalls, 3);

    const lease = seeds.acquire(SEED_A);
    const release = seeds.release(SEED_A);
    await assert.rejects(Promise.resolve().then(() => seeds.acquire(SEED_A)), { code: 'SEED_NOT_READY' });
    assert.equal(engine.releaseCalls, 0);
    lease.release();
    assert.equal(await release, true);
    assert.equal(engine.releaseCalls, 1);
    await assert.rejects(Promise.resolve().then(() => seeds.acquire(SEED_A)), { code: 'SEED_NOT_READY' });
    await seeds.close();
});

test('scheduler bounds active work and queued work', async () => {
    const scheduler = new VerifyScheduler(2, 1);
    const first = deferred();
    const second = deferred();
    const third = deferred();
    let active = 0;
    let maxActive = 0;
    const run = (gate) => scheduler.enqueue({}, async () => {
        active += 1;
        maxActive = Math.max(maxActive, active);
        await gate.promise;
        active -= 1;
    });
    const a = run(first);
    const b = run(second);
    await waitUntil(() => active === 2);
    const c = run(third);
    await waitUntil(() => scheduler.snapshot().queued === 1);
    await assert.rejects(run(deferred()), { code: 'QUEUE_FULL' });
    first.resolve();
    second.resolve();
    await waitUntil(() => active > 0 && scheduler.snapshot().queued === 0);
    third.resolve();
    await Promise.all([a, b, c]);
    assert.equal(maxActive, 2);
    await scheduler.whenIdle();
});

test('hello schema, seed preparation, raw hash comparison, stats, and release', async (t) => {
    const fixture = await startFixtureForTest(t);
    if (!fixture) return;
    const socketStat = await fs.lstat(fixture.config.socketPath);
    assert.equal(socketStat.isSocket(), true);
    assert.equal(socketStat.mode & 0o777, 0o660);
    assert.equal((await fs.readdir(fixture.directory)).some((name) => name.startsWith('.xrv-bind-')), false);
    const client = await TestClient.connect(fixture.config.socketPath);
    try {
        const hello = await client.request({ v: 1, id: 1, op: 'hello', client: 'xmrig-proxy' });
        assert.deepEqual(hello.capabilities, ['prepare_seed', 'release_seed', 'verify', 'ping', 'stats']);
        assert.equal(hello.mode, 'fast');
        assert.equal(hello.allow_light_fallback, false);
        assert.equal(hello.max_frame, 16384);
        assert.equal(hello.vm_pool_size, 4);

        const prepared = await client.request({
            v: 1, id: 2, op: 'prepare_seed', seed_hash: SEED_A.toUpperCase(),
            mode: 'fast', allow_light_fallback: false
        });
        assert.equal(prepared.ok, true);
        assert.equal(prepared.seed_hash, SEED_A);

        const expected = mockHash(Buffer.from(SEED_A, 'hex'), Buffer.from(BLOB, 'hex')).toString('hex');
        const matched = await client.request(verifyRequest(3, SEED_A.toUpperCase(), BLOB.toUpperCase(), expected.toUpperCase()));
        assert.equal(matched.ok, true);
        assert.equal(matched.hash, expected);
        assert.equal(matched.match, true);

        const mismatch = await client.request(verifyRequest(4, SEED_A, BLOB, '00'.repeat(32)));
        assert.equal(mismatch.ok, true);
        assert.equal(mismatch.match, false);

        const stats = await client.request({ v: 1, id: 5, op: 'stats' });
        assert.equal(stats.stats.counters.verify_completed, 2);
        assert.equal(stats.stats.counters.verify_matches, 1);
        assert.equal(stats.stats.counters.verify_mismatches, 1);

        const released = await client.request({ v: 1, id: 6, op: 'release_seed', seed_hash: SEED_A });
        assert.equal(released.released, true);
        const cold = await client.request(verifyRequest(7, SEED_A, BLOB, expected));
        assert.equal(cold.ok, false);
        assert.equal(cold.code, 'SEED_NOT_READY');
        assert.equal(fixture.engine.hashCalls, 2);
    } finally {
        client.close();
        await fixture.stop();
    }
});

test('strict request validation rejects unknown fields and malformed hex without hashing', async (t) => {
    const fixture = await startFixtureForTest(t);
    if (!fixture) return;
    const client = await TestClient.connect(fixture.config.socketPath);
    try {
        await client.request({ v: 1, id: 1, op: 'hello' });
        const badSeed = await client.request({ v: 1, id: 2, op: 'prepare_seed', seed_hash: 'zz'.repeat(32) });
        assert.equal(badSeed.code, 'INVALID_SEED_HASH');
        const extra = await client.request({ v: 1, id: 3, op: 'ping', extra: true });
        assert.equal(extra.code, 'UNKNOWN_FIELD');
        const oddBlob = await client.request(verifyRequest(4, SEED_A, 'abc', '00'.repeat(32)));
        assert.equal(oddBlob.code, 'INVALID_BLOB');
        const badNonce = await client.request({ ...verifyRequest(5, SEED_A, BLOB, '00'.repeat(32)), nonce: '01' });
        assert.equal(badNonce.code, 'INVALID_NONCE');
        assert.equal(fixture.engine.hashCalls, 0);
    } finally {
        client.close();
        await fixture.stop();
    }
});

test('three seed slots are reserved and different warmups are serialized', async (t) => {
    const engine = new MockEngine();
    engine.prepareDelayMs = 15;
    const fixture = await startFixtureForTest(t, { engine });
    if (!fixture) return;
    const client = await TestClient.connect(fixture.config.socketPath);
    try {
        await client.request({ v: 1, id: 1, op: 'hello' });
        const prepares = [SEED_A, SEED_B, SEED_C].map((seed, index) => client.request({
            v: 1, id: index + 2, op: 'prepare_seed', seed_hash: seed
        }));
        const fourth = await client.request({ v: 1, id: 9, op: 'prepare_seed', seed_hash: SEED_D });
        assert.equal(fourth.code, 'SEED_CAPACITY');
        const results = await Promise.all(prepares);
        assert.ok(results.every((result) => result.ok));
        assert.equal(engine.maxActivePrepares, 1);
        assert.equal(engine.prepareCalls, 3);
    } finally {
        client.close();
        await fixture.stop();
    }
});

test('scheduler never exceeds VM count and rejects beyond its bounded queue', async (t) => {
    const engine = new MockEngine();
    const fixture = await startFixtureForTest(t, {
        engine,
        env: { VM_POOL_SIZE: '2', VERIFY_QUEUE_LIMIT: '1' }
    });
    if (!fixture) return;
    const client = await TestClient.connect(fixture.config.socketPath);
    try {
        await client.request({ v: 1, id: 1, op: 'hello' });
        await client.request({ v: 1, id: 2, op: 'prepare_seed', seed_hash: SEED_A });
        const gates = [engine.addHashGate(), engine.addHashGate(), engine.addHashGate()];
        const one = client.request(verifyRequest(10, SEED_A, '01', '00'.repeat(32)));
        const two = client.request(verifyRequest(11, SEED_A, '02', '00'.repeat(32)));
        await waitUntil(() => engine.activeHashes === 2);
        const three = client.request(verifyRequest(12, SEED_A, '03', '00'.repeat(32)));
        await waitUntil(() => fixture.server.scheduler.snapshot().queued === 1);
        const four = await client.request(verifyRequest(13, SEED_A, '04', '00'.repeat(32)));
        assert.equal(four.code, 'QUEUE_FULL');
        assert.equal(engine.maxActiveHashes, 2);
        gates[0].resolve();
        gates[1].resolve();
        await waitUntil(() => engine.hashCalls === 3);
        gates[2].resolve();
        await Promise.all([one, two, three]);
        assert.equal(engine.maxActiveHashes, 2);
    } finally {
        client.close();
        await fixture.stop();
    }
});

test('release retires a seed immediately but waits for an accepted hash', async (t) => {
    const engine = new MockEngine();
    const fixture = await startFixtureForTest(t, { engine });
    if (!fixture) return;
    const client = await TestClient.connect(fixture.config.socketPath);
    try {
        await client.request({ v: 1, id: 1, op: 'hello' });
        await client.request({ v: 1, id: 2, op: 'prepare_seed', seed_hash: SEED_A });
        const gate = engine.addHashGate();
        const share = client.request(verifyRequest(3, SEED_A, BLOB, '00'.repeat(32)));
        await waitUntil(() => engine.activeHashes === 1);
        const release = client.request({ v: 1, id: 4, op: 'release_seed', seed_hash: SEED_A });
        await waitUntil(() => fixture.server.seedManager.getSnapshot().seeds[0].state === 'retiring');
        const late = await client.request(verifyRequest(5, SEED_A, BLOB, '00'.repeat(32)));
        assert.equal(late.code, 'SEED_NOT_READY');
        assert.equal(engine.releaseCalls, 0);
        gate.resolve();
        await share;
        const released = await release;
        assert.equal(released.released, true);
        assert.equal(engine.releaseCalls, 1);
    } finally {
        client.close();
        await fixture.stop();
    }
});

test('per-client input pause preserves a coalesced second request', async (t) => {
    const engine = new MockEngine();
    const fixture = await startFixtureForTest(t, { engine, env: { MAX_PENDING_PER_CLIENT: '1' } });
    if (!fixture) return;
    const client = await TestClient.connect(fixture.config.socketPath);
    try {
        await client.request({ v: 1, id: 1, op: 'hello' });
        await client.request({ v: 1, id: 2, op: 'prepare_seed', seed_hash: SEED_A });
        const gate = engine.addHashGate();
        const firstPromise = client.expect(3);
        const secondPromise = client.expect(4);
        const first = encodeFrame(verifyRequest(3, SEED_A, '01', '00'.repeat(32)), 16384);
        const second = encodeFrame({ v: 1, id: 4, op: 'ping' }, 16384);
        client.socket.write(Buffer.concat([first, second]));
        await waitUntil(() => engine.activeHashes === 1);
        assert.equal(await promiseState(secondPromise), 'pending');
        gate.resolve();
        await firstPromise;
        const ping = await secondPromise;
        assert.equal(ping.pong, true);
    } finally {
        client.close();
        await fixture.stop();
    }
});

test('third concurrent client is rejected', async (t) => {
    const fixture = await startFixtureForTest(t);
    if (!fixture) return;
    const first = await TestClient.connect(fixture.config.socketPath);
    const second = await TestClient.connect(fixture.config.socketPath);
    try {
        await first.request({ v: 1, id: 1, op: 'hello' });
        await second.request({ v: 1, id: 1, op: 'hello' });
        const rejection = await readOneResponse(fixture.config.socketPath);
        assert.equal(rejection.ok, false);
        assert.equal(rejection.code, 'CLIENT_LIMIT');
    } finally {
        first.close();
        second.close();
        await fixture.stop();
    }
});

test('graceful shutdown drains accepted work and removes its own socket', async (t) => {
    const engine = new MockEngine();
    const fixture = await startFixtureForTest(t, { engine });
    if (!fixture) return;
    const client = await TestClient.connect(fixture.config.socketPath);
    await client.request({ v: 1, id: 1, op: 'hello' });
    await client.request({ v: 1, id: 2, op: 'prepare_seed', seed_hash: SEED_A });
    const gate = engine.addHashGate();
    const share = client.request(verifyRequest(3, SEED_A, BLOB, '00'.repeat(32)));
    await waitUntil(() => engine.activeHashes === 1);
    const stopping = fixture.server.stop('test');
    gate.resolve();
    await share;
    await stopping;
    await assert.rejects(fs.lstat(fixture.config.socketPath), { code: 'ENOENT' });
    assert.equal(engine.closed, true);
    await fixture.cleanup();
});

async function startFixture(options = {}) {
    const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'xrv-test-'));
    const socketPath = path.join(directory, 'verifier.sock');
    const env = {
        VERIFIER_SOCKET_PATH: socketPath,
        HANDSHAKE_TIMEOUT_MS: '1000',
        WRITE_STALL_TIMEOUT_MS: '1000',
        SHUTDOWN_TIMEOUT_MS: '2000',
        ...options.env
    };
    const config = loadConfig(env);
    const engine = options.engine || new MockEngine();
    const server = new VerifierServer(config, { engine, log: () => {} });
    try {
        await server.start();
    } catch (error) {
        await fs.rm(directory, { recursive: true, force: true });
        throw error;
    }
    let cleaned = false;
    const cleanup = async () => {
        if (cleaned) return;
        cleaned = true;
        await fs.rm(directory, { recursive: true, force: true });
    };
    return {
        directory, config, engine, server,
        stop: async () => { await server.stop('test'); await cleanup(); },
        cleanup
    };
}

async function startFixtureForTest(t, options = {}) {
    try {
        return await startFixture(options);
    } catch (error) {
        if (error.code === 'EPERM' && error.syscall === 'listen') {
            t.skip('AF_UNIX listen is blocked by this execution sandbox');
            return null;
        }
        throw error;
    }
}

class TestClient {
    constructor(socket) {
        this.socket = socket;
        this.decoder = new FrameDecoder(16384, 65536);
        this.pending = new Map();
        socket.on('data', (chunk) => {
            this.decoder.append(chunk);
            for (;;) {
                const response = this.decoder.next();
                if (response === null) break;
                const pending = this.pending.get(String(response.id));
                if (pending) {
                    this.pending.delete(String(response.id));
                    pending.resolve(response);
                }
            }
        });
        socket.on('error', () => {});
    }

    static async connect(socketPath) {
        const socket = net.createConnection({ path: socketPath });
        await new Promise((resolve, reject) => {
            socket.once('connect', resolve);
            socket.once('error', reject);
        });
        return new TestClient(socket);
    }

    expect(id) {
        return new Promise((resolve, reject) => this.pending.set(String(id), { resolve, reject }));
    }

    request(request) {
        const response = this.expect(request.id);
        this.socket.write(encodeFrame(request, 16384));
        return response;
    }

    close() {
        this.socket.destroy();
    }
}

function verifyRequest(id, seedHash, blob, claimedHash) {
    return {
        v: 1,
        id,
        op: 'verify',
        seed_hash: seedHash,
        blob,
        claimed_hash: claimedHash,
        job_id: `job-${id}`,
        nonce: '01020304',
        share_id: `share-${id}`
    };
}

async function readOneResponse(socketPath) {
    const socket = net.createConnection({ path: socketPath });
    const decoder = new FrameDecoder(16384, 65536);
    return new Promise((resolve, reject) => {
        socket.on('data', (chunk) => {
            decoder.append(chunk);
            const value = decoder.next();
            if (value) {
                socket.destroy();
                resolve(value);
            }
        });
        socket.on('error', reject);
    });
}

async function waitUntil(predicate, timeoutMs = 1000) {
    const deadline = Date.now() + timeoutMs;
    while (!predicate()) {
        if (Date.now() >= deadline) throw new Error('condition timed out');
        await new Promise((resolve) => setTimeout(resolve, 2));
    }
}

async function promiseState(promise) {
    return Promise.race([
        promise.then(() => 'fulfilled', () => 'rejected'),
        new Promise((resolve) => setTimeout(() => resolve('pending'), 10))
    ]);
}

function deferred() {
    let resolve;
    let reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
    return { promise, resolve, reject };
}
