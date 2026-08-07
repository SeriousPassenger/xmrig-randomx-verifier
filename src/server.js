'use strict';

const crypto = require('node:crypto');
const net = require('node:net');
const process = require('node:process');
const { RandomXEngine } = require('./engine');
const { SeedManager } = require('./seed-manager');
const { VerifyScheduler } = require('./scheduler');
const { ServiceError, asServiceError } = require('./errors');
const {
    FrameDecoder,
    encodeFrame,
    validateBase,
    validateOperation,
    validId,
    idKey,
    success,
    failure
} = require('./protocol');
const {
    prepareSocketPath,
    makePrivateBindPath,
    captureBoundSocket,
    publishBoundSocket,
    unlinkIfSameSocket
} = require('./socket-path');

class VerifierServer {
    constructor(config, options = {}) {
        this.config = config;
        this.engine = options.engine || new RandomXEngine(config, options.randomxModule);
        this.seedManager = new SeedManager(this.engine, config.maxSeeds);
        this.scheduler = new VerifyScheduler(config.vmPoolSize, config.queueLimit);
        this.server = net.createServer((socket) => this.#accept(socket));
        this.connections = new Set();
        this.rejectedSockets = new Set();
        this.boundIdentity = null;
        this.privateBindPath = null;
        this.state = 'created';
        this.startedAt = Date.now();
        this.stopPromise = null;
        this.log = options.log || defaultLog;
        this.metrics = {
            connections_total: 0,
            connections_rejected: 0,
            requests_total: 0,
            protocol_errors: 0,
            prepare_ok: 0,
            prepare_failed: 0,
            releases: 0,
            verify_received: 0,
            verify_completed: 0,
            verify_matches: 0,
            verify_mismatches: 0,
            verify_failed: 0,
            queue_rejected: 0,
            queue_ms_total: 0,
            hash_ms_total: 0,
            total_ms_total: 0,
            hash_ms_max: 0
        };
    }

    async start() {
        if (this.state !== 'created') {
            throw new Error('server can only be started once');
        }
        this.state = 'starting';
        await prepareSocketPath(this.config.socketPath);
        this.privateBindPath = makePrivateBindPath(this.config.socketPath);

        const oldUmask = process.umask(0o117); // 0777 & ~0117 = 0660
        let restored = false;
        const restoreUmask = () => {
            if (!restored) {
                restored = true;
                process.umask(oldUmask);
            }
        };

        try {
            await new Promise((resolve, reject) => {
                const onError = (error) => {
                    this.server.off('listening', onListening);
                    reject(error);
                };
                const onListening = () => {
                    this.server.off('error', onError);
                    resolve();
                };
                this.server.once('error', onError);
                this.server.once('listening', onListening);
                this.server.listen(this.privateBindPath);
            });
        } finally {
            restoreUmask();
        }

        try {
            this.boundIdentity = await captureBoundSocket(this.privateBindPath, this.config.socketMode);
            await publishBoundSocket(
                this.privateBindPath,
                this.config.socketPath,
                this.boundIdentity
            );
        } catch (error) {
            await new Promise((resolve) => this.server.close(resolve));
            await unlinkIfSameSocket(
                this.config.socketPath,
                this.boundIdentity || error.socketIdentity
            ).catch(() => {});
            throw error;
        }

        this.state = 'running';
        this.startedAt = Date.now();
        this.log('info', 'verifier listening', {
            socket: this.config.socketPath,
            mode: 'fast',
            vm_pool_size: this.config.vmPoolSize,
            init_threads: this.config.initThreads,
            huge_pages: this.config.enableHugePages
        });
        return this;
    }

    async stop(reason = 'requested') {
        if (this.stopPromise) {
            return this.stopPromise;
        }
        this.stopPromise = this.#stop(reason);
        return this.stopPromise;
    }

    async #stop(reason) {
        if (this.state === 'stopped') {
            return;
        }
        this.state = 'draining';
        this.scheduler.stopAccepting();
        this.log('info', 'verifier draining', { reason });

        const serverClosed = new Promise((resolve) => {
            if (!this.server.listening) {
                resolve();
                return;
            }
            this.server.close(resolve);
        });
        for (const connection of this.connections) {
            connection.stopReading();
        }
        for (const socket of this.rejectedSockets) {
            socket.destroy();
        }

        await this.scheduler.whenIdle();
        await Promise.all([...this.connections].map((connection) => connection.whenRequestsDone()));
        await this.seedManager.close();

        for (const connection of this.connections) {
            connection.endWhenFlushed();
        }
        await withTimeout(
            Promise.all([
                serverClosed,
                ...[...this.connections].map((connection) => connection.whenClosed())
            ]),
            this.config.shutdownTimeoutMs
        ).catch(() => {
            for (const connection of this.connections) {
                connection.destroy();
            }
        });

        await serverClosed;
        await unlinkIfSameSocket(this.config.socketPath, this.boundIdentity).catch((error) => {
            this.log('error', 'socket cleanup failed', { error: error.message });
        });
        this.state = 'stopped';
        this.log('info', 'verifier stopped', { reason });
    }

    async handleRequest(connection, request) {
        const values = validateOperation(request, this.config.maxBlobBytes);
        this.metrics.requests_total += 1;

        switch (request.op) {
        case 'hello':
            return success(request.id, {
                service: 'xmrig-randomx-verifier',
                mode: 'fast',
                allow_light_fallback: false,
                max_frame: this.config.maxFrameBytes,
                capabilities: ['prepare_seed', 'release_seed', 'verify', 'ping', 'stats'],
                vm_pool_size: this.config.vmPoolSize
            });
        case 'ping':
            return success(request.id, {
                pong: true,
                healthy: this.state === 'running',
                state: this.state,
                uptime_ms: Date.now() - this.startedAt
            });
        case 'stats':
            return success(request.id, { stats: this.getStats() });
        case 'prepare_seed':
            return this.#prepare(request.id, values.seedHash);
        case 'release_seed':
            return this.#release(request.id, values.seedHash);
        case 'verify':
            return this.#verify(connection, request.id, values);
        default:
            throw new ServiceError('UNKNOWN_OPERATION', 'unsupported operation');
        }
    }

    async #prepare(id, seedHash) {
        if (this.state !== 'running') {
            throw new ServiceError('SHUTTING_DOWN', 'verifier is not accepting seed preparation', { retryable: true });
        }
        try {
            const result = await this.seedManager.prepare(seedHash);
            this.metrics.prepare_ok += 1;
            return success(id, {
                seed_hash: seedHash,
                state: 'ready',
                already_ready: result.alreadyReady,
                prepare_ms: roundMs(result.prepareMs)
            });
        } catch (error) {
            this.metrics.prepare_failed += 1;
            throw error;
        }
    }

    async #release(id, seedHash) {
        const released = await this.seedManager.release(seedHash);
        if (released) {
            this.metrics.releases += 1;
        }
        return success(id, { seed_hash: seedHash, released });
    }

    async #verify(connection, id, values) {
        if (this.state !== 'running') {
            throw new ServiceError('SHUTTING_DOWN', 'verifier is not accepting shares', { retryable: true });
        }
        this.metrics.verify_received += 1;
        const receivedAt = process.hrtime.bigint();
        let lease = null;

        try {
            lease = this.seedManager.acquire(values.seedHash);
            const result = await this.scheduler.enqueue(connection, async () => {
                lease.assertPresent();
                const hashStarted = process.hrtime.bigint();
                const queueMs = elapsedMs(receivedAt, hashStarted);
                const hash = await this.engine.hash(lease.seed, Buffer.from(values.blob, 'hex'));
                const hashMs = elapsedMs(hashStarted);
                return { hash, queueMs, hashMs };
            });

            const claimed = Buffer.from(values.claimedHash, 'hex');
            const match = crypto.timingSafeEqual(result.hash, claimed);
            const totalMs = elapsedMs(receivedAt);
            this.metrics.verify_completed += 1;
            this.metrics[match ? 'verify_matches' : 'verify_mismatches'] += 1;
            this.metrics.queue_ms_total += result.queueMs;
            this.metrics.hash_ms_total += result.hashMs;
            this.metrics.total_ms_total += totalMs;
            this.metrics.hash_ms_max = Math.max(this.metrics.hash_ms_max, result.hashMs);

            return success(id, {
                hash: result.hash.toString('hex'),
                match,
                queue_ms: roundMs(result.queueMs),
                hash_ms: roundMs(result.hashMs),
                total_ms: roundMs(totalMs)
            });
        } catch (error) {
            this.metrics.verify_failed += 1;
            if (error.code === 'QUEUE_FULL') {
                this.metrics.queue_rejected += 1;
            }
            throw error;
        } finally {
            if (lease) {
                lease.release();
            }
        }
    }

    getStats() {
        const completed = this.metrics.verify_completed;
        return {
            service: {
                state: this.state,
                healthy: this.state === 'running',
                uptime_ms: Date.now() - this.startedAt,
                pid: process.pid,
                rss_bytes: process.memoryUsage().rss,
                mode: 'fast',
                allow_light_fallback: false,
                engine_commit: this.config.engineCommit
            },
            clients: {
                current: this.connections.size,
                max: this.config.maxClients,
                total: this.metrics.connections_total,
                rejected: this.metrics.connections_rejected
            },
            scheduler: this.scheduler.snapshot(),
            seeds: this.seedManager.getSnapshot(),
            counters: { ...this.metrics },
            timing: {
                average_queue_ms: completed ? roundMs(this.metrics.queue_ms_total / completed) : 0,
                average_hash_ms: completed ? roundMs(this.metrics.hash_ms_total / completed) : 0,
                average_total_ms: completed ? roundMs(this.metrics.total_ms_total / completed) : 0,
                maximum_hash_ms: roundMs(this.metrics.hash_ms_max)
            },
            engine: this.engine.snapshot()
        };
    }

    #accept(socket) {
        socket.setNoDelay(true);
        if (this.state !== 'running' || this.connections.size >= this.config.maxClients) {
            this.metrics.connections_rejected += 1;
            const error = new ServiceError('CLIENT_LIMIT', 'verifier client limit reached', { retryable: true });
            socket.on('error', () => {});
            this.rejectedSockets.add(socket);
            const remove = () => this.rejectedSockets.delete(socket);
            socket.once('close', remove);
            const timer = setTimeout(() => socket.destroy(), 1000);
            timer.unref();
            socket.end(encodeFrame(failure(null, error), this.config.maxFrameBytes), () => {
                clearTimeout(timer);
                socket.destroy();
            });
            return;
        }

        const connection = new ClientConnection(this, socket);
        this.connections.add(connection);
        this.metrics.connections_total += 1;
    }

    onConnectionClosed(connection) {
        this.scheduler.cancelOwner(connection);
        this.connections.delete(connection);
    }
}

class ClientConnection {
    constructor(service, socket) {
        this.service = service;
        this.config = service.config;
        this.socket = socket;
        this.decoder = new FrameDecoder(this.config.maxFrameBytes, this.config.maxBufferedInputBytes);
        this.activeIds = new Set();
        this.pendingRequests = 0;
        this.requestWaiters = [];
        this.writeQueue = [];
        this.writeQueueBytes = 0;
        this.writeBlocked = false;
        this.writeStallTimer = null;
        this.frameTimer = null;
        this.helloSeen = false;
        this.inputStopped = false;
        this.closeAfterFlush = false;
        this.ending = false;
        this.closed = false;
        this.drainingInput = false;
        this.closedPromise = new Promise((resolve) => { this.resolveClosed = resolve; });

        this.handshakeTimer = setTimeout(() => {
            this.#fatal(new ServiceError('HELLO_TIMEOUT', 'hello was not received in time', { fatal: true }));
        }, this.config.handshakeTimeoutMs);
        this.handshakeTimer.unref();

        socket.on('data', (chunk) => this.#onData(chunk));
        socket.on('drain', () => this.#onDrain());
        socket.on('error', () => {});
        socket.on('close', () => this.#onClose());
    }

    #onData(chunk) {
        if (this.closed || this.inputStopped) {
            return;
        }
        try {
            this.decoder.append(chunk);
            this.#drainInput();
            this.#updateFrameTimer();
        } catch (error) {
            this.#fatal(asServiceError(error));
        }
    }

    #drainInput() {
        if (this.drainingInput || this.closed || this.inputStopped || this.writeBlocked) {
            return;
        }
        this.drainingInput = true;
        try {
            while (!this.closed && !this.inputStopped && !this.writeBlocked &&
                   this.pendingRequests < this.config.maxPendingPerClient) {
                const request = this.decoder.next();
                if (request === null) {
                    break;
                }
                this.#dispatch(request);
            }
        } catch (error) {
            this.#fatal(asServiceError(error));
        } finally {
            this.drainingInput = false;
            this.#updateFrameTimer();
            this.#updateReadFlow();
        }
    }

    #dispatch(request) {
        let id;
        try {
            id = validateBase(request);
        } catch (error) {
            this.service.metrics.protocol_errors += 1;
            this.send(failure(validId(request.id) ? request.id : null, asServiceError(error)));
            return;
        }

        if (!this.helloSeen) {
            if (request.op !== 'hello') {
                this.#fatal(new ServiceError('HELLO_REQUIRED', 'hello must be the first request', { fatal: true }), id);
                return;
            }
            try {
                // A syntactically valid base envelope is not a successful handshake.
                validateOperation(request, this.config.maxBlobBytes);
            } catch (error) {
                this.#fatal(asServiceError(error), id);
                return;
            }
            this.helloSeen = true;
            clearTimeout(this.handshakeTimer);
        }

        const key = idKey(id);
        if (this.activeIds.has(key)) {
            this.service.metrics.protocol_errors += 1;
            this.send(failure(id, new ServiceError('DUPLICATE_ID', 'request id is already in flight')));
            return;
        }

        this.activeIds.add(key);
        this.pendingRequests += 1;
        let operation;
        operation = this.service.handleRequest(this, request)
            .then((response) => this.send(response))
            .catch((error) => this.send(failure(id, asServiceError(error))))
            .finally(() => {
                this.activeIds.delete(key);
                this.pendingRequests -= 1;
                if (this.pendingRequests === 0) {
                    const waiters = this.requestWaiters.splice(0);
                    for (const resolve of waiters) resolve();
                }
                this.#updateReadFlow();
            });
        void operation;
    }

    send(response) {
        if (this.closed) {
            return;
        }
        let frame;
        try {
            frame = encodeFrame(response, this.config.maxFrameBytes);
        } catch {
            this.destroy();
            return;
        }

        const outputLimit = this.config.maxFrameBytes * (this.config.maxPendingPerClient + 8);
        if (this.writeQueueBytes + frame.length > outputLimit) {
            this.destroy();
            return;
        }
        this.writeQueue.push(frame);
        this.writeQueueBytes += frame.length;
        this.#flushWrites();
    }

    #flushWrites() {
        if (this.closed || this.writeBlocked) {
            return;
        }
        while (this.writeQueue.length > 0) {
            const frame = this.writeQueue.shift();
            this.writeQueueBytes -= frame.length;
            let writable;
            try {
                writable = this.socket.write(frame);
            } catch {
                this.destroy();
                return;
            }
            if (!writable) {
                this.writeBlocked = true;
                this.socket.pause();
                this.writeStallTimer = setTimeout(() => this.destroy(), this.config.writeStallTimeoutMs);
                this.writeStallTimer.unref();
                return;
            }
        }
        if (this.closeAfterFlush) {
            if (!this.ending) {
                this.ending = true;
                this.socket.end(() => this.socket.destroy());
            }
        }
    }

    #onDrain() {
        if (this.closed) return;
        this.writeBlocked = false;
        clearTimeout(this.writeStallTimer);
        this.writeStallTimer = null;
        this.#flushWrites();
        this.#updateReadFlow();
    }

    #fatal(error, id = null) {
        if (this.closed || this.closeAfterFlush) {
            return;
        }
        this.service.metrics.protocol_errors += 1;
        this.inputStopped = true;
        this.socket.pause();
        this.closeAfterFlush = true;
        this.send(failure(id, error));
        this.#flushWrites();
    }

    #updateReadFlow() {
        this.#updateFrameTimer();
        if (this.closed || this.inputStopped || this.writeBlocked ||
            this.pendingRequests >= this.config.maxPendingPerClient) {
            this.socket.pause();
            return;
        }
        this.socket.resume();
        queueMicrotask(() => this.#drainInput());
    }

    #updateFrameTimer() {
        if (this.closed || this.inputStopped || this.writeBlocked ||
            this.pendingRequests >= this.config.maxPendingPerClient ||
            !this.decoder.hasIncompleteFrame()) {
            clearTimeout(this.frameTimer);
            this.frameTimer = null;
            return;
        }
        if (!this.frameTimer) {
            this.frameTimer = setTimeout(() => {
                this.#fatal(new ServiceError('FRAME_TIMEOUT', 'incomplete frame timed out', { fatal: true }));
            }, this.config.handshakeTimeoutMs);
            this.frameTimer.unref();
        }
    }

    #onClose() {
        if (this.closed) return;
        this.closed = true;
        clearTimeout(this.handshakeTimer);
        clearTimeout(this.writeStallTimer);
        clearTimeout(this.frameTimer);
        this.service.onConnectionClosed(this);
        this.resolveClosed();
    }

    stopReading() {
        this.inputStopped = true;
        clearTimeout(this.handshakeTimer);
        clearTimeout(this.frameTimer);
        this.frameTimer = null;
        this.socket.pause();
    }

    whenRequestsDone() {
        if (this.pendingRequests === 0) {
            return Promise.resolve();
        }
        return new Promise((resolve) => this.requestWaiters.push(resolve));
    }

    endWhenFlushed() {
        this.closeAfterFlush = true;
        this.#flushWrites();
    }

    whenClosed() {
        return this.closedPromise;
    }

    destroy() {
        this.socket.destroy();
    }
}

function elapsedMs(start, end = process.hrtime.bigint()) {
    return Number(end - start) / 1e6;
}

function roundMs(value) {
    return Math.round(value * 1000) / 1000;
}

function withTimeout(promise, timeoutMs) {
    return new Promise((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error('operation timed out')), timeoutMs);
        timer.unref();
        promise.then(
            (value) => { clearTimeout(timer); resolve(value); },
            (error) => { clearTimeout(timer); reject(error); }
        );
    });
}

function defaultLog(level, message, fields = {}) {
    const line = JSON.stringify({ time: new Date().toISOString(), level, message, ...fields });
    (level === 'error' ? console.error : console.log)(line);
}

module.exports = { VerifierServer };
