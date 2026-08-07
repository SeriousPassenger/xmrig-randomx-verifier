'use strict';

const path = require('node:path');

const ENGINE_COMMIT = 'e5bc20530e9aaac30f524fd0dd28ff4072ba745d';
const PROTOCOL_VERSION = 1;
const MAX_FRAME_BYTES = 16 * 1024;

function integer(name, fallback, minimum, maximum, env = process.env) {
    const raw = env[name];
    if (raw == null || raw === '') {
        return fallback;
    }

    if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
        throw new Error(`${name} must be an integer`);
    }

    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
        throw new Error(`${name} must be between ${minimum} and ${maximum}`);
    }

    return value;
}

function boolean(name, fallback, env = process.env) {
    const raw = env[name];
    if (raw == null || raw === '') {
        return fallback;
    }
    if (raw === '1' || raw === 'true') {
        return true;
    }
    if (raw === '0' || raw === 'false') {
        return false;
    }
    throw new Error(`${name} must be one of: 0, 1, false, true`);
}

function loadConfig(env = process.env) {
    const socketPath = env.VERIFIER_SOCKET_PATH || '/run/xmrig-randomx-verifier/verifier.sock';
    if (!path.isAbsolute(socketPath) || socketPath.includes('\0')) {
        throw new Error('VERIFIER_SOCKET_PATH must be an absolute filesystem path');
    }

    const vmPoolSize = integer('VM_POOL_SIZE', 4, 1, 64, env);
    const queueLimit = integer('VERIFY_QUEUE_LIMIT', 256, 1, 4096, env);

    return Object.freeze({
        socketPath,
        socketMode: 0o660,
        maxClients: 2,
        maxFrameBytes: MAX_FRAME_BYTES,
        maxBufferedInputBytes: MAX_FRAME_BYTES * 4,
        maxBlobBytes: integer('MAX_BLOB_BYTES', 4096, 1, 8192, env),
        maxSeeds: 3,
        vmPoolSize,
        initThreads: integer('INIT_THREADS', 4, 1, 256, env),
        enableHugePages: boolean('ENABLE_HUGE_PAGES', false, env),
        queueLimit,
        maxPendingPerClient: integer(
            'MAX_PENDING_PER_CLIENT',
            queueLimit + vmPoolSize + 16,
            1,
            8192,
            env
        ),
        handshakeTimeoutMs: integer('HANDSHAKE_TIMEOUT_MS', 5000, 100, 60000, env),
        writeStallTimeoutMs: integer('WRITE_STALL_TIMEOUT_MS', 10000, 100, 120000, env),
        shutdownTimeoutMs: integer('SHUTDOWN_TIMEOUT_MS', 30000, 1000, 300000, env),
        protocolVersion: PROTOCOL_VERSION,
        engineCommit: ENGINE_COMMIT
    });
}

module.exports = { ENGINE_COMMIT, PROTOCOL_VERSION, MAX_FRAME_BYTES, loadConfig };
