#!/usr/bin/env node
'use strict';

const net = require('node:net');
const { FrameDecoder, encodeFrame } = require('../src/protocol');

const socketPath = process.env.VERIFIER_SOCKET_PATH || '/run/xmrig-randomx-verifier/verifier.sock';
const timeoutMs = positiveInteger(process.env.READY_TIMEOUT_MS || '15000');
const deadline = Date.now() + timeoutMs;

async function main() {
    let lastError = null;
    while (Date.now() < deadline) {
        try {
            const response = await hello();
            if (response.v !== 1 || response.ok !== true ||
                response.service !== 'xmrig-randomx-verifier' ||
                response.mode !== 'fast' || response.allow_light_fallback !== false ||
                response.max_frame !== 16384 || !Array.isArray(response.capabilities)) {
                throw new Error('verifier returned an incompatible hello response');
            }
            console.log(`verifier ready at ${socketPath}`);
            return;
        } catch (error) {
            lastError = error;
            await new Promise((resolve) => setTimeout(resolve, 100));
        }
    }
    throw new Error(`verifier was not ready within ${timeoutMs} ms: ${lastError && lastError.message}`);
}

function hello() {
    return new Promise((resolve, reject) => {
        const socket = net.createConnection({ path: socketPath });
        const decoder = new FrameDecoder(16384, 65536);
        const timer = setTimeout(() => finish(new Error('hello timed out')), 2000);
        let settled = false;

        function finish(error, value) {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            socket.destroy();
            if (error) reject(error);
            else resolve(value);
        }

        socket.once('connect', () => {
            socket.write(encodeFrame({
                v: 1,
                id: 'systemd-ready',
                op: 'hello',
                client: 'xmrig-proxy'
            }, 16384));
        });
        socket.on('data', (chunk) => {
            try {
                decoder.append(chunk);
                const response = decoder.next();
                if (response !== null) finish(null, response);
            } catch (error) {
                finish(error);
            }
        });
        socket.once('error', (error) => finish(error));
        socket.once('close', () => {
            if (!settled) finish(new Error('connection closed before hello response'));
        });
    });
}

function positiveInteger(raw) {
    if (!/^[1-9][0-9]*$/.test(raw)) {
        throw new Error('READY_TIMEOUT_MS must be a positive integer');
    }
    const value = Number(raw);
    if (!Number.isSafeInteger(value) || value > 300000) {
        throw new Error('READY_TIMEOUT_MS must not exceed 300000');
    }
    return value;
}

main().catch((error) => {
    console.error(error && error.message ? error.message : error);
    process.exitCode = 1;
});
