#!/usr/bin/env node
'use strict';

const assert = require('node:assert/strict');
const crypto = require('node:crypto');
const { loadConfig } = require('../src/config');
const { RandomXEngine } = require('../src/engine');

// Official Monero calc_pow documentation vector with an exact 32-byte seed.
const SEED = Buffer.from(
    'd432f499205150873b2572b5f033c9c6e4b7c6f3394bd2dd93822cd7085e7307',
    'hex'
);
const INPUT = Buffer.from(
    '0e0ed286da8006ecdc1aab3033cf1716c52f13f9d8ae0051615a2453643de946' +
    '43b550d543becd0000000002abc78b0101ffefc68b0101fcfcf0d4b422025014' +
    'bb4a1eade6622fd781cb1063381cad396efa69719b41aa28b4fce8c7ad4b5f01' +
    '9ce1dc670456b24a5e03c2d9058a2df10fec779e2579753b1847b74ee644f16b' +
    '023c000000000000000000000000000000000000000000000000000000000000' +
    '0000000000000000000000000000000000000000000000000000000000000005' +
    '1399a1bc46a846474f5b33db24eae173a26393b976054ee14f9feefe999252338' +
    '02867097564c9db7a36af5bb5ed33ab46e63092bd8d32cef121608c3258edd555' +
    '62812e21cc7e3ac73045745a72f7d74581d9a0849d6f30e8b2923171253e864f' +
    '4e9ddea3acb5bc755f1c4a878130a70c26297540bc0b7a57affb6b35c1f03d8d' +
    'bd54ece8457531f8cba15bb74516779c01193e212050423020e45aa2c15dcb',
    'hex'
);
const EXPECTED = Buffer.from(
    'd0402d6834e26fb94a9ce38c6424d27d2069896a9b8b1ce685d79936bca6e0a8',
    'hex'
);

async function main() {
    const config = loadConfig({
        ...process.env,
        VERIFIER_SOCKET_PATH: process.env.VERIFIER_SOCKET_PATH || '/run/xmrig-randomx-verifier/verifier.sock'
    });
    const engine = new RandomXEngine(config);
    const started = process.hrtime.bigint();
    try {
        await engine.prepare(SEED);
        const preparedMs = Number(process.hrtime.bigint() - started) / 1e6;
        const hash = await engine.hash(SEED, INPUT);
        assert.equal(hash.length, 32);
        assert(crypto.timingSafeEqual(hash, EXPECTED),
            `hash mismatch: got ${hash.toString('hex')}, expected ${EXPECTED.toString('hex')}`);
        console.log(JSON.stringify({
            ok: true,
            mode: 'fast',
            vm_pool_size: config.vmPoolSize,
            init_threads: config.initThreads,
            huge_pages: config.enableHugePages,
            prepare_ms: Math.round(preparedMs * 1000) / 1000,
            hash: hash.toString('hex'),
            rss_bytes: process.memoryUsage().rss
        }, null, 2));
    } finally {
        engine.close();
    }
}

main().catch((error) => {
    console.error(error && error.stack ? error.stack : error);
    process.exitCode = 1;
});
