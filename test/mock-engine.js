'use strict';

const crypto = require('node:crypto');

class MockEngine {
    constructor() {
        this.seeds = new Set();
        this.prepareCalls = 0;
        this.releaseCalls = 0;
        this.hashCalls = 0;
        this.activeHashes = 0;
        this.maxActiveHashes = 0;
        this.activePrepares = 0;
        this.maxActivePrepares = 0;
        this.prepareDelayMs = 0;
        this.hashGates = [];
        this.closed = false;
    }

    async prepare(seed) {
        this.prepareCalls += 1;
        this.activePrepares += 1;
        this.maxActivePrepares = Math.max(this.maxActivePrepares, this.activePrepares);
        if (this.prepareDelayMs) {
            await new Promise((resolve) => setTimeout(resolve, this.prepareDelayMs));
        }
        this.seeds.add(seed.toString('hex'));
        this.activePrepares -= 1;
    }

    hasSeed(seed) {
        return this.seeds.has(seed.toString('hex'));
    }

    async hash(seed, blob) {
        if (!this.hasSeed(seed)) {
            throw new Error('mock cold hash');
        }
        this.hashCalls += 1;
        this.activeHashes += 1;
        this.maxActiveHashes = Math.max(this.maxActiveHashes, this.activeHashes);
        const gate = this.hashGates.shift();
        if (gate) {
            await gate.promise;
        }
        const result = mockHash(seed, blob);
        this.activeHashes -= 1;
        return result;
    }

    release(seed) {
        this.releaseCalls += 1;
        return this.seeds.delete(seed.toString('hex'));
    }

    snapshot() {
        return { mock: true, seed_count: this.seeds.size, hash_calls: this.hashCalls };
    }

    close() {
        this.closed = true;
        this.seeds.clear();
    }

    addHashGate() {
        let resolve;
        const promise = new Promise((r) => { resolve = r; });
        const gate = { promise, resolve };
        this.hashGates.push(gate);
        return gate;
    }
}

function mockHash(seed, blob) {
    return crypto.createHash('sha256').update(seed).update(blob).digest();
}

module.exports = { MockEngine, mockHash };
