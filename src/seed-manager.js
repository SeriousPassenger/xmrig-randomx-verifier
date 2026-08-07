'use strict';

const { ServiceError } = require('./errors');

class SeedManager {
    constructor(engine, maxSeeds) {
        this.engine = engine;
        this.maxSeeds = maxSeeds;
        this.records = new Map();
        this.prepareTail = Promise.resolve();
        this.closing = false;
    }

    async prepare(seedHash) {
        if (this.closing) {
            throw new ServiceError('SHUTTING_DOWN', 'verifier is shutting down', { retryable: true });
        }

        const existing = this.records.get(seedHash);
        if (existing) {
            if (existing.state === 'ready') {
                if (!this.engine.hasSeed(existing.seed)) {
                    throw new ServiceError('ENGINE_STATE', 'prepared seed disappeared from the native pool');
                }
                return { alreadyReady: true, prepareMs: 0 };
            }
            if (existing.state === 'retiring') {
                throw new ServiceError('SEED_RETIRING', 'seed is being released', { retryable: true });
            }
            await existing.preparePromise;
            if (existing.state !== 'ready') {
                throw new ServiceError('SEED_NOT_READY', 'seed preparation did not complete', { retryable: true });
            }
            return { alreadyReady: false, prepareMs: existing.prepareMs };
        }

        if (this.records.size >= this.maxSeeds) {
            throw new ServiceError(
                'SEED_CAPACITY',
                `all ${this.maxSeeds} seed slots are reserved; release an old seed first`,
                { retryable: true }
            );
        }

        const record = {
            seed: Buffer.from(seedHash, 'hex'),
            state: 'warming',
            outstanding: 0,
            zeroWaiters: [],
            prepareMs: 0,
            preparePromise: null,
            releasePromise: null
        };
        this.records.set(seedHash, record);

        const prepareWork = this.prepareTail.catch(() => {}).then(async () => {
            if (record.state === 'retiring') {
                return;
            }
            const started = process.hrtime.bigint();
            await this.engine.prepare(record.seed);
            record.prepareMs = elapsedMs(started);
            if (record.state !== 'retiring') {
                record.state = 'ready';
            }
        });
        this.prepareTail = prepareWork.catch(() => {});
        record.preparePromise = prepareWork.catch((error) => {
            if (record.state !== 'retiring' && this.records.get(seedHash) === record) {
                this.records.delete(seedHash);
            }
            throw error;
        });

        await record.preparePromise;
        if (record.state !== 'ready') {
            throw new ServiceError('SEED_RETIRING', 'seed was released during preparation', { retryable: true });
        }
        return { alreadyReady: false, prepareMs: record.prepareMs };
    }

    acquire(seedHash) {
        if (this.closing) {
            throw new ServiceError('SHUTTING_DOWN', 'verifier is shutting down', { retryable: true });
        }
        const record = this.records.get(seedHash);
        if (!record || record.state !== 'ready') {
            throw new ServiceError('SEED_NOT_READY', 'seed has not been prepared', { retryable: false });
        }
        if (!this.engine.hasSeed(record.seed)) {
            throw new ServiceError('ENGINE_STATE', 'prepared seed disappeared from the native pool');
        }

        record.outstanding += 1;
        let released = false;
        return {
            seed: record.seed,
            assertPresent: () => {
                if (!this.engine.hasSeed(record.seed)) {
                    throw new ServiceError('ENGINE_STATE', 'prepared seed disappeared before hashing');
                }
            },
            release: () => {
                if (released) {
                    return;
                }
                released = true;
                record.outstanding -= 1;
                if (record.outstanding === 0) {
                    const waiters = record.zeroWaiters.splice(0);
                    for (const resolve of waiters) {
                        resolve();
                    }
                }
            }
        };
    }

    async release(seedHash) {
        const record = this.records.get(seedHash);
        if (!record) {
            return false;
        }
        if (record.releasePromise) {
            return record.releasePromise;
        }

        record.state = 'retiring';
        record.releasePromise = (async () => {
            if (record.preparePromise) {
                await record.preparePromise.catch(() => {});
            }
            await waitForZero(record);
            const released = this.engine.hasSeed(record.seed) ? this.engine.release(record.seed) : false;
            if (this.records.get(seedHash) === record) {
                this.records.delete(seedHash);
            }
            return released;
        })();
        return record.releasePromise;
    }

    getSnapshot() {
        const seeds = [];
        for (const [seedHash, record] of this.records) {
            seeds.push({
                seed_hash: seedHash,
                state: record.state,
                outstanding: record.outstanding,
                prepare_ms: record.prepareMs
            });
        }
        return { count: seeds.length, max: this.maxSeeds, seeds };
    }

    async close() {
        if (this.closing) {
            return;
        }
        this.closing = true;
        await Promise.allSettled([...this.records.values()]
            .map((record) => record.preparePromise)
            .filter(Boolean));
        await Promise.all([...this.records.values()].map(waitForZero));
        this.engine.close();
        this.records.clear();
    }
}

function waitForZero(record) {
    if (record.outstanding === 0) {
        return Promise.resolve();
    }
    return new Promise((resolve) => record.zeroWaiters.push(resolve));
}

function elapsedMs(start) {
    return Number(process.hrtime.bigint() - start) / 1e6;
}

module.exports = { SeedManager };
