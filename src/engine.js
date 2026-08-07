'use strict';

const { ServiceError } = require('./errors');

class RandomXEngine {
    constructor(config, randomxModule = null) {
        // Load the native addon only in production. Unit tests inject a mock engine.
        this.randomx = randomxModule || require('randomx-hashing');
        this.pool = this.randomx.createPoolSeedPool({
            maxSeeds: config.maxSeeds,
            vmPoolSize: config.vmPoolSize,
            mode: 'fast',
            threads: config.initThreads,
            enableJit: true,
            enableAes: true,
            enableHugePages: config.enableHugePages
        });
        this.closed = false;
    }

    async prepare(seed) {
        this.#assertOpen();
        await this.pool.warmSeedAsync(seed);
        if (!this.pool.has(seed)) {
            throw new ServiceError('ENGINE_STATE', 'engine did not retain the prepared seed');
        }
    }

    hasSeed(seed) {
        return !this.closed && this.pool.has(seed);
    }

    async hash(seed, blob) {
        this.#assertOpen();
        // SeedPool.hashAsync() lazily initializes a missing seed. This guard is mandatory.
        if (!this.pool.has(seed)) {
            throw new ServiceError('SEED_NOT_READY', 'seed is absent from the native fast-mode pool');
        }
        const result = await this.pool.hashAsync(seed, blob);
        if (!Buffer.isBuffer(result) || result.length !== 32) {
            throw new ServiceError('ENGINE_RESULT', 'RandomX engine returned an invalid hash');
        }
        return result;
    }

    release(seed) {
        if (this.closed) {
            return false;
        }
        return this.pool.release(seed);
    }

    snapshot() {
        return {
            pool: this.closed ? null : this.pool.getSnapshot(),
            native: typeof this.randomx.getStats === 'function' ? this.randomx.getStats() : null,
            hardware: typeof this.randomx.getHardwareInfo === 'function' ? this.randomx.getHardwareInfo() : null
        };
    }

    close() {
        if (!this.closed) {
            this.closed = true;
            this.pool.releaseAll();
        }
    }

    #assertOpen() {
        if (this.closed) {
            throw new ServiceError('ENGINE_CLOSED', 'RandomX engine is closed');
        }
    }
}

module.exports = { RandomXEngine };
