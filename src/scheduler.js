'use strict';

const { ServiceError } = require('./errors');

class VerifyScheduler {
    constructor(concurrency, queueLimit) {
        this.concurrency = concurrency;
        this.queueLimit = queueLimit;
        this.active = 0;
        this.queue = [];
        this.accepting = true;
        this.highWater = 0;
        this.idleWaiters = [];
    }

    enqueue(owner, run) {
        if (!this.accepting) {
            return Promise.reject(new ServiceError('SHUTTING_DOWN', 'verifier is draining', { retryable: true }));
        }
        if (this.queue.length >= this.queueLimit) {
            return Promise.reject(new ServiceError('QUEUE_FULL', 'verification queue is full', { retryable: true }));
        }

        return new Promise((resolve, reject) => {
            this.queue.push({ owner, run, resolve, reject });
            this.highWater = Math.max(this.highWater, this.queue.length);
            this.#drain();
        });
    }

    cancelOwner(owner) {
        const kept = [];
        for (const job of this.queue) {
            if (job.owner === owner) {
                job.reject(new ServiceError('CLIENT_DISCONNECTED', 'client disconnected before verification'));
            } else {
                kept.push(job);
            }
        }
        this.queue = kept;
        this.#notifyIdle();
    }

    stopAccepting() {
        this.accepting = false;
    }

    whenIdle() {
        if (this.active === 0 && this.queue.length === 0) {
            return Promise.resolve();
        }
        return new Promise((resolve) => this.idleWaiters.push(resolve));
    }

    snapshot() {
        return {
            active: this.active,
            active_limit: this.concurrency,
            queued: this.queue.length,
            queue_limit: this.queueLimit,
            queue_high_water: this.highWater,
            accepting: this.accepting
        };
    }

    #drain() {
        while (this.active < this.concurrency && this.queue.length > 0) {
            const job = this.queue.shift();
            this.active += 1;
            Promise.resolve()
                .then(job.run)
                .then(job.resolve, job.reject)
                .finally(() => {
                    this.active -= 1;
                    this.#drain();
                    this.#notifyIdle();
                });
        }
    }

    #notifyIdle() {
        if (this.active !== 0 || this.queue.length !== 0) {
            return;
        }
        const waiters = this.idleWaiters.splice(0);
        for (const resolve of waiters) {
            resolve();
        }
    }
}

module.exports = { VerifyScheduler };
