'use strict';

class ServiceError extends Error {
    constructor(code, message, options = {}) {
        super(message);
        this.name = 'ServiceError';
        this.code = code;
        this.retryable = options.retryable === true;
        this.fatal = options.fatal === true;
    }
}

function asServiceError(error) {
    if (error instanceof ServiceError) {
        return error;
    }

    return new ServiceError('INTERNAL_ERROR', 'internal verifier error', { retryable: true });
}

module.exports = { ServiceError, asServiceError };
