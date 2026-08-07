'use strict';

const { TextDecoder } = require('node:util');
const { ServiceError } = require('./errors');

const utf8Decoder = new TextDecoder('utf-8', { fatal: true });
const ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;
const HEX_64 = /^[0-9a-fA-F]{64}$/;
const HEX_8 = /^[0-9a-fA-F]{8}$/;
const HEX = /^[0-9a-fA-F]+$/;

class FrameDecoder {
    constructor(maxFrameBytes, maxBufferedBytes) {
        this.maxFrameBytes = maxFrameBytes;
        this.maxBufferedBytes = maxBufferedBytes;
        this.buffer = Buffer.alloc(0);
    }

    append(chunk) {
        if (!Buffer.isBuffer(chunk)) {
            throw new TypeError('decoder input must be a Buffer');
        }
        if (this.buffer.length + chunk.length > this.maxBufferedBytes) {
            throw new ServiceError('INPUT_BUFFER_LIMIT', 'too much buffered input', { fatal: true });
        }
        this.buffer = this.buffer.length === 0 ? chunk : Buffer.concat([this.buffer, chunk]);
    }

    next() {
        if (this.buffer.length < 4) {
            return null;
        }

        const length = this.buffer.readUInt32BE(0);
        if (length === 0 || length > this.maxFrameBytes) {
            throw new ServiceError(
                'INVALID_FRAME_LENGTH',
                `frame length must be between 1 and ${this.maxFrameBytes} bytes`,
                { fatal: true }
            );
        }
        if (this.buffer.length < 4 + length) {
            return null;
        }

        const body = this.buffer.subarray(4, 4 + length);
        this.buffer = this.buffer.subarray(4 + length);

        let text;
        try {
            text = utf8Decoder.decode(body);
        } catch {
            throw new ServiceError('INVALID_UTF8', 'frame is not valid UTF-8', { fatal: true });
        }

        let value;
        try {
            value = JSON.parse(text);
        } catch {
            throw new ServiceError('INVALID_JSON', 'frame is not valid JSON', { fatal: true });
        }
        if (!isPlainObject(value)) {
            throw new ServiceError('INVALID_REQUEST', 'request must be a JSON object', { fatal: true });
        }

        return value;
    }

    hasIncompleteFrame() {
        if (this.buffer.length === 0) {
            return false;
        }
        if (this.buffer.length < 4) {
            return true;
        }
        const length = this.buffer.readUInt32BE(0);
        return length > 0 && length <= this.maxFrameBytes && this.buffer.length < 4 + length;
    }

    hasProcessableFrame() {
        if (this.buffer.length < 4) {
            return false;
        }
        const length = this.buffer.readUInt32BE(0);
        return length === 0 || length > this.maxFrameBytes || this.buffer.length >= 4 + length;
    }
}

function encodeFrame(value, maxFrameBytes) {
    const body = Buffer.from(JSON.stringify(value), 'utf8');
    if (body.length === 0 || body.length > maxFrameBytes) {
        throw new Error('encoded response exceeds frame limit');
    }
    const frame = Buffer.allocUnsafe(4 + body.length);
    frame.writeUInt32BE(body.length, 0);
    body.copy(frame, 4);
    return frame;
}

function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value) &&
        Object.getPrototypeOf(value) === Object.prototype;
}

function validId(value) {
    return (typeof value === 'string' && ID_PATTERN.test(value)) ||
        (Number.isSafeInteger(value) && value >= 0);
}

function validateBase(request) {
    if (request.v !== 1) {
        throw new ServiceError('UNSUPPORTED_VERSION', 'v must be 1');
    }
    if (!validId(request.id)) {
        throw new ServiceError('INVALID_ID', 'id must be a nonnegative safe integer or a 1-128 character token');
    }
    if (typeof request.op !== 'string') {
        throw new ServiceError('INVALID_OPERATION', 'op must be a string');
    }
    return request.id;
}

function requireExactKeys(request, allowed) {
    for (const key of Object.keys(request)) {
        if (!allowed.has(key)) {
            throw new ServiceError('UNKNOWN_FIELD', `field is not allowed for ${request.op}: ${key}`);
        }
    }
}

function requireSeed(value) {
    if (typeof value !== 'string' || !HEX_64.test(value)) {
        throw new ServiceError('INVALID_SEED_HASH', 'seed_hash must be exactly 64 hexadecimal characters');
    }
    return value.toLowerCase();
}

function requireClaim(value) {
    if (typeof value !== 'string' || !HEX_64.test(value)) {
        throw new ServiceError('INVALID_CLAIMED_HASH', 'claimed_hash must be exactly 64 hexadecimal characters');
    }
    return value.toLowerCase();
}

function requireBlob(value, maxBlobBytes) {
    if (typeof value !== 'string' || value.length === 0 || (value.length & 1) !== 0 || !HEX.test(value)) {
        throw new ServiceError('INVALID_BLOB', 'blob must be nonempty, even-length hexadecimal');
    }
    if (value.length / 2 > maxBlobBytes) {
        throw new ServiceError('INVALID_BLOB', `blob exceeds ${maxBlobBytes} bytes`);
    }
    return value.toLowerCase();
}

function requireMetadataId(name, value) {
    if (!validId(value)) {
        throw new ServiceError(`INVALID_${name.toUpperCase()}`, `${name} must be a bounded token or nonnegative safe integer`);
    }
    return value;
}

function validateOperation(request, maxBlobBytes) {
    const base = new Set(['v', 'id', 'op']);
    switch (request.op) {
    case 'hello':
        requireExactKeys(request, new Set([...base, 'client']));
        if (Object.hasOwn(request, 'client') && request.client !== 'xmrig-proxy') {
            throw new ServiceError('INVALID_CLIENT', 'client must be "xmrig-proxy" when supplied');
        }
        return {};
    case 'ping':
    case 'stats':
        requireExactKeys(request, base);
        return {};
    case 'prepare_seed':
        requireExactKeys(request, new Set([...base, 'seed_hash', 'mode', 'allow_light_fallback']));
        if (Object.hasOwn(request, 'mode') && request.mode !== 'fast') {
            throw new ServiceError('INVALID_MODE', 'only mode "fast" is supported');
        }
        if (Object.hasOwn(request, 'allow_light_fallback') && request.allow_light_fallback !== false) {
            throw new ServiceError('LIGHT_FALLBACK_FORBIDDEN', 'light-mode fallback is not supported');
        }
        return { seedHash: requireSeed(request.seed_hash) };
    case 'release_seed':
        requireExactKeys(request, new Set([...base, 'seed_hash']));
        return { seedHash: requireSeed(request.seed_hash) };
    case 'verify':
        requireExactKeys(request, new Set([
            ...base,
            'seed_hash',
            'blob',
            'claimed_hash',
            'job_id',
            'nonce',
            'share_id'
        ]));
        if (typeof request.nonce !== 'string' || !HEX_8.test(request.nonce)) {
            throw new ServiceError('INVALID_NONCE', 'nonce must be exactly 8 hexadecimal characters');
        }
        return {
            seedHash: requireSeed(request.seed_hash),
            blob: requireBlob(request.blob, maxBlobBytes),
            claimedHash: requireClaim(request.claimed_hash),
            jobId: requireMetadataId('job_id', request.job_id),
            nonce: request.nonce.toLowerCase(),
            shareId: requireMetadataId('share_id', request.share_id)
        };
    default:
        throw new ServiceError('UNKNOWN_OPERATION', `unsupported operation: ${request.op}`);
    }
}

function idKey(id) {
    return `${typeof id}:${id}`;
}

function success(id, fields = {}) {
    return { v: 1, id, ok: true, ...fields };
}

function failure(id, error) {
    return {
        v: 1,
        id: validId(id) ? id : null,
        ok: false,
        error: error.message || 'internal verifier error',
        code: error.code || 'INTERNAL_ERROR',
        retryable: error.retryable === true
    };
}

module.exports = {
    FrameDecoder,
    encodeFrame,
    validateBase,
    validateOperation,
    validId,
    idKey,
    success,
    failure
};
