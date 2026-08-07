'use strict';

const fs = require('node:fs/promises');
const crypto = require('node:crypto');
const net = require('node:net');
const path = require('node:path');

async function prepareSocketPath(socketPath) {
    if (!path.isAbsolute(socketPath)) {
        throw new Error('Unix socket path must be absolute');
    }

    const parent = path.dirname(socketPath);
    const [parentStat, resolvedParent] = await Promise.all([
        fs.lstat(parent),
        fs.realpath(parent)
    ]);
    if (!parentStat.isDirectory() || resolvedParent !== parent) {
        throw new Error(`socket parent must be a real directory without symlinks: ${parent}`);
    }
    if ((parentStat.mode & 0o022) !== 0) {
        throw new Error(`socket parent must not be group- or world-writable: ${parent}`);
    }
    if (typeof process.geteuid === 'function' && parentStat.uid !== process.geteuid()) {
        throw new Error(`socket parent must be owned by uid ${process.geteuid()}: ${parent}`);
    }

    let initial;
    try {
        initial = await fs.lstat(socketPath);
    } catch (error) {
        if (error.code === 'ENOENT') {
            return;
        }
        throw error;
    }

    if (!initial.isSocket()) {
        throw new Error(`refusing to replace non-socket path: ${socketPath}`);
    }
    if (typeof process.geteuid === 'function' && initial.uid !== process.geteuid()) {
        throw new Error(`refusing to replace socket owned by uid ${initial.uid}`);
    }

    const active = await socketAcceptsConnections(socketPath);
    if (active) {
        const error = new Error(`another verifier is already listening at ${socketPath}`);
        error.code = 'EADDRINUSE';
        throw error;
    }

    const current = await fs.lstat(socketPath);
    if (!current.isSocket() || current.dev !== initial.dev || current.ino !== initial.ino) {
        throw new Error('socket path changed while checking stale endpoint; refusing to unlink');
    }
    await fs.unlink(socketPath);
}

function makePrivateBindPath(socketPath) {
    const parent = path.dirname(socketPath);
    const suffix = crypto.randomBytes(6).toString('hex');
    return path.join(parent, `.xrv-bind-${process.pid}-${suffix}`);
}

function socketAcceptsConnections(socketPath) {
    return new Promise((resolve, reject) => {
        const socket = net.createConnection({ path: socketPath });
        let settled = false;
        const timer = setTimeout(() => finishReject(new Error('timed out probing existing socket')), 500);
        timer.unref();

        const cleanup = () => {
            clearTimeout(timer);
            socket.removeAllListeners();
            socket.destroy();
        };
        const finish = (value) => {
            if (settled) return;
            settled = true;
            cleanup();
            resolve(value);
        };
        const finishReject = (error) => {
            if (settled) return;
            settled = true;
            cleanup();
            reject(error);
        };

        socket.once('connect', () => finish(true));
        socket.once('error', (error) => {
            if (error.code === 'ECONNREFUSED' || error.code === 'ENOENT') {
                finish(false);
            } else {
                finishReject(error);
            }
        });
    });
}

async function captureBoundSocket(socketPath, mode) {
    const initial = await fs.lstat(socketPath);
    const identity = { dev: initial.dev, ino: initial.ino };
    try {
        if (!initial.isSocket()) {
            throw new Error('bound Unix socket path is no longer a socket');
        }
        await fs.chmod(socketPath, mode);
        const stat = await fs.lstat(socketPath);
        if (!stat.isSocket() || stat.dev !== identity.dev || stat.ino !== identity.ino ||
            (stat.mode & 0o777) !== mode) {
            throw new Error(`failed to secure Unix socket mode ${mode.toString(8)}`);
        }
        return identity;
    } catch (error) {
        error.socketIdentity = identity;
        throw error;
    }
}

async function publishBoundSocket(bindPath, socketPath, identity) {
    // link(2) is atomic and refuses an existing destination. The server remains
    // bound to the same socket inode after the private name is removed.
    await fs.link(bindPath, socketPath);
    try {
        const published = await fs.lstat(socketPath);
        if (!published.isSocket() || published.dev !== identity.dev || published.ino !== identity.ino) {
            throw new Error('published Unix socket identity does not match bound socket');
        }
        await fs.unlink(bindPath);
    } catch (error) {
        await unlinkIfSameSocket(socketPath, identity).catch(() => {});
        throw error;
    }
}

async function unlinkIfSameSocket(socketPath, identity) {
    if (!identity) {
        return false;
    }
    let current;
    try {
        current = await fs.lstat(socketPath);
    } catch (error) {
        if (error.code === 'ENOENT') {
            return false;
        }
        throw error;
    }
    if (!current.isSocket() || current.dev !== identity.dev || current.ino !== identity.ino) {
        return false;
    }
    await fs.unlink(socketPath);
    return true;
}

module.exports = {
    prepareSocketPath,
    makePrivateBindPath,
    captureBoundSocket,
    publishBoundSocket,
    unlinkIfSameSocket
};
