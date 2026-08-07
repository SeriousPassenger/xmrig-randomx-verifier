#!/usr/bin/env node
'use strict';

const { loadConfig } = require('./config');
const { VerifierServer } = require('./server');

async function main() {
    const config = loadConfig();
    const server = new VerifierServer(config);
    await server.start();

    let signalled = false;
    const stop = (signal) => {
        if (signalled) return;
        signalled = true;
        server.stop(signal).catch((error) => {
            console.error(JSON.stringify({
                time: new Date().toISOString(),
                level: 'error',
                message: 'graceful shutdown failed',
                error: error.message
            }));
            process.exitCode = 1;
        });
    };
    process.on('SIGTERM', () => stop('SIGTERM'));
    process.on('SIGINT', () => stop('SIGINT'));
    process.on('uncaughtException', (error) => {
        console.error(JSON.stringify({
            time: new Date().toISOString(),
            level: 'error',
            message: 'uncaught exception; draining verifier',
            error: error && error.message ? error.message : String(error)
        }));
        process.exitCode = 1;
        stop('uncaughtException');
    });
    process.on('unhandledRejection', (error) => {
        console.error(JSON.stringify({
            time: new Date().toISOString(),
            level: 'error',
            message: 'unhandled rejection; draining verifier',
            error: error && error.message ? error.message : String(error)
        }));
        process.exitCode = 1;
        stop('unhandledRejection');
    });
}

main().catch((error) => {
    console.error(JSON.stringify({
        time: new Date().toISOString(),
        level: 'error',
        message: 'verifier startup failed',
        error: error.message
    }));
    process.exitCode = 1;
});
