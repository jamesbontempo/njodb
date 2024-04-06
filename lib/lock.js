"use strict";

const {
    mkdir,
    mkdirSync,
    rmdir,
    rmdirSync
} = require("graceful-fs");

const { promisify } = require("util");

const sleep = (ms) => {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

const sleepSync = (ms) => {
    const start = new Date().getTime();
    let done = false;
    while (!done) {
        if(new Date().getTime() >= start + ms) done = true;
    }
}

const getLockFilePath = (filepath) => {
    return filepath + ".lck";
}

const lockFile = async (filepath) => {
    while (true) {
        try {
            await promisify(mkdir)(getLockFilePath(filepath));
            return async () => { unlockFile(filepath) };
        } catch (e) {
            if (e.code === "EEXIST") {
                await sleep(100);
            } else {
                throw e;
            }
        }
    }
}

const lockFileSync = (filepath) => {
    while (true) {
        try {
            mkdirSync(getLockFilePath(filepath));
            return () => { unlockFileSync(filepath); }
        } catch (e) {
            if (e.code === "EEXIST") {
                sleepSync(100);
            } else {
                throw e;
            }
        }
    }
}

const unlockFile = async (filepath) => {
    await promisify(rmdir)(getLockFilePath(filepath));
}

const unlockFileSync = (filepath) => {
    rmdirSync(getLockFilePath(filepath));
}

exports.lockFile = lockFile;
exports.lockFileSync = lockFileSync;