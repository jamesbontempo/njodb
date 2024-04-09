"use strict";

const {
    mkdir,
    mkdirSync,
    rmdir,
    rmdirSync
} = require("graceful-fs");

const { promisify } = require("util");

const { Sleeper } = require("./sleeper");

const getLockFilePath = (filepath) => {
    return filepath + ".lck";
}

const lockFile = async (filepath) => {
    const sleeper = new Sleeper(100);

    while (true) {
        try {
            await promisify(mkdir)(getLockFilePath(filepath));
            return async () => { unlockFile(filepath) };
        } catch (e) {
            if (e.code === "EEXIST") {
                await sleeper.sleep();
            } else {
                throw e;
            }
        }
    }
}

const lockFileSync = (filepath) => {
    const sleeper = new Sleeper(100);

    while (true) {
        try {
            mkdirSync(getLockFilePath(filepath));
            return () => { unlockFileSync(filepath); }
        } catch (e) {
            if (e.code === "EEXIST") {
                sleeper.sleepSync();
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