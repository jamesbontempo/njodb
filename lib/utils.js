"use strict";

const {
    rename,
    renameSync,
    rmdir,
    rmdirSync,
    unlink,
    unlinkSync
} = require("fs");

const { promisify } = require("util");

const min = (a, b) => {
    if (b === undefined || a <= b) return a;
    return b;
};

const max = (a, b) => {
    if (b === undefined || a > b) return a;
    return b;
};

const convertSize = (size) => {
    const sizes = ["bytes", "KB", "MB", "GB"];

    var index = Math.floor(Math.log2(size)/10);
    if (index > 3) index = 3;

    return Math.round(((size / Math.pow(1024, index)) + Number.EPSILON) * 100) / 100 + " " + sizes[index];
};

const moveFile = async (a, b) => {
    var results = {
        moved: false,
        a: a,
        b: b,
        start: Date.now(),
        end: undefined
    }

    await promisify(rename)(a, b);

    results.moved = true;
    results.end = Date.now();

    return results;
};

const moveFileSync = (a, b) => {
    var results = {
        moved: false,
        a: a,
        b: b,
        start: Date.now(),
        end: undefined
    }

    renameSync(a, b);

    results.moved = true;
    results.end = Date.now();

    return results;
};

const replaceFile = async (a, b) => {
    var results = {
        replaced: false,
        a: a,
        b: b,
        start: Date.now(),
        end: undefined
    };

    await promisify(rename)(a, a + ".old");
    await promisify(rename)(b, a);
    await promisify(unlink)(a + ".old");

    results.replaced = true;
    results.end = Date.now();

    return results;
};

const replaceFileSync = (a, b) => {
    var results = {
        replaced: false,
        a: a,
        b: b,
        start: Date.now(),
        end: undefined
    };

    renameSync(a, a + ".old");
    renameSync(b, a);
    unlinkSync(a + ".old");

    results.replaced = true;
    results.end = Date.now();

    return results;
};

const deleteFile = async (filepath) => {
    var results = {
        deleted: false,
        file: filepath,
        start: Date.now(),
        end: undefined
    };

    await promisify(unlink)(filepath);

    results.deleted = true;
    results.end = Date.now();

    return results;
};

const deleteFileSync = (filepath) => {
    var results = {
        deleted: false,
        file: filepath,
        start: Date.now(),
        end: undefined
    };

    unlinkSync(filepath);

    results.deleted = true;
    results.end = Date.now();

    return results;
}

const deleteDirectory = async (dirpath) => {
    let results = {
        deleted: false,
        directory: dirpath,
        start: Date.now(),
        end: undefined
    };

    await promisify(rmdir)(dirpath);

    results.deleted = true;
    results.end = Date.now();

    return results;
};

const deleteDirectorySync = (dirpath) => {
    var results = {
        deleted: false,
        directory: dirpath,
        start: Date.now(),
        end: undefined
    };

    rmdirSync(dirpath);

    results.deleted = true;
    results.end = Date.now();

    return results;
};

exports.min = min;
exports.max = max;

exports.convertSize = convertSize;

exports.moveFile = moveFile;
exports.moveFileSync = moveFileSync;
exports.replaceFile = replaceFile;
exports.replaceFileSync = replaceFileSync;
exports.deleteFile = deleteFile;
exports.deleteFileSync = deleteFileSync;
exports.deleteDirectory = deleteDirectory;
exports.deleteDirectorySync = deleteDirectorySync;