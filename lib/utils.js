"use strict";

const fs = require("fs");
const util = require("util");

const min = (a, b) => {
    if (b === undefined || a <= b) return a;
    return b;
};

const max = (a, b) => {
    if (b === undefined || a > b) return a;
    return b;
};

const filterStoreNames = (files, dataname) => {
    var storenames = [];
    const re = new RegExp("^" + [dataname, "\\d+", "json"].join(".") + "$");
    for (const file of files) {
        if (re.test(file)) storenames.push(file);
    }
    return storenames;
};

const getStoreNames = async (datapath, dataname) => {
    const files = await util.promisify(fs.readdir)(datapath);
    return filterStoreNames(files, dataname);
}

const getStoreNamesSync = (datapath, dataname) => {
    const files = fs.readdirSync(datapath);
    return filterStoreNames(files, dataname);
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

    await util.promisify(fs.rename)(a, b);

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

    fs.renameSync(a, b);

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

    await util.promisify(fs.rename)(a, a + ".old");
    await util.promisify(fs.rename)(b, a);
    await util.promisify(fs.unlink)(a + ".old");

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

    fs.renameSync(a, a + ".old");
    fs.renameSync(b, a);
    fs.unlinkSync(a + ".old");

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

    await util.promisify(fs.unlink)(filepath);

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

    fs.unlinkSync(filepath);

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

    await util.promisify(fs.rmdir)(dirpath);

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

    fs.rmdirSync(dirpath);

    results.deleted = true;
    results.end = Date.now();

    return results;
};

exports.min = min;
exports.max = max;

exports.filterStoreNames = filterStoreNames;

exports.getStoreNames = getStoreNames;
exports.getStoreNamesSync = getStoreNamesSync;

exports.convertSize = convertSize;

exports.moveFile = moveFile;
exports.moveFileSync = moveFileSync;

exports.replaceFile = replaceFile;
exports.replaceFileSync = replaceFileSync;

exports.deleteFile = deleteFile;
exports.deleteFileSync = deleteFileSync;

exports.deleteDirectory = deleteDirectory;
exports.deleteDirectorySync = deleteDirectorySync;