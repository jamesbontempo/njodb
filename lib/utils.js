"use strict";

const fs = require("fs");

const { promisify } = require("util");

const { join } = require("path");

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

const fileExists = async (a) => {
    try {
        await promisify(fs.access)(a, fs.constants.F_OK);
        return true;
    } catch (error) {
        return false;
    }
}

const fileExistsSync = (a) => {
    try {
        fs.accessSync(a, fs.constants.F_OK);
        return true;
    } catch (error) {
        return false;
    }
}

const copyFile = async (a, b) => {
    await promisify(fs.copyFile)(a, b);
}

const copyFileSync = (a, b) => {
    fs.copyFileSync(a, b);
}

const renameFile = async (a, b) => {
    await promisify(fs.rename)(a, b);
};

const renameFileSync = (a, b) => {
    fs.renameSync(a, b);
};

const deleteFile = async (filepath) => {
    try {
        await promisify(fs.unlink)(filepath);
    } catch(e) {
        if (e.code !== "ENOENT") throw e;
    }
};

const deleteFileSync = (filepath) => {
    try {
        fs.unlinkSync(filepath);
    } catch(e) {
        if (e.code !== "ENOENT") throw e;
    }
}

const replaceFile = async (a, b) => {
    await copyFile(a, a + ".old");

    try {
        await renameFile(b, a);
    } catch(e) {
        await renameFile(a + ".old", a);

        try {
            await deleteFile(b);
        } catch(e) {
            console.error(e);
        };

        throw e;
    }

    try {
        await deleteFile(a + ".old");
    } catch(e) {
        console.error(e);
    }
};

const replaceFileSync = (a, b) => {
    copyFileSync(a, a + ".old");

    try {
        renameFileSync(b, a);
    } catch(e) {
        renameFileSync(a + ".old", a);

        try {
            deleteFileSync(b);
        } catch(e) {
            console.error(e);
        }

        throw e;
    }

    deleteFileSync(a + ".old");
};

const deleteDirectory = async (dirpath) => {
    const entries = await promisify(fs.readdir)(dirpath, {withFileTypes: true});
    for (const entry of entries) {
        if (entry.isFile()) {
            await deleteFile(join(entry.path, entry.name));
        } else if (entry.isDirectory()) {
            await deleteDirectory(entry.path);
        }
    }
    await promisify(fs.rmdir)(dirpath);
};

const deleteDirectorySync = (dirpath) => {
    const entries = fs.readdirSync(dirpath, {withFileTypes: true});
    for (const entry of entries) {
        if (entry.isFile()) {
            deleteFileSync(join(entry.path, entry.name));
        } else if (entry.isDirectory()) {
            deleteDirectorySync(entry.path);
        }
    }
    fs.rmdirSync(dirpath);
};

exports.min = min;
exports.max = max;

exports.convertSize = convertSize;

exports.renameFile = renameFile;
exports.renameFileSync = renameFileSync;
exports.replaceFile = replaceFile;
exports.replaceFileSync = replaceFileSync;
exports.deleteFile = deleteFile;
exports.deleteFileSync = deleteFileSync;
exports.deleteDirectory = deleteDirectory;
exports.deleteDirectorySync = deleteDirectorySync;