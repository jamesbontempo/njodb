const fs = require("fs");
const lockfile = require("proper-lockfile")

const checkValue = (name, value, exists, type, validater) => {
    if (exists && !value) throw new Error(name + " must be defined");

    if (value && type) {
        if (type === "array") {
            if (!Array.isArray(value)) throw new Error(name + " must be an array");
        } else if (type === "function") {
            if (!/\W+return\W+/.test(value.toString())) throw new Error(name + " must return a value");
        } else {
            if (typeof value !== type) throw new Error(name + " must be a " + type);
        }

        if (validater && typeof validater === "function" && !validater(value)) {
            value = (type === "function") ? value.toString() : JSON.stringify(value);
            throw new Error("invalid value for " + name + " (" + value + ")");
        }
    }

    return true;
};

const getRecordError = (method, message, store, line, record) => {
    const position = parseInt(message.match(/(\d+)$/)[0]) + 1 || 0;
    console.error(method + ": Problematic record in " + store + " on line " + line + " at position " + position + " (" + record + ")");
};

const min = (a, b) => {
    if (a <= b) return a;
    return b;
};

const max = (a, b) => {
    if (a > b) return a;
    return b;
};

const accumulateAggregate = (index, projection) => {
    index["min"] = min(index["min"], projection);
    index["max"] = max(index["max"], projection);
    index["count"]++;

    if (typeof projection === "number") {
        const delta1 = projection - index["mean"];
        index["sum"] += projection;
        index["mean"] += delta1 / index["count"];
        const delta2 = projection - index["mean"];
        index["m2"] += delta1 * delta2;
    }

    return index;
};

const convertSize = (size) => {
    var sizeString = "";

    if (size < 1024) {
        sizeString = size + " bytes";
    } else if (size < Math.pow(1024, 2)) {
        sizeString = Math.round(((size / 1024) + Number.EPSILON) * 100) / 100 + " KB";
    } else if (size < Math.pow(1024, 3)) {
        sizeString = Math.round(((size / Math.pow(1024, 2)) + Number.EPSILON) * 100) / 100 + " MB";
    } else {
        sizeString = Math.round(((size / Math.pow(1024, 3)) + Number.EPSILON) * 100) / 100 + " GB";
    }

    return sizeString;
};

const moveFile = (a, b) => {
    return new Promise((resolve, reject) => {
        var results = {
            moved: false,
            start: Date.now(),
            end: undefined
        }

        fs.rename(a, b, (error) => {
            if (error) reject(error);
            results.moved = true;
            results.end = Date.now();
            resolve(results);
        })
    });
};

const moveFileSync = (a, b, lock) => {
    var results = {
        moved: false,
        start: Date.now(),
        end: undefined
    }

    var release;

    if (lock) release = lockfile.lockSync(a);

    fs.renameSync(a, b);

    if (lock) release();

    results.moved = true;
    results.end = Date.now();

    return results;
};

const replaceFile = (a, b) => {
    return new Promise((resolve, reject) => {
        fs.rename (a, a + ".old", (error) => {
            if (error) reject(error);
            fs.rename (b, a, (error) => {
                if (error) {
                    fs.rename (a + ".old", a, (error) => {
                        if (error) reject(error);
                        fs.unlink(b, (error) => {
                            if (error) reject(error);
                        });
                    });
                    reject(error);
                }
                fs.unlink(a + ".old", (error) => {
                    if (error) reject(error);
                    resolve();
                });
            })
        });
    });
};

const replaceFileSync = (a, b, lock) => {
    var results = {
        moved: false,
        start: Date.now(),
        end: undefined
    };

    var release;

    if (lock) { release = lockfile.lockSync(a); }

    fs.renameSync(a, a + ".old");
    fs.renameSync(b, a);
    fs.unlinkSync(a + ".old");

    if (lock) release();

    results.moved = true;
    results.end = Date.now();

    return results;
};

const deleteFile = (filepath, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            deleted: false,
            filepath: filepath,
            start: Date.now(),
            end: undefined
        };

        if (lockoptions) {
            lockfile.lock(filepath, lockoptions).then((release) => {
                fs.unlink(filepath, (error) => {
                    if (error) reject(error);
                    results.deleted = true;
                    results.end = Date.now();
                    release().then(() => resolve(results)).catch(error => reject(error));
                });
            });
        } else {
            fs.unlink(filepath, (error) => {
                if (error) reject(error);
                results.deleted = true;
                results.end = Date.now();
                resolve(results);
            });
        }
    });
};

const deleteFileSync = (filepath, lock) => {
    var results = {
        deleted: false,
        filepath: filepath,
        start: Date.now(),
        end: undefined
    };

    var release;

    if (lock) release = lockfile.lockSync(filepath);

    fs.unlinkSync(filepath);

    if (lock) release();

    results.deleted = true;
    results.end = Date.now();

    return results;
}

const deleteDirectory = (dirpath, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            deleted: false,
            dirpath: dirpath,
            start: Date.now(),
            end: undefined
        };

        if (lockoptions) {
            lockfile.lock(dirpath, lockoptions).then((release) => {
                fs.rmdir(dirpath, (error) => {
                    if (error) reject(error);
                    results.deleted = true;
                    results.end = Date.now();
                    release().then(() => resolve(results)).catch(error => reject(error));
                });
            });
        } else {
            fs.rmdir(dirpath, (error) => {
                if (error) reject(error);
                results.deleted = true;
                results.end = Date.now();
                resolve(results);
            });
        }
    })
};

const deleteDirectorySync = (dirpath, lock) => {
    var results = {
        deleted: false,
        dirpath: dirpath,
        start: Date.now(),
        end: undefined
    };

    var release;

    if (lock) release = lockfile.lockSync(dirpath);

    fs.rmdirSync(dirpath);

    if (lock) release();

    results.deleted = true;
    results.end = Date.now();

    return results;
};

exports.checkValue = checkValue;
exports.getRecordError = getRecordError;
exports.min = min;
exports.max = max;
exports.accumulateAggregate = accumulateAggregate;
exports.convertSize = convertSize;

exports.moveFile = moveFile;
exports.moveFileSync = moveFileSync;

exports.replaceFile = replaceFile;
exports.replaceFileSync = replaceFileSync;

exports.deleteFile = deleteFile;
exports.deleteFileSync = deleteFileSync;

exports.deleteDirectory = deleteDirectory;
exports.deleteDirectorySync = deleteDirectorySync;