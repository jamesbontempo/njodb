const fs = require("fs");
const lockfile = require("proper-lockfile")

const checkValue = (name, value, exists, type, validater) => {
    if (exists && !value) throw new Error(name + " must be defined");

    if (value && type) {
        if (type === "array") {
            if (!Array.isArray(value)) throw new Error(name + " must be an array");
        } else {
            if (typeof value !== type) throw new Error(name + " must be a " + type);
        }

        if (validater && typeof validater === "function" && !validater(value)) {
            throw new Error("invalid value for " + name + " (" + JSON.stringify(value) + ")");
        }
    }

    return true;
};

const moveFile = (a, b) => {
    return new Promise((resolve, reject) => {
        fs.rename(a, b, (error) => {
            if (error) reject(error);
            resolve({});
        })
    });
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

const deleteFile = (filepath, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            deleted: false,
            filepath: filepath,
            start: Date.now(),
            end: undefined
        };

        if (fs.existsSync(filepath)) {
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
        } else {
            results.end = Date.now();
            resolve(results);
        }
    });
};

const deleteDirectory = (dirpath, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            deleted: false,
            dirpath: dirpath,
            start: Date.now(),
            end: undefined
        };

        if (fs.existsSync(dirpath)) {
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
        } else {
            results.end = Date.now();
            resolve(results);
        }
    })
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

exports.checkValue = checkValue;
exports.moveFile = moveFile;
exports.replaceFile = replaceFile;
exports.deleteFile = deleteFile;
exports.deleteDirectory = deleteDirectory;
exports.convertSize = convertSize;