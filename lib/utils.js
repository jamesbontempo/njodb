const fs = require("fs");
const util = require("util");

const checkValue = (name, value, exists, type, validater) => {
    if (exists === true && value === undefined) throw new Error(name + " must be defined");

    if (value !== undefined && type) {
        if (type === "array") {
            if (!Array.isArray(value)) throw new Error(name + " must be an array");
        } else if (type === "function") {
            const functionString = value.toString();
            if (/\s*function/.test(functionString) && !/\W+return\W+/.test(functionString)) throw new Error(name + " must return a value");
        } else {
            if (typeof value !== type) throw new Error(name + " must be a " + type);
        }

        if (validater && typeof validater === "function" && validater(value) === false) {
            value = (type === "function") ? value.toString() : JSON.stringify(value);
            throw new Error("invalid value for " + name + " (" + value + ")");
        }
    }

    return true;
};

const checkData = (data) => {
    var results = {
        inspected: 0,
        data: [],
        errors: [],
        blanks: 0
    }

    for (let i = 0; i < data.length; i++) {
        data[i] = data[i].trim()

        if (data[i].length > 0) {
            try {
                results.data.push(JSON.parse(data[i]));
            } catch {
                results.errors.push({line: i + 1, data: data[i]});
            }
        } else {
            results.blanks++;
        }

        results.inspected++;
    }

    return results;
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

exports.checkValue = checkValue;
exports.checkData = checkData;

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