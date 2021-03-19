const fs = require("fs");
const util = require("util");

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

const parseRecord = (record) => {
    try {
        return JSON.parse(record.trim());
    } catch {
        return undefined;
    }
}

const accumulateAggregate = (index, projection) => {
    index["min"] = min(index["min"], projection);
    index["max"] = max(index["max"], projection);
    index["count"]++;

    // Welford's algorithm
    if (typeof projection === "number") {
        const delta1 = projection - index["mean"];
        index["sum"] += projection;
        index["mean"] += delta1 / index["count"];
        const delta2 = projection - index["mean"];
        index["m2"] += delta1 * delta2;
    }

    return index;
};

const reduceAggregates = (aggregate, result) => {
    const n = aggregate["count"] + result["count"];

    aggregate["min"] = min(aggregate["min"], result["min"]);
    aggregate["max"] = max(aggregate["max"], result["max"]);

    // Parallel version of Welford's algorithm
    if (result["m2"] !== undefined) {
        const delta = result["mean"] - aggregate["mean"];
        const m2 = aggregate["m2"] + result["m2"] + (Math.pow(delta, 2) * ((aggregate["count"] * result["count"]) / n));
        aggregate["m2"] = m2;
        aggregate["varp"] = m2 / n;
        aggregate["vars"] = m2 / (n - 1);
        aggregate["stdp"] = Math.sqrt(m2 / n);
        aggregate["stds"] = Math.sqrt(m2 / (n - 1));
    }

    if (result["sum"] !== undefined) {
        aggregate["mean"] = (aggregate["sum"] + result["sum"]) / n;
        aggregate["sum"] += result["sum"];
    }

    aggregate["count"] = n;

    return aggregate;
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

exports.checkData = checkData;

exports.min = min;
exports.max = max;

exports.filterStoreNames = filterStoreNames;

exports.parseRecord = parseRecord;

exports.accumulateAggregate = accumulateAggregate;
exports.reduceAggregates = reduceAggregates;

exports.convertSize = convertSize;

exports.moveFile = moveFile;
exports.moveFileSync = moveFileSync;

exports.replaceFile = replaceFile;
exports.replaceFileSync = replaceFileSync;

exports.deleteFile = deleteFile;
exports.deleteFileSync = deleteFileSync;

exports.deleteDirectory = deleteDirectory;
exports.deleteDirectorySync = deleteDirectorySync;