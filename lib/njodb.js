const fs = require("fs");
const path = require("path");
const rl = require("readline");
const lockfile = require("proper-lockfile");
const utils = require("./utils");

const getStoreNames = (datapath, dataname) => {
    return new Promise ((resolve, reject) => {
        var storenames = [];
        const re = new RegExp("^" + [dataname, "\\d+", "json"].join(".") + "$");
        fs.readdir(datapath, (error, files) => {
            if (error) reject(error);
            for (const file of files) {
                if (re.test(file)) storenames.push(file);
            }
            resolve(storenames);
        });
    });
}

const getStoreNamesSync = (datapath, dataname) => {
    var storenames = [];
    const re = new RegExp("^" + [dataname, "\\d+", "json"].join(".") + "$");
    const files = fs.readdirSync(datapath);
    for (const file of files) {
        if (re.test(file)) storenames.push(file);
    }
    return storenames;
};

const statsStoreData = (store, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: path.resolve(store),
            size: 0,
            records: 0,
            errors: [],
            created: undefined,
            modified: undefined,
            start: Date.now(),
            end: undefined
        };

        var line = 0;

        results.records = 0;
        const stats = fs.statSync(store);
        results.size = stats.size;
        results.created = stats.birthtime;
        results.modified = stats.mtime;

        lockfile.lock(store, lockoptions).then((release) => {
            const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

            readstream.on("line", (record) => {
                line++;
                try {
                    record = JSON.parse(record);
                    results.records++;
                } catch {
                    results.errors.push({line: line, data: record});
                }
            });

            readstream.on("close", () => {
                results.end = Date.now()
                release().then(() => resolve(results)).catch(error => reject(error));
            });
        }).catch(error => reject(error));
    });
};

const statsStoreDataSync = (store) => {
    var results = {
        store: path.resolve(store),
        size: 0,
        records: 0,
        errors: [],
        created: undefined,
        modified: undefined,
        start: Date.now(),
        end: undefined
    };

    var line = 0;

    results.records = 0;
    const stats = fs.statSync(store);
    results.size = stats.size;
    results.created = stats.birthtime;
    results.modified = stats.mtime;

    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");
    release();

    const data = file.split("\n");

    for (var record of data) {
        line++;
        if (record.length > 0) {
            try {
                record = JSON.parse(record);
                results.records++;
            } catch {
                record = {error: record};
                results.errors.push({line: line, data: record});
            }
        }
    }

    results.end = Date.now();
    return results;
};

const distributeStoreData = async (datapath, dataname, storenames, temppath, datastores, lockoptions) => {
    var results = {
        stores: undefined,
        records: undefined,
        start: Date.now(),
        end: undefined,
        elapsed: undefined
    };

    var storepaths = [];
    var tempstorepaths = [];

    var locks = [];

    for (let storename of storenames) {
        const storepath = path.join(datapath, storename);
        storepaths.push(storepath);
        locks.push(lockfile.lock(storepath, lockoptions));
    }

    const releases = await Promise.all(locks);

    var writes = [];
    var writers = [];
    var records = 0;

    for (let i = 0; i < datastores; i++) {
        const tempstorepath = path.join(temppath, [dataname, i, results.start, "json"].join("."));
        tempstorepaths.push(tempstorepath);
        fs.writeFileSync(tempstorepath, "");
        writers.push(fs.createWriteStream(tempstorepath, {flags: "r+"}));
    }

    for (let storename of storenames) {
        writes.push(new Promise((resolve, reject) => {
            var line = 0;
            const store = path.join(datapath, storename);
            var stores;

            const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

            reader.on("line", record => {
                if (line % datastores === 0) stores = Array.from(Array(datastores).keys());
                const storenumber = stores.splice(Math.floor(Math.random()*stores.length), 1)[0];
                writers[storenumber].write(record + "\n");
                line++;
                records++;
            });

            reader.on("close", () => {
                resolve({store: store, records: line});
            });

            reader.on("error", error => {
                reject(error);
            });
        }));
    }

    await Promise.all(writes);

    for (let writer of writers) {
        writer.end();
    }

    var deletes = [];

    for (let storepath of storepaths) {
        deletes.push(utils.deleteFile(storepath));
    }

    await Promise.all(deletes);

    for (const release of releases) {
        release();
    }

    var moves = [];

    for (let i = 0; i < tempstorepaths.length; i++) {
        moves.push(utils.moveFile(tempstorepaths[i], path.join(datapath, [dataname, i, "json"].join("."))))
    }

    await Promise.all(moves);

    results.stores = tempstorepaths.length,
    results.records = records;
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;

};

const distributeStoreDataSync = (datapath, dataname, storenames, temppath, datastores) => {
    var results = {
        stores: undefined,
        records: undefined,
        start: Date.now(),
        end: undefined,
        elapsed: undefined
    };

    var storepaths = [];
    var tempstorepaths = [];

    var releases = [];
    var data = [];

    for (let storename of storenames) {
        const storepath = path.join(datapath, storename);
        storepaths.push(storepath);
        releases.push(lockfile.lockSync(storepath));
        const file = fs.readFileSync(storepath, "utf8").trimEnd();
        if (file.length > 0) data = data.concat(file.split("\n"));
    }

    var records = [];

    for (var i = 0; i < data.length; i++) {
        if (i === i % datastores) records[i] = [];
        records[i % datastores] += data[i] + "\n";
    }

    var stores = Array.from(Array(datastores).keys());

    for (var j = 0; j < records.length; j++) {
        const storenumber = stores.splice(Math.floor(Math.random()*stores.length), 1)[0];
        const tempstorepath = path.join(temppath, [dataname, storenumber, results.start, "json"].join("."));
        tempstorepaths.push(tempstorepath);
        fs.appendFileSync(tempstorepath, records[j]);
    }

    for (let storepath of storepaths) {
        utils.deleteFileSync(storepath);
    }

    for (const release of releases) {
        release();
    }

    for (let i = 0; i < tempstorepaths.length; i++) {
        utils.moveFileSync(tempstorepaths[i], path.join(datapath, [dataname, i, "json"].join(".")));
    }

    results.stores = tempstorepaths.length,
    results.records = data.length;
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;

};

const insertStoreData = (store, data, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: path.resolve(store),
            inserted: 0,
            start: Date.now(),
            end: undefined
        };

        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                fs.appendFile(store, data, "utf8", (error) => {
                    if (error) reject(error);
                    results.inserted = data.split("\n").length - 1;
                    results.end = Date.now();
                    release().then(() => resolve(results)).catch(error => reject(error));
                });
            }).catch(error => reject(error));
        } else {
            fs.appendFile(store, data, "utf8", (error) => {
                if (error) reject(error);
                results.inserted = data.split("\n").length - 1;
                results.end = Date.now();
                resolve(results);
            });
        }
    });
};

const insertStoreDataSync = (store, data) => {
    var results = {
        store: path.resolve(store),
        inserted: 0,
        start: Date.now(),
        end: undefined
    };

    if (fs.existsSync(store)) {
        const release = lockfile.lockSync(store);
        fs.appendFileSync(store, data, "utf8");
        release();
    } else {
        fs.appendFileSync(store, data, "utf8");
    }

    results.end = Date.now();
    results.inserted = data.split("\n").length - 1;

    return results;
};

const selectStoreData = (store, selecter, projecter, lockoptions) => {
    return new Promise ((resolve, reject) => {
        var results = {
            store: path.resolve(store),
            data: [],
            selected: 0,
            ignored: 0,
            errors: [],
            start: Date.now(),
            end: undefined
        };

        var line = 0;
        var data = [];

        lockfile.lock(store, lockoptions).then((release) => {
            const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

            reader.on ("line", (record) => {
                line++;

                try {
                    record = JSON.parse(record);
                    if (selecter(record)) {
                        if (projecter) {
                            data.push(projecter(record));
                        } else {
                            data.push(record);
                        }
                        results.selected++;
                    } else {
                        results.ignored++;
                    }
                } catch (error) {
                    results.errors.push({line: line, data: record});
                }
            });

            reader.on ("close", () => {
                results.data = data;
                results.end = Date.now();
                release().then(() => resolve(results)).catch(error => reject(error));
            });
        }).catch(error => reject(error));
    });
};

const selectStoreDataSync = (store, selecter, projecter) => {
    var results = {
        store: path.resolve(store),
        data: [],
        selected: 0,
        ignored: 0,
        errors: [],
        start: Date.now(),
        end: undefined
    };

    var line = 0;
    var data = [];

    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");
    release();

    const records = file.split("\n");

    for (var record of records) {
        if (record.length > 0) {
            line++;

            try {
                record = JSON.parse(record);
                if (selecter(record)) {
                    if (projecter) {
                        data.push(projecter(record));
                    } else {
                        data.push(record);
                    }
                    results.selected++;
                } else {
                    results.ignored++;
                }
            } catch (error) {
                results.errors.push({line: line, data: record});
            }
        }
    }

    results.data = data;
    results.end = Date.now();

    return results;
};

const aggregateStoreData = (store, selecter, indexer, projecter, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: path.resolve(store),
            aggregates: {},
            unindexed: 0,
            errors: [],
            start: Date.now(),
            end: undefined
        };

        var line = 0;
        var aggregates = {};

        lockfile.lock(store, lockoptions).then((release) => {
            const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

            readstream.on("line", (record) => {
                line++;

                try {
                    record = JSON.parse(record);

                    if (selecter(record)) {
                        var index;

                        try {
                            index = indexer(record);
                        } catch {
                            results.unindexed++;
                        }

                        var projection;
                        var fields;

                        if (projecter) {
                            projection = projecter(record);
                            fields = Object.keys(projection);
                        } else {
                            projection = record;
                            fields = Object.keys(record);
                        }

                        if (!aggregates[index]) aggregates[index] = {};

                        for (const field of fields) {
                            if (projection[field] !== undefined) {
                                if (aggregates[index][field]) {
                                    utils.accumulateAggregate(aggregates[index][field], projection[field]);
                                } else {
                                    aggregates[index][field] = {
                                        min: projection[field],
                                        max: projection[field],
                                        count: 1,
                                        sum: (typeof projection[field] === "number") ? projection[field] : undefined,
                                        mean: (typeof projection[field] === "number") ? projection[field] : undefined,
                                        m2: (typeof projection[field] === "number") ? 0 : undefined
                                    };
                                }
                            }
                        }
                    }
                } catch (error) {
                    results.errors.push({line: line, data: record});
                }
            });

            readstream.on("close", () => {
                results.aggregates = aggregates;
                results.end = Date.now();
                release().then(() => resolve(results)).catch(error => reject(error));
            });
        }).catch(error => reject(error));
    });
}

const aggregateStoreDataSync = (store, selecter, indexer, projecter) => {
    var results = {
        store: path.resolve(store),
        aggregates: {},
        indexed: 0,
        unindexed: 0,
        errors: [],
        start: Date.now(),
        end: undefined
    };

    var line = 0;
    var aggregates = {};

    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");
    release();

    const records = file.split("\n");

    for (var record of records) {
        if (record.length > 0) {
            line++;

            try {
                record = JSON.parse(record);

                if (selecter(record)) {
                    const index = indexer(record);

                    if (index === undefined) {
                        results.unindexed++;
                    } else {
                        var projection;
                        var fields;

                        if (projecter) {
                            projection = projecter(record);
                            fields = Object.keys(projection);
                        } else {
                            projection = record;
                            fields = Object.keys(record);
                        }

                        if (!aggregates[index]) aggregates[index] = {};

                        for (const field of fields) {
                            if (projection[field] !== undefined) {
                                if (aggregates[index][field]) {
                                    utils.accumulateAggregate(aggregates[index][field], projection[field]);
                                } else {
                                    aggregates[index][field] = {
                                        min: projection[field],
                                        max: projection[field],
                                        count: 1,
                                        sum: (typeof projection[field] === "number") ? projection[field] : undefined,
                                        mean: (typeof projection[field] === "number") ? projection[field] : undefined,
                                        m2: (typeof projection[field] === "number") ? 0 : undefined
                                    };
                                }
                            }
                        }

                        results.indexed++;
                    }
                }
            } catch (error) {
                results.errors.push({line: line, data: record});
            }

        }
    }

    results.aggregates = aggregates;
    results.end = Date.now();

    return results;
}

const updateStoreData = (store, selecter, updater, tempstore, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: path.resolve(store),
            updated: 0,
            unchanged: 0,
            errors: [],
            start: Date.now(),
            end: undefined
        };

        var line = 0;

        lockfile.lock(store, lockoptions).then((release) => {
            const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
            const writer = fs.createWriteStream(tempstore);

            reader.on ("line", (record) => {
                line++;

                try {
                    record = JSON.parse(record);

                    if (selecter(record)) {
                        record = updater(record)
                        results.updated++;
                    } else {
                        results.unchanged++;
                    }

                    writer.write(JSON.stringify(updater(record)) + "\n");

                } catch (error) {
                    results.errors.push({line: line, data: record});
                    writer.write(record);
                }
            });

            reader.on ("close", () => {
                results.end = Date.now();

                if (results.updated > 0) {
                    utils.replaceFile(store, tempstore).then(() => {
                        release().then(() => resolve(results)).catch(error => reject(error));
                    }).catch(error => reject(error));
                } else {
                    fs.unlink(tempstore, (error) => {
                        if (error) reject(error);
                        release().then(() => resolve(results)).catch(error => reject(error));
                    });
                }
            });
        }).catch(error => reject(error));
    });
};

const updateStoreDataSync = (store, selecter, updater, tempstore) => {
    var results = {
        store: path.resolve(store),
        updated: 0,
        unchanged: 0,
        errors: [],
        start: Date.now(),
        end: undefined
    };

    var line = 0;
    var data = [];

    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8").trimEnd();
    const records = file.split("\n");

    for (var record of records) {
        line++

        try {
            record = JSON.parse(record);

            if (selecter(record)) {
                record = updater(record);
                results.updated++;
            } else {
                results.unchanged++;
            }

            data.push(JSON.stringify(record));

        } catch (error) {
            results.errors.push({line: line, data: record});
            data.push(record);
        }
    }

    if (results.updated > 0) {
        fs.appendFileSync(tempstore, data.join("\n") + "\n", "utf8");
        utils.replaceFileSync(store, tempstore);
    }

    release();

    results.end = Date.now();
    return results;
};

const deleteStoreData = (store, selecter, tempstore, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: path.resolve(store),
            deleted: 0,
            retained: 0,
            errors: [],
            start: Date.now(),
            end: undefined
        };

        var line = 0;

        lockfile.lock(store, lockoptions).then((release) => {
            const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
            const writer = fs.createWriteStream(tempstore);

            reader.on ("line", (record) => {
                line++;

                try {
                    record = JSON.parse(record);
                    if (selecter(record)) {
                        results.deleted++;
                    } else {
                        writer.write(JSON.stringify(record) + "\n");
                        results.retained++;
                    }
                } catch (error) {
                    results.errors.push({line: line, data: record});
                    writer.write(record);
                }
            });

            reader.on ("close", () => {
                results.end = Date.now();

                if (results.deleted > 0) {
                    utils.replaceFile(store, tempstore)
                        .then(() => {
                            release().then(() => resolve(results)).catch(error => reject(error));
                        }).catch(error => reject(error));
                } else {
                    fs.unlink(tempstore, (error) => {
                        if (error) reject(error);
                        release().then(() => resolve(results)).catch(error => reject(error));
                    });
                }
            });
        }).catch(error => reject(error));
    });
};

const deleteStoreDataSync = (store, selecter, tempstore) => {
    var results = {
        store: path.resolve(store),
        deleted: 0,
        retained: 0,
        errors: [],
        start: Date.now(),
        end: undefined
    };

    var line = 0;
    var data = [];

    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");

    const records = file.split("\n");

    for (var record of records) {
        if (record.length > 0) {
            try {
                record = JSON.parse(record);
                if (selecter(record)) {
                    results.deleted++;
                } else {
                    data.push(JSON.stringify(record));
                    results.retained++;
                }
            } catch (error) {
                results.errors.push({line: line, data: record});
                data.push(record);
            }
        }
    }

    if (results.deleted > 0) {
        fs.appendFileSync(tempstore, data.join("\n") + "\n");
        utils.replaceFileSync(store, tempstore);
    }

    release();

    results.end = Date.now();
    return results;
};

exports.getStoreNames = getStoreNames;
exports.getStoreNamesSync = getStoreNamesSync;

exports.statsStoreData = statsStoreData;
exports.statsStoreDataSync = statsStoreDataSync;

exports.distributeStoreData = distributeStoreData;
exports.distributeStoreDataSync = distributeStoreDataSync;

exports.insertStoreData = insertStoreData;
exports.insertStoreDataSync = insertStoreDataSync;

exports.selectStoreData = selectStoreData;
exports.selectStoreDataSync = selectStoreDataSync;

exports.aggregateStoreData = aggregateStoreData;
exports.aggregateStoreDataSync = aggregateStoreDataSync;

exports.updateStoreData = updateStoreData;
exports.updateStoreDataSync = updateStoreDataSync;

exports.deleteStoreData = deleteStoreData;
exports.deleteStoreDataSync = deleteStoreDataSync;






