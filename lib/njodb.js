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

const dropStore = async (store, lockoptions) => {
    if (fs.existsSync(store)) {
        lockfile.lock(store, lockoptions).then(() => {
            utils.deleteFile(store)
            .then(results => {return results})
            .catch(error => {throw error});
        }).catch(error => {throw error});
    }
};

const statsStoreData = (store, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: store,
            size: 0,
            records: 0,
            created: undefined,
            modified: undefined,
            start: Date.now(),
            end: undefined
        };

        if (fs.existsSync(store)) {
            results.records = 0;
            const stats = fs.statSync(store);
            results.size = stats.size;
            results.created = stats.birthtime;
            results.modified = stats.mtime;

            lockfile.lock(store, lockoptions).then((release) => {
                const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

                readstream.on("line", () => {
                    results.records++;
                });

                readstream.on("close", () => {
                    results.end = Date.now()
                    release().then(() => resolve(results)).catch(error => reject(error));
                });
            }).catch(error => reject(error));
        } else {
            results.end = Date.now();
            resolve(results);
        }
    });
};

const distributeStoreData = async (datapath, dataname, storenames, temppath, datastores, lockoptions) => {
    var results = {
        stores: undefined,
        records: undefined,
        start: Date.now(),
        end: undefined
    };

    var storepaths = [];
    var tempstorepaths = [];

    var locks = [];

    for (let storename of storenames) {
        const storepath = path.join(datapath, storename);
        storepaths.push(storepath);
        locks.push(lockfile.lock(storepath, lockoptions));
    }

    await Promise.all(locks);

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

            reader.on("close", () => resolve({store: store, records: line}));

            reader.on("error", error => reject(error));
        }));
    }

    await Promise.all(writes);

    for (let writer of writers) {
        writer.end();
    }

    // delete old data files from datadir, move new ones to datadir

    var deletes = [];

    for (let storepath of storepaths) {
        deletes.push(utils.deleteFile(storepath));
    }

    await Promise.all(deletes);

    var moves = [];

    for (let i = 0; i < tempstorepaths.length; i++) {
        moves.push(utils.moveFile(tempstorepaths[i], path.join(datapath, [dataname, i, "json"].join("."))))
    }

    await Promise.all(moves);

    var unlocks = [];

    for (let storepath of storepaths) {
        if (fs.existsSync(storepath)) unlocks.push(lockfile.unlock(storepath, lockoptions));
    }

    await Promise.all(unlocks);

    results.stores = tempstorepaths.length,
    results.records = records;
    results.end = Date.now();

    return results;

};

const insertStoreData = (store, data, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: store,
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

const selectStoreData = (store, selecter, projecter, lockoptions) => {
    return new Promise ((resolve, reject) => {
        var results = {
            store: store,
            data: [],
            selected: 0,
            ignored: 0,
            start: Date.now(),
            end: undefined
        };

        var line = 0;
        var data = [];

        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

                reader.on ("line", (record) => {
                    line++;

                    try {
                        record = JSON.parse(record);
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("SELECT: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }

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

                });

                reader.on ("close", () => {
                    results.data = data;
                    results.end = Date.now();
                    release().then(() => resolve(results)).catch(error => reject(error));
                });
            }).catch(error => reject(error));
        } else {
            results.end = Date.now();
            resolve(results);
        }

    });

};

const deleteStoreData = (store, selecter, tempstore, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: store,
            deleted: 0,
            retained: 0,
            start: Date.now(),
            end: undefined
        };

        var line = 0;

        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                const writer = fs.createWriteStream(tempstore);

                reader.on ("line", (record) => {
                    line++;

                    try {
                        record = JSON.parse(record);
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("DELETE: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }

                    if (!selecter(record)) {
                        writer.write(JSON.stringify(record) + "\n");
                        results.retained++;
                    } else {
                        results.deleted++;
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
        } else {
            results.end = Date.now();
            resolve(results);
        }
    });
};

const updateStoreData = (store, selecter, updater, tempstore, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: store,
            updated: 0,
            unchanged: 0,
            start: Date.now(),
            end: undefined
        };

        var line = 0;

        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                const writer = fs.createWriteStream(tempstore);

                reader.on ("line", (record) => {
                    line++;

                    try {
                        record = JSON.parse(record);
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("UPDATE: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }

                    if (selecter(record)) {
                        record = updater(record);
                        results.updated++;
                    } else {
                        results.unchanged++;
                    }

                    writer.write(JSON.stringify(record) + "\n");
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
        } else {
            resolve(results);
        }
    });
};

const aggregateStoreData = (store, selecter, indexer, projecter, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = {
            store: store,
            aggregates: {},
            start: Date.now(),
            end: undefined
        };

        var line = 0;
        var aggregates = {};

        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

                readstream.on("line", (record) => {
                    line++;

                    try {
                        record = JSON.parse(record);
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("AGGREGATE: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }

                    if (selecter(record)) {
                        var index;

                        try {
                            index = indexer(record);
                        } catch {
                            console.error("AGGREGATE: Can't index record in " + store + " on line " + line + ": " + record);
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

                        for (var i = 0; i < fields.length; i++) {
                            if (projection[fields[i]] !== undefined) {
                                if (aggregates[index][fields[i]]) {
                                    if (projection[fields[i]] < aggregates[index][fields[i]]["min"]) aggregates[index][fields[i]]["min"] = projection[fields[i]];
                                    if (projection[fields[i]] > aggregates[index][fields[i]]["max"]) aggregates[index][fields[i]]["max"] = projection[fields[i]];
                                    aggregates[index][fields[i]]["count"]++;
                                    if (typeof projection[fields[i]] === "number") {
                                        const delta1 = projection[fields[i]] - aggregates[index][fields[i]]["mean"];
                                        aggregates[index][fields[i]]["sum"] += projection[fields[i]];
                                        aggregates[index][fields[i]]["mean"] += delta1 / aggregates[index][fields[i]]["count"];
                                        const delta2 = projection[fields[i]] - aggregates[index][fields[i]]["mean"];
                                        aggregates[index][fields[i]]["m2"] += delta1 * delta2;
                                    }
                                } else {
                                    aggregates[index][fields[i]] = {
                                        min: projection[fields[i]],
                                        max: projection[fields[i]],
                                        count: 1,
                                        sum: (typeof projection[fields[i]] === "number") ? projection[fields[i]] : undefined,
                                        mean: (typeof projection[fields[i]] === "number") ? projection[fields[i]] : undefined,
                                        m2: (typeof projection[fields[i]] === "number") ? 0 : undefined
                                    };
                                }
                            }
                        }
                    }
                });

                readstream.on("close", () => {
                    results.aggregates = aggregates;
                    results.end = Date.now();
                    release().then(() => resolve(results)).catch(error => reject(error));
                });
            }).catch(error => reject(error));
        } else {
            results.end = Date.now();
            resolve(results);
        }
    });
}

const indexStoreData = (store, field, lockoptions) => {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var index = {};

        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                readstream.on("line", (record) => {
                    line++;

                    try {
                        record = JSON.parse(record);
                        if (record[field] !== undefined) {
                            if (index[record[field]]) {
                                index[record[field]].push(line);
                            } else {
                                index[record[field]] = [line];
                            }
                        }
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("INDEX: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }

                });

                readstream.on("close", () => {
                    release()
                        .then(() => {
                            resolve(
                                {
                                    store: store,
                                    field: field,
                                    index: index,
                                    start: start,
                                    end: Date.now()
                                }
                            );

                        })
                        .catch(error => {
                            reject(error);
                        });
                });
            }).catch((error) => {
                reject (error);
            });
        }
    });
};

exports.getStoreNames = getStoreNames;
exports.distributeStoreData = distributeStoreData;
exports.dropStore = dropStore;
exports.selectStoreData = selectStoreData;
exports.insertStoreData = insertStoreData;
exports.updateStoreData = updateStoreData;
exports.deleteStoreData = deleteStoreData;
exports.aggregateStoreData = aggregateStoreData;
exports.statsStoreData = statsStoreData;
exports.indexStoreData = indexStoreData;