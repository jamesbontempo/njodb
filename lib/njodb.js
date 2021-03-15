const fs = require("fs");
const path = require("path");
const rl = require("readline");
const util = require("util");
const lockfile = require("proper-lockfile");
const utils = require("./utils");
const objects = require("./objects");
const handlers = require("./handlers");

const getStoreNames = (datapath, dataname) => {
    return new Promise ((resolve, reject) => {
        fs.readdir(datapath, (error, files) => {
            if (error) reject(error);
            resolve(utils.filterStoreNames(files, dataname));
        });
    });
}

const getStoreNamesSync = (datapath, dataname) => {
    const files = fs.readdirSync(datapath);
    return utils.filterStoreNames(files, dataname);
};

const statsStoreData = async (store, lockoptions) => {
    const release = await lockfile.lock(store, lockoptions);

    const handlerResults = await new Promise((resolve, reject) => {
        const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
        const handler = new handlers.statsHandler();

        reader.on("line", record => handler.next(record));
        reader.on("close", () => resolve(handler.return()));
        reader.on("error", error => reject(error));
    });

    release();

    var results = Object.assign({store: path.resolve(store)}, handlerResults)

    const stats = await util.promisify(fs.stat)(store);
    results.size = stats.size;
    results.created = stats.birthtime;
    results.modified = stats.mtime;

    results.end = Date.now()

    return results;
};

const statsStoreDataSync = (store) => {
    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");

    release();

    const data = file.split("\n");
    const handler = new handlers.statsHandler();

    for (var record of data) {
        handler.next(record)
    }

    var results = Object.assign({store: path.resolve(store)}, handler.return());

    const stats = fs.statSync(store);
    results.size = stats.size;
    results.created = stats.birthtime;
    results.modified = stats.mtime;

    results.end = Date.now();

    return results;
};

const distributeStoreData = async (datapath, dataname, storenames, temppath, datastores, lockoptions) => {
    var results = objects.distributeResults();

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

                line++;
                try {
                    record = JSON.stringify(JSON.parse(record));
                    results.records++;
                } catch {
                    results.errors.push({line: line, data: record});
                } finally {
                    writers[storenumber].write(record + "\n");
                }
            });

            reader.on("close", () => {
                resolve(true);
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
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;

};

const distributeStoreDataSync = (datapath, dataname, storenames, temppath, datastores) => {
    var results = objects.distributeResults();

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
        try {
            data[i] = JSON.stringify(JSON.parse(data[i]));
            results.records++;
        } catch (error) {
            results.errors.push({line: i, data: data[i]});
        } finally {
            if (i === i % datastores) records[i] = [];
            records[i % datastores] += data[i] + "\n";
        }

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
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;

};

const insertStoreData = async (store, data, lockoptions) => {
    var results = Object.assign({store: path.resolve(store)}, objects.insertResults());
    var release;

    if (fs.existsSync(store)) release = await lockfile.lock(store, lockoptions);
    await util.promisify(fs.appendFile)(store, data, "utf8");
    if (release) release();

    results.inserted = data.split("\n").length - 1;
    results.end = Date.now();

    return results;
};

const insertStoreDataSync = (store, data) => {
    var results = Object.assign({store: path.resolve(store)}, objects.insertResults());
    var release;

    if (fs.existsSync(store)) release = lockfile.lockSync(store);
    fs.appendFileSync(store, data, "utf8");
    if (release) release();

    results.inserted = data.split("\n").length - 1;
    results.end = Date.now();

    return results;
};

const insertFileData = async (file, datapath, storenames, lockoptions) => {
    var results = {
        inserted: 0,
        errors: [],
        blanks: 0,
        start: Date.now(),
        end: undefined
    };

    var locks = [];
    var writers = [];

    const datastores = storenames.length;

    for (let storename of storenames) {
        const storepath = path.join(datapath, storename);
        locks.push(lockfile.lock(storepath, lockoptions));
        writers.push(fs.createWriteStream(storepath, {flags: "r+"}));
    }

    const releases = await Promise.all(locks);

    await new Promise((resolve, reject) => {
        var line = 0;
        var stores;

        const reader = rl.createInterface({input: fs.createReadStream(file), crlfDelay: Infinity});

        reader.on("line", record => {

            if (line % datastores === 0) stores = Array.from(Array(datastores).keys());
            const storenumber = stores.splice(Math.floor(Math.random()*stores.length), 1)[0];

            line++;

            if (record.trim().length > 0) {
                try {
                    record = JSON.parse(record);
                    results.inserted++;
                } catch {
                    results.errors.push({line: line, data: record});
                } finally {
                    writers[storenumber].write(JSON.stringify(record) + "\n");
                }
            } else {
                results.blanks++;
            }
        });

        reader.on("close", () => {
            resolve(true);
        });

        reader.on("error", error => {
            reject(error);
        });
    });

    for (const writer of writers) {
        writer.end();
    }

    for (const release of releases) {
        release();
    }

    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;
}

const selectStoreData = async (store, selecter, projecter, lockoptions) => {
    const release = await lockfile.lock(store, lockoptions);

    const generatorResults = await new Promise((resolve, reject) => {
        const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
        const handler = new handlers.selectHandler(selecter, projecter);

        reader.on ("line", record => handler.next(record));
        reader.on ("close", () => resolve(handler.return()));
        reader.on("error", error => reject(error));
    });

    release();

    return Object.assign({store: path.resolve(store)}, generatorResults);
};

const selectStoreDataSync = (store, selecter, projecter) => {
    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");
    release();

    const records = file.split("\n");
    const handler = new handlers.selectHandler(selecter, projecter);

    for (var record of records) {
        handler.next(record);
    }

    return Object.assign({store: path.resolve(store)}, handler.return());
};

const aggregateStoreData = (store, selecter, indexer, projecter, lockoptions) => {
    return new Promise((resolve, reject) => {
        var results = objects.aggregateResults(store);

        var line = 0;
        var aggregates = {};

        lockfile.lock(store, lockoptions).then((release) => {
            const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

            readstream.on("line", (record) => {
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

                            if (aggregates[index]) {
                                aggregates[index].count++;
                            } else {
                                aggregates[index] = {
                                    count: 1,
                                    aggregates: {}
                                };
                            }

                            if (projecter) {
                                projection = projecter(record);
                                fields = Object.keys(projection);
                            } else {
                                projection = record;
                                fields = Object.keys(record);
                            }

                            for (const field of fields) {
                                if (projection[field] !== undefined) {
                                    if (aggregates[index].aggregates[field]) {
                                        utils.accumulateAggregate(aggregates[index].aggregates[field], projection[field]);
                                    } else {
                                        aggregates[index].aggregates[field] = {
                                            min: projection[field],
                                            max: projection[field],
                                            count: 1
                                        };

                                        if (typeof projection[field] === "number") {
                                            aggregates[index].aggregates[field].sum = projection[field];
                                            aggregates[index].aggregates[field].mean = projection[field];
                                            aggregates[index].aggregates[field].m2 = 0;
                                        }
                                    }
                                }
                            }

                            results.indexed++;
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
    var results = objects.aggregateResults(store);

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

                        if (aggregates[index]) {
                            aggregates[index].count++;
                        } else {
                            aggregates[index] = {
                                count: 1,
                                aggregates: {}
                            };
                        }

                        if (projecter) {
                            projection = projecter(record);
                            fields = Object.keys(projection);
                        } else {
                            projection = record;
                            fields = Object.keys(record);
                        }

                        for (const field of fields) {
                            if (projection[field] !== undefined) {
                                if (aggregates[index].aggregates[field]) {
                                    utils.accumulateAggregate(aggregates[index].aggregates[field], projection[field]);
                                } else {
                                    aggregates[index].aggregates[field] = {
                                        min: projection[field],
                                        max: projection[field],
                                        count: 1
                                    };
                                    if (typeof projection[field] === "number") {
                                        aggregates[index].aggregates[field]["sum"] = projection[field];
                                        aggregates[index].aggregates[field]["mean"] = projection[field];
                                        aggregates[index].aggregates[field]["m2"] = 0;
                                    }
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

const updateStoreData = async (store, selecter, updater, tempstore, lockoptions) => {
    const release = await lockfile.lock(store, lockoptions);

    const handlerResults = await new Promise ((resolve, reject) => {
        const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
        const writer = fs.createWriteStream(tempstore);
        const handler = new handlers.updateHandler(selecter, updater);

        reader.on ("line", record => handler.next(record, writer));
        reader.on ("close", () => resolve(handler.return()));
        reader.on("error", error => reject(error));
    });

    var results = Object.assign({store: path.resolve(store)}, handlerResults);

    if (results.updated > 0) {
        await utils.replaceFile(store, tempstore);
    } else {
        await util.promisify(fs.unlink)(tempstore);
    }

    release();

    results.end = Date.now();
    delete results.data;

    return results;
};

const updateStoreDataSync = (store, selecter, updater, tempstore) => {
    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8").trimEnd();
    const records = file.split("\n");

    const handler = new handlers.updateHandler(selecter, updater);

    for (var record of records) {
        handler.next(record);
    }

    var results = Object.assign({store: path.resolve(store)}, handler.return());

    if (results.updated > 0) {
        fs.appendFileSync(tempstore, results.data.join("\n") + "\n", "utf8");
        utils.replaceFileSync(store, tempstore);
    }

    release();

    results.end = Date.now();
    delete results.data;

    return results;
};

const deleteStoreData = async (store, selecter, tempstore, lockoptions) => {
    const release = await lockfile.lock(store, lockoptions);

    const handlerResults = await new Promise((resolve, reject) => {
        const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
        const writer = fs.createWriteStream(tempstore);
        const handler = new handlers.deleteHandler(selecter);

        reader.on ("line", record => handler.next(record, writer));
        reader.on ("close", () => resolve(handler.return()));
        reader.on("error", error => reject(error));
    });

    var results = Object.assign({store: path.resolve(store)}, handlerResults);

    if (results.deleted > 0) {
        await utils.replaceFile(store, tempstore)
    } else {
        await util.promisify(fs.unlink)(tempstore);
    }

    release();

    results.end = Date.now();
    delete results.data;

    return results;

};

const deleteStoreDataSync = (store, selecter, tempstore) => {
    const release = lockfile.lockSync(store);
    const file = fs.readFileSync(store, "utf8");
    const records = file.split("\n");

    const handler = new handlers.deleteHandler(selecter);

    for (var record of records) {
        handler.next(record)
    }

    var results = Object.assign({store: path.resolve(store)}, handler.return());

    if (results.deleted > 0) {
        fs.appendFileSync(tempstore, results.data.join("\n") + "\n");
        utils.replaceFileSync(store, tempstore);
    }

    release();

    results.end = Date.now();
    return results;
};

const dropEverything = async (root, datapath, storenames, temppath, lockoptions) => {
    var locks = [];

    for (let storename of storenames) {
        locks.push(lockfile.lock(path.join(datapath, storename), lockoptions));
    }

    const releases = await Promise.all(locks);

    var deletes = [];

    for (let storename of storenames) {
        deletes.push(utils.deleteFile(path.join(datapath, storename)));
    }

    var results = await Promise.all(deletes);

    for (const release of releases) {
        release();
    }

    deletes = [
        utils.deleteDirectory(temppath),
        utils.deleteDirectory(datapath),
        utils.deleteFile(path.join(root, "njodb.properties"))
    ];

    results = results.concat(await Promise.all(deletes));

    return results;
}

const dropEverythingSync = (root, datapath, storenames, temppath) => {
    var results = [];
    var releases = [];

    for (let storename of storenames) {
        releases.push(lockfile.lockSync(path.join(datapath, storename)));
    }

    for (let storename of storenames) {
        results.push(utils.deleteFileSync(path.join(datapath, storename)));
    }

    for (const release of releases) {
        release();
    }

    results.push(utils.deleteDirectorySync(temppath));
    results.push(utils.deleteDirectorySync(datapath));
    results.push(utils.deleteFileSync(path.join(root, "njodb.properties")));

    return results;
}

exports.getStoreNames = getStoreNames;
exports.getStoreNamesSync = getStoreNamesSync;

exports.statsStoreData = statsStoreData;
exports.statsStoreDataSync = statsStoreDataSync;

exports.distributeStoreData = distributeStoreData;
exports.distributeStoreDataSync = distributeStoreDataSync;

exports.insertStoreData = insertStoreData;
exports.insertStoreDataSync = insertStoreDataSync;

exports.insertFileData = insertFileData;

exports.selectStoreData = selectStoreData;
exports.selectStoreDataSync = selectStoreDataSync;

exports.aggregateStoreData = aggregateStoreData;
exports.aggregateStoreDataSync = aggregateStoreDataSync;

exports.updateStoreData = updateStoreData;
exports.updateStoreDataSync = updateStoreDataSync;

exports.deleteStoreData = deleteStoreData;
exports.deleteStoreDataSync = deleteStoreDataSync;

exports.dropEverything = dropEverything;
exports.dropEverythingSync = dropEverythingSync;
