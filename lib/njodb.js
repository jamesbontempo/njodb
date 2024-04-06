"use strict";

const {
    appendFile,
    appendFileSync,
    createReadStream,
    createWriteStream,
    readFileSync,
    readdir,
    readdirSync,
    stat,
    statSync,
    writeFile
} = require("fs");

const {
    join,
    resolve
} = require("path");

const { createInterface } = require("readline");

const { promisify } = require("util");

const { Randomizer } = require("./randomizer");

const { Result } = require("./result");

const { Handler } = require("./handler");

const {
    deleteFile,
    deleteFileSync,
    deleteDirectory,
    deleteDirectorySync,
    renameFile,
    renameFileSync,
    replaceFile,
    replaceFileSync
} = require("./utils");



const {
    lockFile,
    lockFileSync
} = require("./lock")

const filterStoreNames = (files, dataname) => {
    let storenames = [];
    const re = new RegExp("^" + [dataname, "\\d+", "json"].join(".") + "$");
    for (const file of files) {
        if (re.test(file)) storenames.push(file);
    }
    return storenames;
};

const getStoreNames = async (datapath, dataname) => {
    const files = await promisify(readdir)(datapath);
    return filterStoreNames(files, dataname);
}

const getStoreNamesSync = (datapath, dataname) => {
    const files = readdirSync(datapath);
    return filterStoreNames(files, dataname);
};

// Database management

const statsStoreData = async (store) => {
    const unlockFile = await lockFile(store);

    const handlerResults = await new Promise((resolve, reject) => {
        const reader = createInterface({input: createReadStream(store), crlfDelay: Infinity});
        const handler = Handler("stats");

        reader.on("line", record => handler.next(record));
        reader.on("close", () => resolve(handler.return()));
        reader.on("error", error => reject(error));
    });

    await unlockFile();

    let results = Object.assign({store: resolve(store)}, handlerResults)
    const stats = await promisify(stat)(store);

    results.size = stats.size;
    results.created = stats.birthtime;
    results.modified = stats.mtime;

    results.end = Date.now()

    return results;
};

const statsStoreDataSync = (store) => {
    const unlockFileSync = lockFileSync(store);
    const file = readFileSync(store, "utf8");

    unlockFileSync();

    const data = file.split("\n");
    const handler = Handler("stats");

    for (var record of data) {
        handler.next(record)
    }

    let results = Object.assign({store: resolve(store)}, handler.return());
    const stats = statSync(store);

    results.size = stats.size;
    results.created = stats.birthtime;
    results.modified = stats.mtime;

    results.end = Date.now();

    return results;
};

const distributeStoreData = async (properties) => {
    let results = Result("distribute");
    let storepaths = [];
    let tempstorepaths = [];
    let lockFiles = [];

    for (let storename of properties.storenames) {
        const storepath = join(properties.datapath, storename);
        storepaths.push(storepath);
        lockFiles.push(lockFile(storepath));
    }

    const unlockFiles = await Promise.all(lockFiles);

    let writes = [];
    let writers = [];

    for (let i = 0; i < properties.datastores; i++) {
        const tempstorepath = join(properties.temppath, [properties.dataname, i, results.start, "json"].join("."));
        tempstorepaths.push(tempstorepath);
        await promisify(writeFile)(tempstorepath, "");
        writers.push(createWriteStream(tempstorepath, {flags: "r+"}));
    }

    for (let storename of properties.storenames) {
        writes.push(new Promise((resolve, reject) => {
            let line = 0;
            const store = join(properties.datapath, storename);
            const randomizer = Randomizer(Array.from(Array(properties.datastores).keys()), false);
            const reader = createInterface({input: createReadStream(store), crlfDelay: Infinity});

            reader.on("line", record => {
                const storenumber = randomizer.next();

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

    let deletes = [];

    for (let storepath of storepaths) {
        deletes.push(deleteFile(storepath));
    }

    await Promise.all(deletes);

    for (const unlockFile of unlockFiles) {
        unlockFile();
    }

    let moves = [];

    for (let i = 0; i < tempstorepaths.length; i++) {
        moves.push(renameFile(tempstorepaths[i], join(properties.datapath, [properties.dataname, i, "json"].join("."))))
    }

    await Promise.all(moves);

    results.stores = tempstorepaths.length,
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;

};

const distributeStoreDataSync = (properties) => {
    let results = Result("distribute");
    let storepaths = [];
    let tempstorepaths = [];
    let unlockFilesSync = [];
    let data = [];

    for (let storename of properties.storenames) {
        const storepath = join(properties.datapath, storename);
        storepaths.push(storepath);
        unlockFilesSync.push(lockFileSync(storepath));
        const file = readFileSync(storepath, "utf8").trimEnd();
        if (file.length > 0) data = data.concat(file.split("\n"));
    }

    let records = [];

    for (var i = 0; i < data.length; i++) {
        try {
            data[i] = JSON.stringify(JSON.parse(data[i]));
            results.records++;
        } catch (error) {
            results.errors.push({line: i, data: data[i]});
        } finally {
            if (i === i % properties.datastores) records[i] = [];
            records[i % properties.datastores] += data[i] + "\n";
        }

    }

    const randomizer = Randomizer(Array.from(Array(properties.datastores).keys()), false);

    for (var j = 0; j < records.length; j++) {
        const storenumber = randomizer.next();
        const tempstorepath = join(properties.temppath, [properties.dataname, storenumber, results.start, "json"].join("."));
        tempstorepaths.push(tempstorepath);
        appendFileSync(tempstorepath, records[j]);
    }

    for (let storepath of storepaths) {
        deleteFileSync(storepath);
    }

    for (const unlockFileSync of unlockFilesSync) {
        unlockFileSync();
    }

    for (let i = 0; i < tempstorepaths.length; i++) {
        renameFileSync(tempstorepaths[i], join(properties.datapath, [properties.dataname, i, "json"].join(".")));
    }

    results.stores = tempstorepaths.length,
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;

};

const dropEverything = async (properties) => {
    let results = Result("drop");

    await deleteDirectory(properties.temppath);
    await deleteDirectory(properties.datapath);
    await deleteFile(join(properties.root, "njodb.properties"));

    results.dropped = true;
    results.end = Date.now();
    results.elapsed = results.end - results.start;
    
    return results;
}

const dropEverythingSync = (properties) => {
    let results = Result("drop");

    deleteDirectorySync(properties.temppath);
    deleteDirectorySync(properties.datapath);
    deleteFileSync(join(properties.root, "njodb.properties"));

    results.dropped = true;
    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;
}

// Data manipulation

const insertStoreData = async (store, data) => {
    // How do I try/catch these insert functions?
    let results = Object.assign({store: resolve(store)}, Result("insert"));
    const unlockFile = await lockFile(store);

    await promisify(appendFile)(store, data, "utf8");

    await unlockFile();

    results.inserted = (data.length > 0) ? data.split("\n").length - 1: 0;
    results.end = Date.now();

    return results;
};

const insertStoreDataSync = (store, data) => {
    let results = Object.assign({store: resolve(store)}, Result("insert"));
    const unlockFileSync = lockFileSync(store);

    appendFileSync(store, data, "utf8");

    unlockFileSync();

    results.inserted = (data.length > 0) ? data.split("\n").length - 1 : 0;
    results.end = Date.now();

    return results;
};

const insertFileData = async (file, datapath, storenames) => {
    let results = Result("insertFile");
    let datastores = storenames.length;
    let lockFiles = [];
    let writers = [];

    for (let storename of storenames) {
        const storepath = join(datapath, storename);
        lockFiles.push(lockFile(storepath));
        writers.push(createWriteStream(storepath, {flags: "r+"}));
    }

    const unlockFiles = await Promise.all(lockFiles);

    await new Promise((resolve, reject) => {
        const randomizer = Randomizer(Array.from(Array(datastores).keys()), false);
        const reader = createInterface({input: createReadStream(file), crlfDelay: Infinity});

        reader.on("line", record => {
            record = record.trim();

            const storenumber = randomizer.next();
            results.lines++;

            if (record.length > 0) {
                try {
                    record = JSON.parse(record);
                    results.inserted++;
                } catch(error) {
                    results.errors.push({error: error.message, line: results.lines, data: record});
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

    for (const unlockFile of unlockFiles) {
        unlockFile();
    }

    results.end = Date.now();
    results.elapsed = results.end - results.start;

    return results;
}

const selectStoreData = async (store, match, project) => {
    let results = Object.assign({store: store}, Result("select"));
    const unlockFile = await lockFile(store);

    // How do I better do these try/catch blocks & handle errors & other issues?
    try {
        const handlerResults = await new Promise((resolve, reject) => {
            const reader = createInterface({input: createReadStream(store), crlfDelay: Infinity});
            const handler = Handler("select", match, project);

            reader.on("line", record => handler.next(record));
            reader.on("close", () => resolve(handler.return()));
            reader.on("error", error => reject(error));
        });

        results = Object.assign(results, handlerResults);
    } finally {
        unlockFile();
    }

    return results;
};

const selectStoreDataSync = (store, match, project) => {
    let results = Object.assign({store: store}, Result("select"));
    const unlockFileSync = lockFileSync(store);

    try {
        let file = readFileSync(store, "utf8");

        const records = file.split("\n");
        const handler = Handler("select", match, project);

        for (var record of records) {
            handler.next(record);
        }

        results = Object.assign(results, handler.return());
    } finally {
        unlockFileSync();
    }    

    return results;
};

const updateStoreData = async (store, match, update, tempstore) => {
    let results = Object.assign({store: store, tempstore: tempstore}, Result("update"));
    const unlockFile = await lockFile(store);

    try {
        const handlerResults = await new Promise ((resolve, reject) => {
            const writer = createWriteStream(tempstore);

            writer.on("open", () => {
                const reader = createInterface({input: createReadStream(store), crlfDelay: Infinity});
                const handler = Handler("update", match, update);

                reader.on("line", record => handler.next(record, writer));

                reader.on("close", () => {
                    writer.end();
                    resolve(handler.return());
                });
    
                reader.on("error", error => reject(error));
            });

            writer.on("error", error => reject(error));
        });

        results = Object.assign(results, handlerResults);

        if (results.updated > 0) {
            try {
                await replaceFile(store, tempstore);
            } catch {
                results.errors = [...results.records];
                results.updated = 0;
            }
        } else {
            try {
                await deleteFile(tempstore);
            } catch(e) {
                console.error(e)
            }
        }

        results.end = Date.now();
        delete results.data;
        delete results.records;
    } finally {
        unlockFile();
    }

    return results;
};

const updateStoreDataSync = (store, match, update, tempstore) => {
    let results = Object.assign({store: store, tempstore: tempstore}, Result("update"));
    const unlockFileSync = lockFileSync(store);

    try {
        let file = readFileSync(store, "utf8").trimEnd();

        const records = file.split("\n");
        const handler = Handler("update", match, update);

        for (var record of records) {
            handler.next(record);
        }

        results = Object.assign(results, handler.return());

        if (results.updated > 0) {
            try {
                appendFileSync(tempstore, results.data.join("\n") + "\n", "utf8");
                replaceFileSync(store, tempstore);
            } catch {
                results.errors = [...results.records];
                results.updated = 0;
            }
        } else {
            try {
                deleteFileSync(tempstore);
            } catch(e) {
                console.error(e)
            }
        }

        results.end = Date.now();
        delete results.data;
        delete results.records;
    } finally {
        unlockFileSync();
    }

    return results;
};

const deleteStoreData = async (store, match, tempstore) => {
    let results = Object.assign({store: store, tempstore: tempstore}, Result("delete"));
    const unlockFile = await lockFile(store);

    try {
        const handlerResults = await new Promise((resolve, reject) => {
            const writer = createWriteStream(tempstore);

            writer.on("open", () => {
                const reader = createInterface({input: createReadStream(store), crlfDelay: Infinity});
                const handler = Handler("delete", match);

                reader.on("line", record => handler.next(record, writer));

                reader.on("close", () => {
                    writer.end();
                    resolve(handler.return());
                });
    
                reader.on("error", error => reject(error));    
            });

            writer.on("error", error => reject(error));    
        });

        results = Object.assign(results, handlerResults);

        if (results.deleted > 0) {
            try {
                replaceFile(store, tempstore);
            } catch {
                results.errors = [...results.records];
                results.deleted = 0;
            }
        } else {
            try {
                await deleteFile(tempstore);
            } catch(e) {
                console.error(e)
            }
        }

        results.end = Date.now();
        delete results.data;
        delete results.records;
    } finally {
        unlockFile()
    }

    return results;
};

const deleteStoreDataSync = (store, match, tempstore) => {
    let results = Object.assign({store: store, tempstore: tempstore}, Result("delete"));
    const unlockFileSync = lockFileSync(store);

    try {
        let file = readFileSync(store, "utf8");

        const records = file.split("\n");
        const handler = Handler("delete", match);

        for (var record of records) {
            handler.next(record)
        }

        results = Object.assign(results, handler.return());

        if (results.deleted > 0) {
            try {
                appendFileSync(tempstore, results.data.join("\n") + "\n", "utf8");
                replaceFileSync(store, tempstore);
            } catch {
                results.errors = [...results.records];
                results.updated = 0;
            }
        } else {
            try {
                deleteFileSync(tempstore);
            } catch(e) {
                console.error(e)
            }
        }

        results.end = Date.now();
        delete results.data;
        delete results.records;
    } finally {
        unlockFileSync();
    }
    
    return results;
};

const aggregateStoreData = async (store, match, index, project) => {
    let results = Object.assign({store: store}, Result("aggregate"));
    const unlockFile = await lockFile(store);

    try {
        const handlerResults = await new Promise ((resolve, reject) => {
            const reader = createInterface({input: createReadStream(store), crlfDelay: Infinity});
            const handler = Handler("aggregate", match, index, project);

            reader.on("line", record => handler.next(record));
            reader.on("close", () => resolve(handler.return()));
            reader.on("error", error => reject(error));
        });

        results = Object.assign(results, handlerResults);
    } finally {
        unlockFile();
    }

    return results;
}

const aggregateStoreDataSync = (store, match, index, project) => {
    let results = Object.assign({store: store}, Result("aggregate"));
    const unlockFileSync = lockFileSync(store);

    try {
        let file = readFileSync(store, "utf8");

        const records = file.split("\n");
        const handler = Handler("aggregate", match, index, project);

        for (var record of records) {
            handler.next(record);
        }

        results = Object.assign({store: store}, handler.return());
    } finally {
        unlockFileSync();
    }

    return results;
}

exports.getStoreNames = getStoreNames;
exports.getStoreNamesSync = getStoreNamesSync;

// Database management
exports.statsStoreData = statsStoreData;
exports.statsStoreDataSync = statsStoreDataSync;
exports.distributeStoreData = distributeStoreData;
exports.distributeStoreDataSync = distributeStoreDataSync;
exports.dropEverything = dropEverything;
exports.dropEverythingSync = dropEverythingSync;

// Data manipulation
exports.insertStoreData = insertStoreData;
exports.insertStoreDataSync = insertStoreDataSync;
exports.insertFileData = insertFileData;
exports.selectStoreData = selectStoreData;
exports.selectStoreDataSync = selectStoreDataSync;
exports.updateStoreData = updateStoreData;
exports.updateStoreDataSync = updateStoreDataSync;
exports.deleteStoreData = deleteStoreData;
exports.deleteStoreDataSync = deleteStoreDataSync;
exports.aggregateStoreData = aggregateStoreData;
exports.aggregateStoreDataSync = aggregateStoreDataSync;

