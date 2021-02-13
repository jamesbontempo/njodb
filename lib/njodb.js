const fs = require("fs");
const rl = require("readline");
const lockfile = require("proper-lockfile");

const insertStoreData = function(store, data) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        const inserted = data.split("\n").length - 1;
        lockfile.lock(store, {retries: 5}).then((release) => {
            fs.appendFile(store, data, "utf8", (error) => {
                if (error) reject(error);
                release().then(resolve({store: store, inserted: inserted, start: start, end: Date.now()}));
            });
        }).catch((error) => {
            reject(error);
        });
    });
};

const selectStoreData = function(store, selector) {
    return new Promise ((resolve, reject) => {
        const start = Date.now();
        var selected = 0;
        var ignored = 0;
        var data = [];
        if (fs.existsSync(store)) {
            lockfile.lock(store, {retries: 5}).then((release) => {
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                reader.on ("line", (record) => {
                    record = JSON.parse(record);
                    if (selector(record)) {
                        data.push(record);
                        selected++;
                    } else {
                        ignored++;
                    }
                });
                reader.on ("close", () => {
                    release().then(resolve({store: store, data: data, selected: selected, ignored: ignored, start: start, end: Date.now()}));
                })
            }).catch((error) => {
                reject(error);
            });
        }
    });
};

const deleteStoreData = function(store, selector, tempstore) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var deleted = 0;
        var retained = 0;
        if (fs.existsSync(store)) {
            lockfile.lock(store, {retries: 5}).then((release) => {
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                const writer = fs.createWriteStream(tempstore);
                reader.on ("line", (record) => {
                    record = JSON.parse(record);
                    if (!selector(record)) {
                        writer.write(JSON.stringify(record) + "\n");
                        retained++;
                    } else {
                        deleted++;
                    }
                });
                reader.on ("close", () => {
                    if (deleted > 0) {
                        fs.rename (store, store + ".old", (error) => {
                            if (error) reject(error);
                            fs.rename (tempstore, store, (error) => {
                                if (error) {
                                    fs.rename (store + ".old", store, (error) => {
                                        if (error) reject(error);
                                        fs.unlink(tempstore, (error) => {
                                            if (error) reject(error);
                                        });
                                    });
                                    reject(error);
                                }
                                fs.unlink(store + ".old", (error) => {
                                    if (error) reject(error);
                                    release().then(resolve ({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()}));
                                });
                            })
                        });
                    } else {
                        fs.unlink(tempstore, (error) => {
                            if (error) reject(error);
                            release().then(resolve ({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()}));
                        });
                    }
                });
            }).catch((error) => {
                reject (error);
            });
        }
    });
};

const updateStoreData = function(store, selector, updator, tempstore) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var updated = 0;
        var unchanged = 0;
        if (fs.existsSync(store)) {
            lockfile.lock(store, {retries: 5}).then((release) => {
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                const writer = fs.createWriteStream(tempstore);
                reader.on ("line", (record) => {
                    record = JSON.parse(record);
                    if (selector(record)) {
                        record = updator(record);
                        updated++;
                    } else {
                        unchanged++;
                    }
                    writer.write(JSON.stringify(record) + "\n");
                });
                reader.on ("close", () => {
                    if (updated > 0) {
                        fs.rename (store, store + ".old", (error) => {
                            if (error) reject(error);
                            fs.rename (tempstore, store, (error) => {
                                if (error) {
                                    fs.rename (store + ".old", store, (error) => {
                                        if (error) reject(error);
                                        fs.unlink(tempstore, (error) => {
                                            if (error) reject(error);
                                        });
                                    });
                                    reject(error);
                                }
                                fs.unlink(store + ".old", (error) => {
                                    if (error) reject(error);
                                    release().then(resolve ({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()}));
                                });
                            })
                        });
                    } else {
                        fs.unlink(tempstore, (error) => {
                            if (error) reject(error);
                            release().then(resolve ({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()}));
                        });
                    }
                });
            }).catch((error) => {
                reject(error);
            });
        }
    });
};

const indexStoreData = function(store, field) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var index = {};

        try {
            const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});

            readstream.on("line", (record) => {
                line++;
                record = JSON.parse(record);
                if (record[field] !== undefined) {
                    if (index[record[field]]) {
                        index[record[field]].push(line);
                    } else {
                        index[record[field]] = [line];
                    }
                }
            });

            readstream.on("close", () => {
                resolve ({store: store, field: field, index: index, start: start, end: Date.now()});
            });
        } catch (error) {
            reject (error);
        }
    });
};

exports.selectStoreData = selectStoreData;
exports.insertStoreData = insertStoreData;
exports.updateStoreData = updateStoreData;
exports.deleteStoreData = deleteStoreData;
exports.indexStoreData = indexStoreData;