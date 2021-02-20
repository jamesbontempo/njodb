const fs = require("fs");
const rl = require("readline");
const lockfile = require("proper-lockfile");

const insertStoreData = async (store, data, lockoptions, debug) => {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        const inserted = data.split("\n").length - 1;
        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                if (debug) console.log([store, "insertStoreData", "lock", Date.now() - start].join("\t"));
                fs.appendFile(store, data, "utf8", (error) => {
                    if (error) reject(error);
                    release()
                        .then(() => {
                            if (debug) console.log([store, "insertStoreData", "done", Date.now() - start].join("\t"));
                            resolve({store: store, inserted: inserted, start: start, end: Date.now()});
                        });
                });
            }).catch((error) => {
                reject(error);
            });
        } else {
            fs.appendFile(store, data, "utf8", (error) => {
                if (error) reject(error);
                if (debug) console.log([store, "insertStoreData", "done", Date.now() - start].join("\t"));
                resolve({store: store, inserted: inserted, start: start, end: Date.now()});
            });
        }
    });
};

const selectStoreData = async (store, selecter, projecter, lockoptions, debug) => {
    return new Promise ((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var selected = 0;
        var ignored = 0;
        var data = [];
        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                if (debug) console.log([store, "selectStoreData", "lock", Date.now() - start].join("\t"));
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
                            selected++;
                        } else {
                            ignored++;
                        }
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("SELECT: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }
                });
                reader.on ("close", () => {
                    release()
                        .then(() => {
                            if (debug) console.log([store, JSON.stringify(data), "selectStoreData", "done", Date.now() - start].join("\t"));
                            resolve({store: store, data: data, selected: selected, ignored: ignored, start: start, end: Date.now()});
                        })
                        .catch(error => reject(error));
                })
            }).catch((error) => {
                reject(error);
            });
        } else {
            resolve({store: store, data: data, selected: selected, ignored: ignored, start: start, end: Date.now()});
        }
    });
};

const deleteStoreData = async (store, selecter, tempstore, lockoptions, debug) => {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var deleted = 0;
        var retained = 0;
        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                if (debug) console.log([store, "deleteStoreData", "lock", Date.now() - start].join("\t"));
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                const writer = fs.createWriteStream(tempstore);
                reader.on ("line", (record) => {
                    line++;
                    try {
                        record = JSON.parse(record);
                        if (!selecter(record)) {
                            writer.write(JSON.stringify(record) + "\n");
                            retained++;
                        } else {
                            deleted++;
                        }
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("DELETE: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
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
                                    release()
                                        .then(() => {
                                            if (debug) console.log([store, "deleteStoreData", "done", Date.now() - start].join("\t"));
                                            resolve({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()})
                                        });
                                });
                            })
                        });
                    } else {
                        fs.unlink(tempstore, (error) => {
                            if (error) reject(error);
                            release()
                                .then(() => {
                                    if (debug) console.log([store, "deleteStoreData", "done", Date.now() - start].join("\t"));
                                    resolve({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()});
                                });
                        });
                    }
                });
            }).catch((error) => {
                reject(error);
            });
        } else {
            resolve({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()});
        }
    });
};

const updateStoreData = async (store, selecter, updater, tempstore, lockoptions, debug) => {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var updated = 0;
        var unchanged = 0;
        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                if (debug) console.log([store, "updateStoreData", "lock", Date.now() - start].join("\t"));
                const reader = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                const writer = fs.createWriteStream(tempstore);
                reader.on ("line", (record) => {
                    line++;
                    try {
                        record = JSON.parse(record);
                        if (selecter(record)) {
                            record = updater(record);
                            updated++;
                        } else {
                            unchanged++;
                        }
                        writer.write(JSON.stringify(record) + "\n");
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("UPDATE: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }
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
                                    release()
                                        .then(() => {
                                            if (debug) console.log([store, "updateStoreData", "done", Date.now() - start].join("\t"));
                                            resolve({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()});
                                        });
                                });
                            })
                        });
                    } else {
                        fs.unlink(tempstore, (error) => {
                            if (error) reject(error);
                            release()
                                .then(() => {
                                    if (debug) console.log([store, "updateStoreData", "done", Date.now() - start].join("\t"));
                                    resolve({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()});
                                });
                        });
                    }
                });
            }).catch((error) => {
                reject(error);
            });
        } else {
            resolve({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()});
        }
    });
};

const aggregateStoreData = async (store, selecter, indexer, projecter, lockoptions, debug) => {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var aggregates = {};
        if (fs.existsSync(store)) {
            lockfile.lock(store, lockoptions).then((release) => {
                if (debug) console.log([store, "aggregateStoreData", "lock", Date.now() - start].join("\t"));
                const readstream = rl.createInterface({input: fs.createReadStream(store), crlfDelay: Infinity});
                readstream.on("line", (record) => {
                    line++;
                    try {
                        record = JSON.parse(record);
                        if (selecter(record)) {
                            const index = indexer(record);
                            const projection = projecter(record);
                            const fields = Object.keys(projection);
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
                    } catch (error) {
                        if (error instanceof SyntaxError) {
                            const position = parseInt(error.message.match(/(\d+)$/)[0]) + 1 || 0;
                            console.error("AGGREGATE: Problematic record in " + store + " on line " + line + " at position " + position + ": " + record);
                        } else {
                            reject(error);
                        }
                    }
                });
                readstream.on("close", () => {
                    release()
                        .then(() => {
                            if (debug) console.log([store, "aggregateStoreData", "done", Date.now() - start].join("\t"));
                            resolve({store: store, aggregates: aggregates, start: start, end: Date.now()});
                        });
                });
            }).catch((error) => {
                reject (error);
            });
        } else {
            resolve({store: store, aggregates: aggregates, start: start, end: Date.now()});
        }
    });
}

const indexStoreData = async (store, field, lockoptions, debug) => {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var line = 0;
        var index = {};
        lockfile.lock(store, lockoptions).then((release) => {
            if (debug) console.log([store, "indexStoreData", "lock", Date.now() - start].join("\t"));
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
                        if (debug) console.log([store, "indexStoreData", "done", Date.now() - start].join("\t"));
                        resolve({store: store, field: field, index: index, start: start, end: Date.now()})
                    });
            });
        }).catch((error) => {
            reject (error);
        });
    });
};

exports.selectStoreData = selectStoreData;
exports.insertStoreData = insertStoreData;
exports.updateStoreData = updateStoreData;
exports.deleteStoreData = deleteStoreData;
exports.aggregateStoreData = aggregateStoreData;
exports.indexStoreData = indexStoreData;