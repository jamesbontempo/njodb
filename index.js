const fs = require("fs");
const path = require("path");
const rl = require("readline");

const defaults = {
    "root": "./",
    "datapath": "./data",
    "dataname": "data",
    "datastores": 5,
    "readlock": false,
    "writelock": true
};

const insertStoreData = function(store, data) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        const inserted = data.split("\n").length - 1;
        fs.appendFile(store, data, "utf8", (error) => {
            if (error) reject(error);
            resolve({store: store, inserted: inserted, start: start, end: Date.now()});
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
            try {
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
                    resolve({store: store, data: data, selected: selected, ignored: ignored, start: start, end: Date.now()});
                })
            } catch (error) {
                reject(error);
            }
        }
    });
};

const deleteStoreData = function(store, selector, tempstore) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var deleted = 0;
        var retained = 0;
        if (fs.existsSync(store)) {
            try {
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
                                    resolve ({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()});
                                });
                            })
                        });
                    } else {
                        fs.unlink(tempstore, (error) => {
                            if (error) reject(error);
                            resolve ({store: store, deleted: deleted, retained: retained, start: start, end: Date.now()});
                        });
                    }
                });
            } catch (error) {
                reject (error);
            }
        }
    });
};

const updateStoreData = function(store, selector, updator, tempstore) {
    return new Promise((resolve, reject) => {
        const start = Date.now();
        var updated = 0;
        var unchanged = 0;
        if (fs.existsSync(store)) {
            try {
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
                                    resolve ({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()});
                                });
                            })
                        });
                    } else {
                        fs.unlink(tempstore, (error) => {
                            if (error) reject(error);
                            resolve ({store: store, updated: updated, unchanged: unchanged, start: start, end: Date.now()});
                        });
                    }
                });
            } catch (error) {
                reject (error);
            }
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
}

class Database {

    constructor(root) {
        this.properties = {};

        root = (root && typeof root === "string" && root.length > 0) ? root : defaults.root;

        this.properties = JSON.parse(fs.readFileSync(path.join(root, "nodedb.properties")));
        this.properties.root = root;

        if (!this.properties.datadir) this.properties.datadir = defaults.datadir;
        this.properties.datapath = path.join(this.properties.root, this.properties.datadir);

        if (!this.properties.dataname) this.properties.dataname = defaults.dataname;
        if (!this.properties.datastores) this.properties.datastores = defaults.datastores;
        if (!this.properties.readlock) this.properties.readlock = defaults.readlock;
        if (!this.properties.writelock) this.properties.writelock = defaults.writelock;
    }

    getProperties() {
        return this.properties;
    }

    insert(data) {
        if (!Array.isArray(data) || (Array.isArray(data) && data.length === 0)) throw new Error ("Invalid data");

        return new Promise((resolve, reject) => {
            var promises = [];
            var records = [];

            for (var i = 0; i < data.length; i++) {
                if (i === i % this.properties.datastores) records[i] = [];
                records[i % this.properties.datastores] += JSON.stringify(data[i]) + "\n";
            }

            var stores = Array.from(Array(this.properties.datastores).keys());

            for (var j = 0; j < records.length; j++) {
                const storenumber = stores.splice(Math.floor(Math.random()*stores.length), 1)[0];
                const storepath = path.join(this.properties.datapath, this.properties.dataname + "." + storenumber + ".json")
                promises.push(insertStoreData(storepath, records[j]));
            }

            Promise.all(promises)
                .then(results => {
                    var inserted = 0;
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        inserted += results[i].inserted;
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }
                    resolve({inserted: inserted, start: start, end: end, details: results});
                })
                .catch(error => reject(error));
        });

    }

    select(selector) {
        if (!(selector && typeof selector === "function")) selector = function () { return true; };

        return new Promise((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, this.properties.dataname + "." + i + ".json");
                promises.push(selectStoreData(storepath, selector));
            }

            Promise.all(promises)
                .then(results => {
                    var data = [];
                    var selected = 0;
                    var ignored = 0;
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        data = data.concat(results[i].data);
                        selected += results[i].selected;
                        ignored += results[i].ignored;
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }
                    resolve({data: data, selected: selected, ignored: ignored, start: start, end: end, details: results})
                })
                .catch(error => reject(error));
        });
    }

    delete(selector) {
        if (!(selector && typeof selector === "function")) throw new Error("selector must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.datapath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(deleteStoreData(storepath, selector, tempstorepath));
            }

            Promise.all(promises)
                .then(results => {
                    var deleted = 0;
                    var retained = 0;
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        deleted += results[i].deleted;
                        retained += results[i].retained;
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }
                    resolve({deleted: deleted, retained: retained, start: start, end: end, details: results})
                })
                .catch(error => reject(error));
        });
    }

    update(selector, updator) {
        if (!(selector && typeof selector === "function"))  throw new Error("selector must be defined");
        if (!(updator && typeof updator === "function"))  throw new Error("updator must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.datapath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(updateStoreData(storepath, selector, updator, tempstorepath));
            }

            Promise.all(promises)
                .then(results => {
                    var updated = 0;
                    var unchanged = 0;
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        updated += results[i].updated;
                        unchanged += results[i].unchanged;
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }
                    resolve({updated: updated, unchanged: unchanged, start: start, end: end, details: results})
                })
                .catch(error => reject(error));
        });
    }

    index(field) {
        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                promises.push(indexStoreData(storepath, field));
            }

            Promise.all(promises)
                .then(results => {
                    var index = {};
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                        const keys = Object.keys(results[i].index).sort((a, b) => {if (a < b) return -1; if (a > b) return 1; return 0; });
                        for (var j = 0; j < keys.length; j++) {
                            if (index[keys[j]]) {
                                index[keys[j]].push({store: results[i].store, lines: results[i].index[keys[j]]});
                            } else {
                                index[keys[j]] = [{store: results[i].store, lines: results[i].index[keys[j]]}];
                            }
                        }
                    }
                    const indexpath = path.join(this.properties.datapath, ["index", field, "json"].join("."));
                    fs.writeFile(indexpath, JSON.stringify(index), (error) => {
                        if (error) throw error;
                        resolve({field: field, path: indexpath, size: Object.keys(index).length, start: start, end: end, details: results});
                    });
                })
                .catch(error => reject(error));
        });
    }
}

exports.Database = Database;
