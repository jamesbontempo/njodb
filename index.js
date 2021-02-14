const fs = require("fs");
const path = require("path");
const njodb = require("./lib/njodb");

const defaults = {
    "root": "./",
    "datapath": "./data",
    "dataname": "data",
    "datastores": 5,
    "lockoptions": {
        "stale": 2000,
        "update": 1000,
        "retries": {
            "retries": 5000,
            "minTimeout": 250,
            "maxTimeout": 5000,
            "factor": 0.15,
            "randomize": false
        }
    },
    "debug": false
};



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

        if (typeof this.properties.debug !== "boolean") this.properties.debug = defaults.debug;

        this.properties.lockoptions = (this.properties.lockoptions && typeof this.properties.lockoptions === "object") ? this.properties.lockoptions : defaults.lockoptions;
    }

    getProperties() {
        return this.properties;
    }

    getDebug() {
        return this.properties.debug;
    }

    setDebug(debug) {
        if (typeof debug === "boolean") {
            this.properties.debug = debug;
        }
        return this.getDebug();
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
                promises.push(njodb.insertStoreData(storepath, records[j], this.properties.lockoptions, this.properties.debug));
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
                promises.push(njodb.selectStoreData(storepath, selector, this.properties.lockoptions, this.properties.debug));
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

    update(selector, updator) {
        if (!(selector && typeof selector === "function"))  throw new Error("selector must be defined");
        if (!(updator && typeof updator === "function"))  throw new Error("updator must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.datapath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(njodb.updateStoreData(storepath, selector, updator, tempstorepath, this.properties.lockoptions, this.properties.debug));
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

    delete(selector) {
        if (!(selector && typeof selector === "function")) throw new Error("selector must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.datapath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(njodb.deleteStoreData(storepath, selector, tempstorepath, this.properties.lockoptions, this.properties.debug));
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

    index(field) {
        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                promises.push(njodb.indexStoreData(storepath, field, this.properties.lockoptions, this.properties.debug));
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
