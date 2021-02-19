const fs = require("fs");
const path = require("path");
const njodb = require("./lib/njodb");

const defaults = {
    "datadir": "data",
    "dataname": "data",
    "datastores": 5,
    "tempdir": "tmp",
    "lockoptions": {
        "stale": 5000,
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

        root = (root && typeof root === "string" && root.length > 0) ? root : process.cwd();

        const propertiesFile = path.join(root, "njodb.properties");

        if (fs.existsSync(propertiesFile)) {
            this.setProperties(JSON.parse(fs.readFileSync(propertiesFile)));
        } else {
            if (!fs.existsSync(root)) fs.mkdirSync(root);
            fs.writeFileSync(propertiesFile, JSON.stringify(defaults, null, 4));
            this.properties = defaults;
        }

        this.properties.root = root;

        this.properties.datapath = path.join(root, this.properties.datadir);
        this.properties.temppath = path.join(root, this.properties.tempdir);

        if (!fs.existsSync(this.properties.datapath)) fs.mkdirSync(this.properties.datapath);
        if (!fs.existsSync(this.properties.temppath)) fs.mkdirSync(this.properties.temppath);
    }

    getProperties() {
        return this.properties;
    }

    setProperties(properties) {
        if (properties && typeof properties === "object") this.properties = properties;

        if (!(properties.datadir && typeof properties.datadir === "string")) this.properties.datadir = defaults.datadir;
        if (!(properties.dataname && typeof properties.dataname === "string")) this.properties.dataname = defaults.dataname;
        if (!(properties.datastores && typeof properties.datastores === "number")) this.properties.datastores = defaults.datastores;
        if (!(properties.tempdir && typeof properties.tempdir === "string")) this.properties.tempdir = defaults.tempdir;
        if (!(properties.debug && typeof properties.debug === "boolean")) this.properties.debug = defaults.debug;
        this.properties.lockoptions = (properties.lockoptions && typeof this.properties.lockoptions === "object") ? properties.lockoptions : defaults.lockoptions;

        return this.properties;
    }

    getDebug() {
        return this.properties.debug;
    }

    setDebug(debug) {
        if (debug && typeof debug === "boolean") {
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

    select(selecter) {
        if (!(selecter && typeof selecter === "function")) selecter = function () { return true; };

        return new Promise((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, this.properties.dataname + "." + i + ".json");
                promises.push(njodb.selectStoreData(storepath, selecter, this.properties.lockoptions, this.properties.debug));
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

    update(selecter, updator) {
        if (!(selecter && typeof selecter === "function"))  throw new Error("selecter must be defined");
        if (!(updator && typeof updator === "function"))  throw new Error("updator must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.temppath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(njodb.updateStoreData(storepath, selecter, updator, tempstorepath, this.properties.lockoptions, this.properties.debug));
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

    delete(selecter) {
        if (!(selecter && typeof selecter === "function")) throw new Error("selecter must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.temppath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(njodb.deleteStoreData(storepath, selecter, tempstorepath, this.properties.lockoptions, this.properties.debug));
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

    aggregate(selecter, indexer, field) {
        if (!(selecter && typeof selecter === "function")) throw new Error("selecter must be defined");
        if (!(indexer && typeof indexer === "function")) throw new Error("indexer must be defined");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                promises.push(njodb.aggregateStoreData(storepath, selecter, indexer, field, this.properties.lockoptions, this.properties.debug));
            }

            Promise.all(promises)
                .then(results => {
                    var aggregate = {};
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        const indexes = Object.keys(results[i].aggregate);
                        for (var j = 0; j < indexes.length; j++) {
                            if (aggregate[indexes[j]]) {
                                if (results[i].aggregate[indexes[j]]["min"] < aggregate[indexes[j]]["min"]) aggregate[indexes[j]]["min"] = results[i].aggregate[indexes[j]]["min"];
                                if (results[i].aggregate[indexes[j]]["max"] > aggregate[indexes[j]]["max"]) aggregate[indexes[j]]["max"] = results[i].aggregate[indexes[j]]["max"];
                                if (results[i].aggregate[indexes[j]]["m2"] !== undefined) {
                                    const n = aggregate[indexes[j]]["count"] + results[i].aggregate[indexes[j]]["count"];
                                    const delta = results[i].aggregate[indexes[j]]["mean"] - aggregate[indexes[j]]["mean"];
                                    const m2 = aggregate[indexes[j]]["m2"] + results[i].aggregate[indexes[j]]["m2"] + (Math.pow(delta, 2) * ((aggregate[indexes[j]]["count"] * results[i].aggregate[indexes[j]]["count"]) / n));
                                    aggregate[indexes[j]]["m2"] = m2;
                                    aggregate[indexes[j]]["varp"] = m2 / n;
                                    aggregate[indexes[j]]["vars"] = m2 / (n - 1);
                                    aggregate[indexes[j]]["stdp"] = Math.sqrt(m2 / n);
                                    aggregate[indexes[j]]["stds"] = Math.sqrt(m2 / (n - 1));
                                }
                                if (results[i].aggregate[indexes[j]]["sum"] !== undefined) {
                                    aggregate[indexes[j]]["mean"] = (aggregate[indexes[j]]["sum"] + results[i].aggregate[indexes[j]]["sum"]) / (aggregate[indexes[j]]["count"] + results[i].aggregate[indexes[j]]["count"]);
                                    aggregate[indexes[j]]["sum"] += results[i].aggregate[indexes[j]]["sum"];
                                }
                                aggregate[indexes[j]]["count"] += results[i].aggregate[indexes[j]]["count"];
                            } else {
                                aggregate[indexes[j]] = {
                                    field: results[i].aggregate[indexes[j]]["field"],
                                    min: results[i].aggregate[indexes[j]]["min"],
                                    max: results[i].aggregate[indexes[j]]["max"],
                                    count: results[i].aggregate[indexes[j]]["count"],
                                    sum:  (results[i].aggregate[indexes[j]]["sum"] !== undefined) ? results[i].aggregate[indexes[j]]["sum"] : undefined,
                                    mean:  (results[i].aggregate[indexes[j]]["mean"] !== undefined) ? results[i].aggregate[indexes[j]]["mean"] : undefined,
                                    varp: (results[i].aggregate[indexes[j]]["m2"] !== undefined) ? results[i].aggregate[indexes[j]]["m2"] / results[i].aggregate[indexes[j]]["count"] : undefined,
                                    vars: (results[i].aggregate[indexes[j]]["m2"] !== undefined) ? results[i].aggregate[indexes[j]]["m2"] / (results[i].aggregate[indexes[j]]["count"] - 1) : undefined,
                                    stdp: (results[i].aggregate[indexes[j]]["m2"] !== undefined) ? Math.sqrt(results[i].aggregate[indexes[j]]["m2"] / results[i].aggregate[indexes[j]]["count"]) : undefined,
                                    stds: (results[i].aggregate[indexes[j]]["m2"] !== undefined) ? Math.sqrt(results[i].aggregate[indexes[j]]["m2"] / (results[i].aggregate[indexes[j]]["count"] - 1)) : undefined,
                                    m2: (results[i].aggregate[indexes[j]]["m2"] !== undefined) ? results[i].aggregate[indexes[j]]["m2"] : undefined
                                };
                            }
                        }
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }
                    resolve({data: Object.keys(aggregate).map(index => {return {index: index, field: aggregate[index].field, min: aggregate[index].min, max: aggregate[index].max, count: aggregate[index].count, sum: aggregate[index].sum, mean: aggregate[index].mean, varp: (aggregate[index].count > 1) ? aggregate[index].varp : undefined, vars: (aggregate[index].count > 1) ? aggregate[index].vars : undefined, stdp: (aggregate[index].count > 1) ? aggregate[index].stdp : undefined, stds: (aggregate[index].count > 1) ? aggregate[index].stds : undefined}}), start: start, end: end, details: results})
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
