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

    getStats() {
        // return stats about datastores (names, filesize, number of records, etc.)
    }

    async insert(data) {
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

    async select(selecter, projecter) {
        if (!(selecter && typeof selecter === "function"))  throw new Error("selecter must be defined and must be a function");
        if (projecter && typeof projecter !== "function")  throw new Error("projecter must be a function");

        return new Promise((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, this.properties.dataname + "." + i + ".json");
                promises.push(njodb.selectStoreData(storepath, selecter, projecter, this.properties.lockoptions, this.properties.debug));
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
                .catch(error => {console.log(error); reject(error)});
        });
    }

    async update(selecter, updater) {
        if (!(selecter && typeof selecter === "function"))  throw new Error("selecter must be defined and must be a function");
        if (!(updater && typeof updater === "function"))  throw new Error("updater must be defined and must be a function");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                const tempstorepath = path.join(this.properties.temppath, [this.properties.dataname, i, Date.now(), "json"].join("."));
                promises.push(njodb.updateStoreData(storepath, selecter, updater, tempstorepath, this.properties.lockoptions, this.properties.debug));
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

    async delete(selecter) {
        if (!(selecter && typeof selecter === "function")) throw new Error("selecter must be defined and must be a function");

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

    async aggregate(selecter, indexer, projecter) {
        if (!(selecter && typeof selecter === "function")) throw new Error("selecter must be defined and must be a function");
        if (!(indexer && typeof indexer === "function")) throw new Error("indexer must be defined and must be a function");
        if (!(projecter && typeof projecter === "function")) throw new Error("projecter must be defined and must be a function");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, [this.properties.dataname, i, "json"].join("."));
                promises.push(njodb.aggregateStoreData(storepath, selecter, indexer, projecter, this.properties.lockoptions, this.properties.debug));
            }

            Promise.all(promises)
                .then(results => {
                    var aggregates = {};
                    var start = Date.now();
                    var end = 0;
                    for (var i = 0; i < results.length; i++) {
                        const indexes = Object.keys(results[i].aggregates);
                        for (var j = 0; j < indexes.length; j++) {
                            const fields = Object.keys(results[i].aggregates[indexes[j]]);
                            for (var k = 0; k < fields.length; k++) {
                                if (!aggregates[indexes[j]]) aggregates[indexes[j]] = {};
                                if (aggregates[indexes[j]][fields[k]]) {
                                    if (results[i].aggregates[indexes[j]][fields[k]]["min"] < aggregates[indexes[j]][fields[k]]["min"]) aggregates[indexes[j]][fields[k]]["min"] = results[i].aggregates[indexes[j]][fields[k]]["min"];
                                    if (results[i].aggregates[indexes[j]][fields[k]]["max"] > aggregates[indexes[j]][fields[k]]["max"]) aggregates[indexes[j]][fields[k]]["max"] = results[i].aggregates[indexes[j]][fields[k]]["max"];
                                    if (results[i].aggregates[indexes[j]][fields[k]]["m2"] !== undefined) {
                                        const n = aggregates[indexes[j]][fields[k]]["count"] + results[i].aggregates[indexes[j]][fields[k]]["count"];
                                        const delta = results[i].aggregates[indexes[j]][fields[k]]["mean"] - aggregates[indexes[j]][fields[k]]["mean"];
                                        const m2 = aggregates[indexes[j]][fields[k]]["m2"] + results[i].aggregates[indexes[j]][fields[k]]["m2"] + (Math.pow(delta, 2) * ((aggregates[indexes[j]][fields[k]]["count"] * results[i].aggregates[indexes[j]][fields[k]]["count"]) / n));
                                        aggregates[indexes[j]][fields[k]]["m2"] = m2;
                                        aggregates[indexes[j]][fields[k]]["varp"] = m2 / n;
                                        aggregates[indexes[j]][fields[k]]["vars"] = m2 / (n - 1);
                                        aggregates[indexes[j]][fields[k]]["stdp"] = Math.sqrt(m2 / n);
                                        aggregates[indexes[j]][fields[k]]["stds"] = Math.sqrt(m2 / (n - 1));
                                    }
                                    if (results[i].aggregates[indexes[j]][fields[k]]["sum"] !== undefined) {
                                        aggregates[indexes[j]][fields[k]]["mean"] = (aggregates[indexes[j]][fields[k]]["sum"] + results[i].aggregates[indexes[j]][fields[k]]["sum"]) / (aggregates[indexes[j]][fields[k]]["count"] + results[i].aggregates[indexes[j]][fields[k]]["count"]);
                                        aggregates[indexes[j]][fields[k]]["sum"] += results[i].aggregates[indexes[j]][fields[k]]["sum"];
                                    }
                                    aggregates[indexes[j]][fields[k]]["count"] += results[i].aggregates[indexes[j]][fields[k]]["count"];
                                } else {
                                    aggregates[indexes[j]][fields[k]] = {
                                        field: results[i].aggregates[indexes[j]][fields[k]]["field"],
                                        min: results[i].aggregates[indexes[j]][fields[k]]["min"],
                                        max: results[i].aggregates[indexes[j]][fields[k]]["max"],
                                        count: results[i].aggregates[indexes[j]][fields[k]]["count"],
                                        sum:  (results[i].aggregates[indexes[j]][fields[k]]["sum"] !== undefined) ? results[i].aggregates[indexes[j]][fields[k]]["sum"] : undefined,
                                        mean:  (results[i].aggregates[indexes[j]][fields[k]]["mean"] !== undefined) ? results[i].aggregates[indexes[j]][fields[k]]["mean"] : undefined,
                                        varp: (results[i].aggregates[indexes[j]][fields[k]]["m2"] !== undefined) ? results[i].aggregates[indexes[j]][fields[k]]["m2"] / results[i].aggregates[indexes[j]][fields[k]]["count"] : undefined,
                                        vars: (results[i].aggregates[indexes[j]][fields[k]]["m2"] !== undefined) ? results[i].aggregates[indexes[j]][fields[k]]["m2"] / (results[i].aggregates[indexes[j]][fields[k]]["count"] - 1) : undefined,
                                        stdp: (results[i].aggregates[indexes[j]][fields[k]]["m2"] !== undefined) ? Math.sqrt(results[i].aggregates[indexes[j]][fields[k]]["m2"] / results[i].aggregates[indexes[j]][fields[k]]["count"]) : undefined,
                                        stds: (results[i].aggregates[indexes[j]][fields[k]]["m2"] !== undefined) ? Math.sqrt(results[i].aggregates[indexes[j]][fields[k]]["m2"] / (results[i].aggregates[indexes[j]][fields[k]]["count"] - 1)) : undefined,
                                        m2: (results[i].aggregates[indexes[j]][fields[k]]["m2"] !== undefined) ? results[i].aggregates[indexes[j]][fields[k]]["m2"] : undefined
                                    };
                                }
                            }
                        }
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }
                    resolve({data: Object.keys(aggregates).map(index => { return {index: index, aggregates: Object.keys(aggregates[index]).map(field => {return {field: field, data: aggregates[index][field]}})}; }), start: start, end: end, details: results})
                })
                .catch(error => reject(error));
        });

    }

    async index(field) {
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
