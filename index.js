const fs = require("fs");
const path = require("path");
const njodb = require("./lib/njodb");
const utils = require("./lib/utils");

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
        return new Promise((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storename = [this.properties.dataname, i, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename);

                promises.push(
                    njodb.statsStoreData(
                        storepath,
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
            }

            Promise.all(promises)
                .then(results => {

                    var stats = {
                        size: 0,
                        records: 0,
                        min: undefined,
                        max: undefined,
                        mean: undefined,
                        m2: 0
                    };

                    var start = Date.now();
                    var end = 0;

                    for (var i = 0; i < results.length; i++) {

                        if (i === 0) {
                            stats.min = results[i].records;
                            stats.max = results[i].records;
                            stats.mean = results[i].records;
                        } else {
                            if (results[i].records < stats.min) stats.min = results[i].records;
                            if (results[i].records > stats.max) stats.max = results[i].records;
                            const delta1 = results[i].records - stats.mean;
                            stats.mean += delta1 / (i + 2);
                            const delta2 = results[i].records - stats.mean;
                            stats.m2 += delta1 * delta2;
                        }

                        if (results[i].size) stats.size += results[i].size;
                        if (results[i].records) stats.records += results[i].records;
                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;

                    }

                    resolve(
                        {
                            size: utils.convertSize(stats.size),
                            records: stats.records,
                            min: stats.min,
                            max: stats.max,
                            mean: stats.mean,
                            var: stats.m2/(results.length),
                            std: Math.sqrt(stats.m2/(results.length)),
                            start: start,
                            end: end,
                            details: results
                        }
                    );
                })
                .catch(error => {
                    reject(error);
                });
        });
    }

    async insert(data) {
        utils.checkValue("data", data, true, "array");

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
                const storename = [this.properties.dataname, storenumber, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename)

                promises.push(
                    njodb.insertStoreData(
                        storepath,
                        records[j],
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
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

                    resolve(
                        {
                            inserted: inserted,
                            start: start,
                            end: end,
                            details: results
                        }
                    );
                })
                .catch(error => {
                    reject(error);
                });
        });

    }

    async select(selecter, projecter) {
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("projecter", projecter, false, "function");

        return new Promise((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storepath = path.join(this.properties.datapath, this.properties.dataname + "." + i + ".json");

                promises.push(
                    njodb.selectStoreData(
                        storepath,
                        selecter,
                        projecter,
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
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

                    resolve(
                        {
                            data: data,
                            selected: selected,
                            ignored: ignored,
                            start: start,
                            end: end,
                            details: results
                        }
                    );
                })
                .catch(error => {
                    reject(error);
                });
        });
    }

    async update(selecter, updater) {
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("updater", updater, true, "function");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storename = [this.properties.dataname, i, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename);
                const tempstorename = [this.properties.dataname, i, Date.now(), "json"].join(".");
                const tempstorepath = path.join(this.properties.temppath, tempstorename);

                promises.push(
                    njodb.updateStoreData(
                        storepath,
                        selecter,
                        updater,
                        tempstorepath,
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
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

                    resolve(
                        {
                            updated: updated,
                            unchanged: unchanged,
                            start: start,
                            end: end,
                            details: results
                        }
                    );
                })
                .catch(error => {
                    reject(error);
                });
        });
    }

    async delete(selecter) {
        utils.checkValue("selecter", selecter, true, "function");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storename = [this.properties.dataname, i, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename);
                const tempstorename = [this.properties.dataname, i, Date.now(), "json"].join(".");
                const tempstorepath = path.join(this.properties.temppath, tempstorename);

                promises.push(
                    njodb.deleteStoreData(
                        storepath,
                        selecter,
                        tempstorepath,
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
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

                    resolve(
                        {
                            deleted: deleted,
                            retained: retained,
                            start: start,
                            end: end,
                            details: results
                        }
                    );
                })
                .catch(error => {
                    reject(error);
                });
        });
    }

    async aggregate(selecter, indexer, projecter) {
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("indexer", indexer, true, "function");
        utils.checkValue("projecter", projecter, false, "function");

        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storename = [this.properties.dataname, i, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename);

                promises.push(
                    njodb.aggregateStoreData(
                        storepath,
                        selecter,
                        indexer,
                        projecter,
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
            }

            Promise.all(promises)
                .then(results => {

                    var aggregates = {};
                    var start = Date.now();
                    var end = 0;

                    for (var i = 0; i < results.length; i++) {

                        const indexes = Object.keys(results[i].aggregates);

                        for (var j = 0; j < indexes.length; j++) {

                            if (!aggregates[indexes[j]]) aggregates[indexes[j]] = {};

                            const fields = Object.keys(results[i].aggregates[indexes[j]]);

                            for (var k = 0; k < fields.length; k++) {

                                const aggregateObject = aggregates[indexes[j]][fields[k]];
                                const resultObject = results[i].aggregates[indexes[j]][fields[k]];

                                if (aggregateObject) {
                                    if (resultObject["min"] < aggregateObject["min"]) aggregateObject["min"] = resultObject["min"];
                                    if (resultObject["max"] > aggregateObject["max"]) aggregateObject["max"] = resultObject["max"];

                                    if (resultObject["m2"] !== undefined) {
                                        const n = aggregateObject["count"] + resultObject["count"];
                                        const delta = resultObject["mean"] - aggregateObject["mean"];
                                        const m2 = aggregateObject["m2"] + resultObject["m2"] + (Math.pow(delta, 2) * ((aggregateObject["count"] * resultObject["count"]) / n));
                                        aggregateObject["m2"] = m2;
                                        aggregateObject["varp"] = m2 / n;
                                        aggregateObject["vars"] = m2 / (n - 1);
                                        aggregateObject["stdp"] = Math.sqrt(m2 / n);
                                        aggregateObject["stds"] = Math.sqrt(m2 / (n - 1));
                                    }

                                    if (resultObject["sum"] !== undefined) {
                                        aggregateObject["mean"] = (aggregateObject["sum"] + resultObject["sum"]) / ( aggregateObject["count"] + resultObject["count"]);
                                        aggregateObject["sum"] += resultObject["sum"];
                                    }

                                    aggregateObject["count"] += resultObject["count"];

                                } else {

                                    aggregates[indexes[j]][fields[k]] = {
                                        min: resultObject["min"],
                                        max: resultObject["max"],
                                        count: resultObject["count"],
                                        sum: resultObject["sum"],
                                        mean: resultObject["mean"],
                                        varp: undefined,
                                        vars: undefined,
                                        stdp: undefined,
                                        stds: undefined,
                                        m2: undefined
                                    };

                                    if (resultObject["m2"] !== undefined) {
                                        aggregates[indexes[j]][fields[k]]["varp"] = resultObject["m2"] / resultObject["count"];
                                        aggregates[indexes[j]][fields[k]]["vars"] = resultObject["m2"] / (resultObject["count"] - 1);
                                        aggregates[indexes[j]][fields[k]]["stdp"] = Math.sqrt(resultObject["m2"] / resultObject["count"]);
                                        aggregates[indexes[j]][fields[k]]["stds"] = Math.sqrt(resultObject["m2"] / (resultObject["count"] - 1));
                                        aggregates[indexes[j]][fields[k]]["m2"] = resultObject["m2"];
                                    }

                                }
                            }
                        }

                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;

                    }

                    resolve(
                        {
                            data: Object.keys(aggregates).map(index => {
                                return {
                                    index: index,
                                    aggregates: Object.keys(aggregates[index]).map(field => {
                                        delete aggregates[index][field].m2;
                                        return {
                                            field: field,
                                            data: aggregates[index][field]
                                        };
                                    })
                                };
                            }),
                            start: start,
                            end: end,
                            details: results
                        }
                    );
                })
                .catch(error => {
                    reject(error);
                });
        });

    }

    async drop() {
        return new Promise((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storename = [this.properties.dataname, i, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename);

                promises.push(
                    Promise.resolve(
                        njodb.deleteFile(
                            storepath,
                            this.properties.lockoptions,
                            this.properties.debug
                        )
                    )
                );
            }

            Promise.all(promises)
            .then(() => {
                promises.push([
                    njodb.deleteDirectory(this.properties.temppath),
                    njodb.deleteDirectory(this.properties.datapath),
                    njodb.deleteFile(path.join(this.properties.root, "njodb.properties"))
                ]);

                Promise.all(promises)
                    .then(results => {
                        var start = Date.now();
                        var end = 0;

                        for (var i = 0; i < results.length; i++) {
                            if (results[i].start < start) start = results[i].start;
                            if (results[i].end > end) end = results[i].end;
                        }

                        resolve(
                            {
                                dropped: true,
                                start: start,
                                end: end
                            }
                        );
                    })
                    .catch(error => {
                        reject(error);
                    });
                })
            .catch(error => {
                reject(error);
            });
        });
    }

    async index(field) {
        return new Promise ((resolve, reject) => {
            var promises = [];

            for (var i = 0; i < this.properties.datastores; i++) {
                const storename = [this.properties.dataname, i, "json"].join(".");
                const storepath = path.join(this.properties.datapath, storename);

                promises.push(
                    njodb.indexStoreData(
                        storepath,
                        field,
                        this.properties.lockoptions,
                        this.properties.debug
                    )
                );
            }

            Promise.all(promises)
                .then(results => {

                    var index = {};
                    var start = Date.now();
                    var end = 0;

                    for (var i = 0; i < results.length; i++) {
                        const keys = Object.keys(results[i].index).sort((a, b) => {if (a < b) return -1; if (a > b) return 1; return 0; });

                        for (var j = 0; j < keys.length; j++) {
                            if (index[keys[j]]) {
                                index[keys[j]].push({store: results[i].store, lines: results[i].index[keys[j]]});
                            } else {
                                index[keys[j]] = [{store: results[i].store, lines: results[i].index[keys[j]]}];
                            }
                        }

                        if (results[i].start < start) start = results[i].start;
                        if (results[i].end > end) end = results[i].end;
                    }

                    const indexpath = path.join(this.properties.datapath, ["index", field, "json"].join("."));

                    fs.writeFile(indexpath, JSON.stringify(index), (error) => {
                        if (error) throw error;
                        resolve(
                            {
                                field: field,
                                path: indexpath,
                                size: Object.keys(index).length,
                                start: start,
                                end: end,
                                details: results
                            }
                        );
                    });
                })
                .catch(error => {
                    reject(error);
                });
        });
    }
}

exports.Database = Database;
