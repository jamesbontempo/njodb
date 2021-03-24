"use strict";

const fs = require("fs");
const path = require("path");

const njodb = require("./lib/njodb");
const reduce = require("./lib/reduce");
const utils = require("./lib/utils");
const validators = require("./lib/validators");

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
    }
};

const saveProperties = (root, properties) => {
    properties = {
        "datadir": properties.datadir,
        "dataname": properties.dataname,
        "datastores": properties.datastores,
        "tempdir": properties.tempdir,
        "lockoptions": properties.lockoptions
    };
    const propertiesFile = path.join(root, "njodb.properties");
    fs.writeFileSync(propertiesFile, JSON.stringify(properties, null, 4));
    return properties;
}

class Database {

    constructor(root) {
        if (root) validators.validatePath(root);

        this.properties = {};
        this.properties.root = (root) ? root : process.cwd();

        if (!fs.existsSync(this.properties.root)) fs.mkdirSync(this.properties.root);

        const propertiesFile = path.join(this.properties.root, "njodb.properties");

        if (fs.existsSync(propertiesFile)) {
            try {
                this.setProperties(JSON.parse(fs.readFileSync(propertiesFile)));
            } catch {
                this.setProperties(defaults);
            }
        } else {
            this.setProperties(defaults);
        }

        if (!fs.existsSync(this.properties.datapath)) fs.mkdirSync(this.properties.datapath);
        if (!fs.existsSync(this.properties.temppath)) fs.mkdirSync(this.properties.temppath);

        this.properties.storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        return this;
    }

    // Database management methods

    getProperties() {
        return this.properties;
    }

    setProperties(properties) {
        validators.validateObject(properties);

        this.properties.datadir = (validators.validateName(properties.datadir)) ? properties.datadir : defaults.datadir;
        this.properties.dataname = (validators.validateName(properties.dataname)) ? properties.dataname : defaults.dataname;
        this.properties.datastores = (validators.validateSize(properties.datastores)) ? properties.datastores : defaults.datastores;
        this.properties.tempdir = (validators.validateName(properties.tempdir)) ? properties.tempdir : defaults.tempdir;
        this.properties.lockoptions = (validators.validateObject(properties.lockoptions)) ? properties.lockoptions : defaults.lockoptions;
        this.properties.datapath = path.join(this.properties.root, this.properties.datadir);
        this.properties.temppath = path.join(this.properties.root, this.properties.tempdir);

        saveProperties(this.properties.root, this.properties);

        return this.properties;
    }

    async stats() {
        var stats = {
            root: path.resolve(this.properties.root),
            data: path.resolve(this.properties.datapath),
            temp: path.resolve(this.properties.temppath)
        };

        var promises = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            promises.push(njodb.statsStoreData(storepath, this.properties.lockoptions));
        }

        const results = await Promise.all(promises);

        return Object.assign(stats, reduce.statsReduce(results));
    }

    statsSync() {
        var stats = {
            root: path.resolve(this.properties.root),
            data: path.resolve(this.properties.datapath),
            temp: path.resolve(this.properties.temppath)
        };

        var results = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            results.push(njodb.statsStoreDataSync(storepath));
        }

        return Object.assign(stats, reduce.statsReduce(results));
    }

    async grow() {
        this.properties.datastores++;
        const results = await njodb.distributeStoreData(this.properties);
        this.properties.storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);
        saveProperties(this.properties.root, this.properties);
        return results;
    }

    growSync() {
        this.properties.datastores++;
        const results = njodb.distributeStoreDataSync(this.properties);
        this.properties.storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);
        saveProperties(this.properties.root, this.properties);
        return results;
    }

    async shrink() {
        if (this.properties.datastores > 1) {
            this.properties.datastores--;
            const results = await njodb.distributeStoreData(this.properties);
            this.properties.storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);
            saveProperties(this.properties.root, this.properties);
            return results;
        } else {
            throw new Error("Database cannot shrink any further");
        }
    }

    shrinkSync() {
        if (this.properties.datastores > 1) {
            this.properties.datastores--;
            const results = njodb.distributeStoreDataSync(this.properties);
            this.properties.storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);
            saveProperties(this.properties.root, this.properties);
            return results;
        } else {
            throw new Error("Database cannot shrink any further");
        }
    }

    async resize(size) {
        validators.validateSize(size);
        this.properties.datastores = size;
        const results = await njodb.distributeStoreData(this.properties);
        this.properties.storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);
        saveProperties(this.properties.root, this.properties);
        return results;
    }

    resizeSync(size) {
        validators.validateSize(size);
        this.properties.datastores = size;
        const results = njodb.distributeStoreDataSync(this.properties);
        this.properties.storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);
        saveProperties(this.properties.root, this.properties);
        return results;
    }

    async drop() {
        const results = await njodb.dropEverything(this.properties);
        return reduce.dropReduce(results);
    }

    dropSync() {
        const results = njodb.dropEverythingSync(this.properties);
        return reduce.dropReduce(results);
    }

    // Data manipulation methods

    async insert(data) {
        validators.validateArray(data);

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
                    this.properties.lockoptions
                )
            );
        }

        const results = await Promise.all(promises);

        this.properties.storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        return reduce.insertReduce(results);
    }

    insertSync(data) {
        validators.validateArray(data);

        var results = [];
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

            results.push(
                njodb.insertStoreDataSync(
                    storepath,
                    records[j],
                    this.properties.lockoptions
                )
            );
        }

        this.properties.storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        return reduce.insertReduce(results);
    }

    async insertFile(file) {
        validators.validatePath(file);

        const results = await njodb.insertFileData(
            file,
            this.properties.datapath,
            this.properties.storenames,
            this.properties.lockoptions
        );

        return results;
    }

    insertFileSync(file) {
        validators.validatePath(file);

        const data = fs.readFileSync(file, "utf8").split("\n");

        const checked = utils.checkData(data);

        const results = this.insertSync(checked.data);

        results.inspected = checked.inspected;
        results.errors = checked.errors;
        results.blanks = checked.blanks;

        return results;
    }

    async select(selecter, projecter) {
        validators.validateFunction(selecter);
        if (projecter) validators.validateFunction(projecter);

        var promises = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            promises.push(
                njodb.selectStoreData(
                    storepath,
                    selecter,
                    projecter,
                    this.properties.lockoptions
                )
            );
        }

        const results = await Promise.all(promises);
        return reduce.selectReduce(results);
    }

    selectSync(selecter, projecter) {
        validators.validateFunction(selecter);
        if (projecter) validators.validateFunction(projecter);

        var results = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            results.push(
                njodb.selectStoreDataSync(
                    storepath,
                    selecter,
                    projecter
                )
            );
        }

        return reduce.selectReduce(results);
    }

    async update(selecter, updater) {
        validators.validateFunction(selecter);
        validators.validateFunction(updater);

        var promises = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            const tempstorename = [storename, Date.now(), "tmp"].join(".");
            const tempstorepath = path.join(this.properties.temppath, tempstorename);

            promises.push(
                njodb.updateStoreData(
                    storepath,
                    selecter,
                    updater,
                    tempstorepath,
                    this.properties.lockoptions
                )
            );
        }

        const results = await Promise.all(promises);
        return  reduce.updateReduce(results);
    }

    updateSync(selecter, updater) {
        validators.validateFunction(selecter);
        validators.validateFunction(updater);

        var results = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            const tempstorename = [storename, Date.now(), "tmp"].join(".");
            const tempstorepath = path.join(this.properties.temppath, tempstorename);

            results.push(
                njodb.updateStoreDataSync(
                    storepath,
                    selecter,
                    updater,
                    tempstorepath
                )
            );
        }

        return  reduce.updateReduce(results);
    }

    async delete(selecter) {
        validators.validateFunction(selecter);

        var promises = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            const tempstorename = [storename, Date.now(), "tmp"].join(".");
            const tempstorepath = path.join(this.properties.temppath, tempstorename);

            promises.push(
                njodb.deleteStoreData(
                    storepath,
                    selecter,
                    tempstorepath,
                    this.properties.lockoptions
                )
            );
        }

        const results = await Promise.all(promises);
        return reduce.deleteReduce(results);
    }

    deleteSync(selecter) {
        validators.validateFunction(selecter);

        var results = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            const tempstorename = [storename, Date.now(), "tmp"].join(".");
            const tempstorepath = path.join(this.properties.temppath, tempstorename);

            results.push(
                njodb.deleteStoreDataSync(
                    storepath,
                    selecter,
                    tempstorepath
                )
            );
        }

        return reduce.deleteReduce(results);
    }

    async aggregate(selecter, indexer, projecter) {
        validators.validateFunction(selecter);
        validators.validateFunction(indexer);
        if (projecter) validators.validateFunction(projecter);

        var promises = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);

            promises.push(
                njodb.aggregateStoreData(
                    storepath,
                    selecter,
                    indexer,
                    projecter,
                    this.properties.lockoptions
                )
            );
        }

        const results = await Promise.all(promises);
        return reduce.aggregateReduce(results);
    }

    aggregateSync(selecter, indexer, projecter) {
        validators.validateFunction(selecter);
        validators.validateFunction(indexer);
        if (projecter) validators.validateFunction(projecter);

        var results = [];

        for (const storename of this.properties.storenames) {
            const storepath = path.join(this.properties.datapath, storename);

            results.push(
                njodb.aggregateStoreDataSync(
                    storepath,
                    selecter,
                    indexer,
                    projecter
                )
            );
        }

        return reduce.aggregateReduce(results);
    }
}

exports.Database = Database;
