const fs = require("fs");
const path = require("path");
const njodb = require("./lib/njodb");
const reduce = require("./lib/reduce");
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
        utils.checkValue("root", root, false, "string", function(root) { return root.length > 0; });

        this.properties = {};
        this.properties.root = (root) ? root : process.cwd();

        const propertiesFile = path.join(this.properties.root, "njodb.properties");

        if (fs.existsSync(propertiesFile)) {
            this.setProperties(JSON.parse(fs.readFileSync(propertiesFile)));
        } else {
            if (!fs.existsSync(this.properties.root)) fs.mkdirSync(this.properties.root);
            this.setProperties(defaults);
        }

        if (!fs.existsSync(this.properties.datapath)) fs.mkdirSync(this.properties.datapath);
        if (!fs.existsSync(this.properties.temppath)) fs.mkdirSync(this.properties.temppath);

        return this;
    }

    // Database management methods

    getProperties() {
        return this.properties;
    }

    setProperties(properties) {
        utils.checkValue("properties", properties, true, "object");

        this.properties.datadir = (properties.datadir && typeof properties.datadir === "string") ? properties.datadir : defaults.datadir;
        this.properties.dataname = (properties.dataname && typeof properties.dataname === "string") ? properties.dataname : defaults.dataname;
        this.properties.datastores = (properties.datastores && typeof properties.datastores === "number") ? properties.datastores : defaults.datastores;
        this.properties.tempdir = (properties.tempdir && typeof properties.tempdir === "string") ? properties.tempdir : defaults.tempdir;
        this.properties.lockoptions = (properties.lockoptions && typeof properties.lockoptions === "object") ? properties.lockoptions : defaults.lockoptions;
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

        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
            const storepath = path.join(this.properties.datapath, storename);

            promises.push(
                njodb.statsStoreData(
                    storepath,
                    this.properties.lockoptions
                )
            );
        }

        const results = await Promise.all(promises);

        return Object.assign(stats, reduce.getStatsReduce(results));
    }

    statsSync() {
        var stats = {
            root: path.resolve(this.properties.root),
            data: path.resolve(this.properties.datapath),
            temp: path.resolve(this.properties.temppath)
        };

        var results = [];

        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
            const storepath = path.join(this.properties.datapath, storename);
            results.push(njodb.statsStoreDataSync(storepath));
        }

        return Object.assign(stats, reduce.getStatsReduce(results));
    }

    async getStats() {
        return this.stats();
    }

    getStatsSync() {
        return this.statsSync();
    }

    async grow() {
        this.properties.datastores++;
        saveProperties(this.properties.root, this.properties);

        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        const results = await njodb.distributeStoreData(
            this.properties.datapath,
            this.properties.dataname,
            storenames,
            this.properties.temppath,
            this.properties.datastores,
            this.properties.lockoptions
        );

        return results;
    }

    growSync() {
        this.properties.datastores++;
        saveProperties(this.properties.root, this.properties);

        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        const results = njodb.distributeStoreDataSync(
            this.properties.datapath,
            this.properties.dataname,
            storenames,
            this.properties.temppath,
            this.properties.datastores
        );

        return results;
    }

    async shrink() {
        if (this.properties.datastores > 1) {
            this.properties.datastores--;
            saveProperties(this.properties.root, this.properties);

            const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

            const results = await njodb.distributeStoreData(
                this.properties.datapath,
                this.properties.dataname,
                storenames,
                this.properties.temppath,
                this.properties.datastores,
                this.properties.lockoptions
            );

            return results;
        } else {
            throw new Error("database cannot shrink any further");
        }
    }

    shrinkSync() {
        if (this.properties.datastores > 1) {
            this.properties.datastores--;
            saveProperties(this.properties.root, this.properties);

            const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

            const results = njodb.distributeStoreDataSync(
                this.properties.datapath,
                this.properties.dataname,
                storenames,
                this.properties.temppath,
                this.properties.datastores
            );

            return results;
        } else {
            throw new Error("database cannot shrink any further");
        }
    }

    async resize(size) {
        utils.checkValue("size", size, false, "number", function(size) { return size > 0; });

        this.properties.datastores = size;
        saveProperties(this.properties.root, this.properties);

        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        const results = await njodb.distributeStoreData(
            this.properties.datapath,
            this.properties.dataname,
            storenames,
            this.properties.temppath,
            size,
            this.properties.lockoptions
        );

        return results;
    }

    resizeSync(size) {
        utils.checkValue("size", size, false, "number", function(size) { return size > 0; });

        this.properties.datastores = size;
        saveProperties(this.properties.root, this.properties);

        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        const results = njodb.distributeStoreDataSync(
            this.properties.datapath,
            this.properties.dataname,
            storenames,
            this.properties.temppath,
            size
        );

        return results;
    }

    async drop() {
        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        const results = await njodb.dropEverything(
            this.properties.root,
            this.properties.datapath,
            storenames,
            this.properties.temppath,
            this.properties.lockoptions
        );

        return reduce.dropReduce(results);
    }

    dropSync() {
        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        const results = njodb.dropEverythingSync(
            this.properties.root,
            this.properties.datapath,
            storenames,
            this.properties.temppath
        );

        return reduce.dropReduce(results);
    }

    // Data manipulation methods

    async insert(data) {
        utils.checkValue("data", data, true, "array", function(data) { return data.length > 0; });

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
        return reduce.insertReduce(results);
    }

    insertSync(data) {
        utils.checkValue("data", data, true, "array", data => data.length > 0);

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

        return reduce.insertReduce(results);
    }

    async insertFile(file) {
        utils.checkValue("file", file, true, "string", function(file) { return fs.existsSync(file); });

        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        const results = await njodb.insertFileData(
            file,
            this.properties.datapath,
            storenames,
            this.properties.lockoptions
        );

        return results;
    }

    insertFileSync(file) {
        utils.checkValue("file", file, true, "string", function(file) { return fs.existsSync(file); });

        const data = fs.readFileSync(file, "utf8").split("\n");

        const checked = utils.checkData(data);

        const results = this.insertSync(checked.data);

        results.inspected = checked.inspected;
        results.errors = checked.errors;
        results.blanks = checked.blanks;

        return results;
    }

    async select(selecter, projecter) {
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("projecter", projecter, false, "function");

        var promises = [];
        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("projecter", projecter, false, "function");

        var results = [];
        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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

    async aggregate(selecter, indexer, projecter) {
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("indexer", indexer, true, "function");
        utils.checkValue("projecter", projecter, false, "function");

        var promises = [];
        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("indexer", indexer, true, "function");

        utils.checkValue(
            "projecter",
            projecter,
            false,
            "function"
        );

        var results = [];
        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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

    async update(selecter, updater) {
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("updater", updater, true, "function");

        var promises = [];
        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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
        utils.checkValue("selecter", selecter, true, "function");
        utils.checkValue("updater", updater, true, "function");

        var results = [];
        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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
        utils.checkValue("selecter", selecter, true, "function");

        var promises = [];
        const storenames = await njodb.getStoreNames(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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
        utils.checkValue("selecter", selecter, true, "function");

        var results = [];
        const storenames = njodb.getStoreNamesSync(this.properties.datapath, this.properties.dataname);

        for (const storename of storenames) {
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
}

exports.Database = Database;
