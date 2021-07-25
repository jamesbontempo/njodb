const njodb = require("../index");
const fs = require("fs");
const path = require("path");
const expect = require("chai").expect;

const defaults = {
    "datadir": "data",
    "dataname": "data",
    "datapath": path.join(__dirname, "data"),
    "datastores": 5,
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
    "root": __dirname,
    "storenames": [],
    "tempdir": "tmp",
    "temppath": path.join(__dirname, "tmp")
};

const properties = {
    "datadir": "data",
    "dataname": "data",
    "datastores": 5,
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
    "tempdir": "tmp"
};

const userProperties = {
    "datadir": "mydata",
    "dataname": "data",
    "datapath": path.join(__dirname, "mydata"),
    "datastores": 2,
    "lockoptions": {
        "stale": 1000,
        "update": 1000,
        "retries": {
            "retries": 500,
            "minTimeout": 250,
            "maxTimeout": 5000,
            "factor": 0.15,
            "randomize": false
        }
    },
    "root": __dirname,
    "storenames": [],
    "tempdir": "tmp",
    "temppath": path.join(__dirname, "tmp")
};

const badJSON = "{\"datadir: \"data\",\"dataname\": \"data\",\"datastores\": 5,\"tempdir\": \"tmp\"}";

const firstNames = ["James", "John", "Robert", "Michael", "William", "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth"];
const lastNames = ["Smith", "Johsnon", "Williams", "Jones", "Brown", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"];
const states = ["Maryland", "District of Columbia", "Virginia", "California", "Connecticut", "Illinois"];
const regions = ["Northeast", "Southeast", "Midwest", "Southwest", "West"];

var inserts = [];
var selects = [];
var selectsProjection = [];
var updates = [];
var deletes = [];

var minFirstName = "William";
var maxFirstName = "Elizabeth";

var minLastName = "Williams";
var maxLastName = "Brown";

var minState = "Virginia";
var maxState = "California";

for (var i = 0; i < 50; i++) {
    const firstName = firstNames[Math.floor(Math.random()*firstNames.length)];
    const lastName = lastNames[Math.floor(Math.random()*lastNames.length)];
    const state = states[Math.floor(Math.random()*states.length)];

    if (firstName < minFirstName) minFirstName = firstName;
    if (firstName > maxFirstName) maxFirstName = firstName;

    if (lastName < minLastName) minLastName = lastName;
    if (lastName > maxLastName) maxLastName = lastName;

    if (state < minState) minState = state;
    if (state > maxState) maxState = state;

    const data = {
        id: i,
        firstName: firstName,
        lastName:lastName,
        state: state,
        favoriteNumber: i,
    };

    if (Math.random() < 0.5) {
        data.region = regions[Math.floor(Math.random()*regions.length)];
    }

    inserts.push(data);

    if (firstName === "James" && lastName !== "Smith") {
        selects.push(data);
        selectsProjection.push(
            {
                id: i,
                fullName: firstName + " " + lastName,
                newFavoriteNumber: i * 5
            }
        );
    }

    if (lastName === "Smith") {
        var update = {
            id: i,
            firstName: firstName,
            lastName:"Smythe",
            state: state,
            favoriteNumber: i,
        };
        if (data.region !== undefined) update.region = data.region;
        updates.push(update);
    }

    if (i % 5 === 0) {
        deletes.push(data);
    }
}

var db;

describe("NJODB async tests", () => {

    it("Creates a new NJODB instance", () => {
        const properties = Object.assign({}, defaults);
        delete properties.root;
        delete properties.datapath;
        delete properties.temppath;

        fs.writeFileSync(path.join(__dirname, "njodb.properties"), JSON.stringify(properties), "utf8");

        db = new njodb.Database(__dirname);
        expect(db.getProperties()).to.deep.equal(defaults);
        expect(fs.existsSync(path.join(defaults.root, "njodb.properties"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "data"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "tmp"))).to.equal(true);
    });

    it("Inserts data asynchronously", async () => {
        return db.insert(inserts).then(results => {
            expect(results.inserted).to.equal(inserts.length);
        });
    });

    it("Selects inserted data asynchronously", async () => {
        return db.select(()=>true).then(results => {
            expect(results.selected).to.equal(inserts.length);
        });
    });

    it("Selects some data asynchronously", async () => {
        return db.select(function(record) { return record.firstName === "James" && record.lastName !== "Smith"; }).then(results => {
            expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(selects);
            expect(results.selected).to.equal(selects.length);
            expect(results.ignored).to.equal(inserts.length - selects.length);
        });
    });

    it("Selects some data with projection asynchronously", async () => {
        return db.select(function(record) { return record.firstName === "James" && record.lastName !== "Smith"; }, function(record) { return {id: record.id, fullName: record.firstName + " " + record.lastName, newFavoriteNumber: record.favoriteNumber * 5}; }).then(results => {
            expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(selectsProjection);
            expect(results.selected).to.equal(selectsProjection.length);
            expect(results.ignored).to.equal(inserts.length - selectsProjection.length);
        });
    });

    it("Aggregates all data asynchronously", async () => {
        return db.aggregate(() => true, () => true).then(results => {
            expect(results.data[0].aggregates.length).to.equal(6);
            expect(results.data[0].count).to.equal(50);
            for (const aggregate of results.data[0].aggregates) {
                const field = aggregate.field;
                const data = aggregate.data;
                switch(field) {
                    case "id":
                        expect(data.min).to.equal(0);
                        expect(data.max).to.equal(49);
                        expect(data.sum).to.equal(1225);
                        expect(data.mean).to.equal(24.5);
                        expect(Math.abs(data.varp - 208.25) < 0.00000000001).to.equal(true);
                        expect(Math.abs(data.vars - 212.5) < 0.00000000001).to.equal(true);
                        expect(Math.abs(data.stdp - Math.sqrt(208.25)) < 0.00000000001).to.equal(true);
                        expect(Math.abs(data.stds - Math.sqrt(212.5)) < 0.00000000001).to.equal(true);
                        break;
                    case "firstName":
                        expect(data.min).to.equal(minFirstName);
                        expect(data.max).to.equal(maxFirstName);
                        expect(data.sum).to.equal(undefined);
                        expect(data.mean).to.equal(undefined);
                        expect(data.varp).to.equal(undefined);
                        expect(data.vars).to.equal(undefined);
                        expect(data.stdp).to.equal(undefined);
                        expect(data.stds).to.equal(undefined);
                        break;
                    case "lastName":
                        expect(data.min).to.equal(minLastName);
                        expect(data.max).to.equal(maxLastName);
                        expect(data.sum).to.equal(undefined);
                        expect(data.mean).to.equal(undefined);
                        expect(data.varp).to.equal(undefined);
                        expect(data.vars).to.equal(undefined);
                        expect(data.stdp).to.equal(undefined);
                        expect(data.stds).to.equal(undefined);
                        break;
                    case "state":
                        expect(data.min).to.equal(minState);
                        expect(data.max).to.equal(maxState);
                        expect(data.sum).to.equal(undefined);
                        expect(data.mean).to.equal(undefined);
                        expect(data.varp).to.equal(undefined);
                        expect(data.vars).to.equal(undefined);
                        expect(data.stdp).to.equal(undefined);
                        expect(data.stds).to.equal(undefined);
                        break;
                    case "favoriteNumber":
                        expect(data.min).to.equal(0);
                        expect(data.max).to.equal(49);
                        expect(data.sum).to.equal(1225);
                        expect(data.mean).to.equal(24.5);
                        expect(Math.abs(data.varp - 208.25) < 0.00000000001).to.equal(true);
                        expect(Math.abs(data.vars - 212.5) < 0.00000000001).to.equal(true);
                        expect(Math.abs(data.stdp - Math.sqrt(208.25)) < 0.00000000001).to.equal(true);
                        expect(Math.abs(data.stds - Math.sqrt(212.5)) < 0.00000000001).to.equal(true);
                        break;
                }
            }
        });
    });

    it("Aggregates all data with projection asynchronously", async () => {
        return db.aggregate(() => true, () => true, record => { return {id2: record.id * 2}; }).then(results => {
            expect(results.data[0].count).to.equal(50);
            const aggregates = results.data[0].aggregates;
            expect(aggregates.length).to.equal(1);
            const data = aggregates[0].data;
            expect(data.min).to.equal(0);
            expect(data.max).to.equal(98);
            expect(data.sum).to.equal(2450);
            expect(data.mean).to.equal(49);
            expect(Math.abs(data.varp - 833) < 0.00000000001).to.equal(true);
            expect(Math.abs(data.vars - 850) < 0.00000000001).to.equal(true);
            expect(Math.abs(data.stdp - Math.sqrt(833)) < 0.00000000001).to.equal(true);
            expect(Math.abs(data.stds - Math.sqrt(850)) < 0.00000000001).to.equal(true);
        });
    });

    it("Aggregrates some data asynchronously", async () => {
        return db.aggregate(function() { return true; }, function(record) { return record.region; }).then(results => {
            const indexed = inserts.filter(record => record.region !== undefined).length;
            expect(results.indexed).to.equal(indexed);
            expect(results.unindexed).to.equal(inserts.length - indexed);
        });
    });

    it("Updates some data asynchronously", async () => {
        return db.update(function(record) { return record.lastName === "Smith"; }, function(record) { record.lastName = "Smythe"; return record; })
            .then(results => {
                expect(results.updated).to.equal(updates.length);
                expect(results.unchanged).to.equal(inserts.length - updates.length);
            });
    });

    it("Selects updated data asynchronously", async () => {
        return db.select(r => r.lastName==="Smythe").then(results => {
            expect(results.selected).to.equal(updates.length);
        });
    });

    it("Deletes data asynchronously", async () => {
        return db.delete(function(record) { return record.id % 5 === 0; }).then(results => {
            expect(results.deleted).to.equal(deletes.length);
            expect(results.retained).to.equal(inserts.length - deletes.length);
        });
    });

    it("Selects deleted data asynchronously", async () => {
        return db.select(r => r.id%5===0).then(results => {
            expect(results.selected).to.equal(0);
            expect(results.ignored).to.equal(inserts.length - deletes.length);
        });
    });

    it("Grows database asynchronously", async () => {
        return db.grow().then(results => {
            expect(results.stores).to.equal(defaults.datastores + 1);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Resizes database asynchronously", async () => {
        return db.resize(3).then(results => {
            expect(results.stores).to.equal(3);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Shrinks database asynchronously", async () => {
        return db.shrink().then(results => {
            expect(results.stores).to.equal(2);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Gets statistics about the database asynchronously", async () => {
        return db.stats().then(results => {
            expect(results.stores).to.equal(2);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Inserts some data from a file asynchronously", async () => {
        return db.insertFile(path.join(__dirname, "data.json")).then(results => {
            expect(results.inserted).to.equal(50);
            expect(results.errors.length).to.equal(1);
            expect(results.blanks).to.equal(3);
        });
    });

    it("Inserts some more data asynchronously", async () => {
        return db.insert(inserts).then(results => {
            expect(results.inserted).to.equal(inserts.length);
        });
    });

    it("Drops database asynchronously", async () => {
        return db.drop().then((results) => {
            expect(results.dropped).to.equal(true);
        });
    });
});

describe("NJODB sync tests", () => {

    it("Creates a new NJODB instance", () => {
        fs.writeFileSync(path.join(__dirname, "njodb.properties"), JSON.stringify(properties));

        db = new njodb.Database(__dirname);
        expect(db.getProperties()).to.deep.equal(defaults);
        expect(fs.existsSync(path.join(defaults.root, "data"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "tmp"))).to.equal(true);
    });

    it("Inserts data synchronously", () => {
        const results = db.insertSync(inserts);
        expect(results.inserted).to.equal(inserts.length);
    });

    it("Selects inserted data synchronously", async () => {
        const results = db.selectSync(()=>true);
        expect(results.selected).to.equal(inserts.length);
    });

    it("Selects some data synchronously", () => {
        const results = db.selectSync(function(record) { return record.firstName === "James" && record.lastName !== "Smith"; });
        expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(selects);
        expect(results.selected).to.equal(selects.length);
        expect(results.ignored).to.equal(inserts.length - selects.length);
    });

    it("Select some data with projection synchronously", () => {
        const results = db.selectSync(function(record) { return record.firstName === "James" && record.lastName !== "Smith"; }, function(record) { return {id: record.id, fullName: record.firstName + " " + record.lastName, newFavoriteNumber: record.favoriteNumber * 5}; });
        expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(selectsProjection);
        expect(results.selected).to.equal(selectsProjection.length);
        expect(results.ignored).to.equal(inserts.length - selectsProjection.length);
    });

    it("Aggregates all data synchronously", async () => {
        const results = db.aggregateSync(() => true, () => true);
        expect(results.data[0].count).to.equal(50);
        expect(results.data[0].aggregates.length).to.equal(6);
        for (const aggregate of results.data[0].aggregates) {
            const field = aggregate.field;
            const data = aggregate.data;
            switch(field) {
                case "id":
                    expect(data.min).to.equal(0);
                    expect(data.max).to.equal(49);
                    expect(data.sum).to.equal(1225);
                    expect(data.mean).to.equal(24.5);
                    expect(Math.abs(data.varp - 208.25) < 0.00000000001).to.equal(true);
                    expect(Math.abs(data.vars - 212.5) < 0.00000000001).to.equal(true);
                    expect(Math.abs(data.stdp - Math.sqrt(208.25)) < 0.00000000001).to.equal(true);
                    expect(Math.abs(data.stds - Math.sqrt(212.5)) < 0.00000000001).to.equal(true);
                    break;
                case "firstName":
                    expect(data.min).to.equal(minFirstName);
                    expect(data.max).to.equal(maxFirstName);
                    expect(data.sum).to.equal(undefined);
                    expect(data.mean).to.equal(undefined);
                    expect(data.varp).to.equal(undefined);
                    expect(data.vars).to.equal(undefined);
                    expect(data.stdp).to.equal(undefined);
                    expect(data.stds).to.equal(undefined);
                    break;
                case "lastName":
                    expect(data.min).to.equal(minLastName);
                    expect(data.max).to.equal(maxLastName);
                    expect(data.sum).to.equal(undefined);
                    expect(data.mean).to.equal(undefined);
                    expect(data.varp).to.equal(undefined);
                    expect(data.vars).to.equal(undefined);
                    expect(data.stdp).to.equal(undefined);
                    expect(data.stds).to.equal(undefined);
                    break;
                case "state":
                    expect(data.min).to.equal(minState);
                    expect(data.max).to.equal(maxState);
                    expect(data.sum).to.equal(undefined);
                    expect(data.mean).to.equal(undefined);
                    expect(data.varp).to.equal(undefined);
                    expect(data.vars).to.equal(undefined);
                    expect(data.stdp).to.equal(undefined);
                    expect(data.stds).to.equal(undefined);
                    break;
                case "favoriteNumber":
                    expect(data.min).to.equal(0);
                    expect(data.max).to.equal(49);
                    expect(data.sum).to.equal(1225);
                    expect(data.mean).to.equal(24.5);
                    expect(Math.abs(data.varp - 208.25) < 0.00000000001).to.equal(true);
                    expect(Math.abs(data.vars - 212.5) < 0.00000000001).to.equal(true);
                    expect(Math.abs(data.stdp - Math.sqrt(208.25)) < 0.00000000001).to.equal(true);
                    expect(Math.abs(data.stds - Math.sqrt(212.5)) < 0.00000000001).to.equal(true);
                    break;
            }
        }
    });

    it("Aggregates some data with projection synchronously", async () => {
        const results = db.aggregateSync(() => true, record => record.state, record => { return {id2: record.id * 2}; });

        expect(results.data.length).to.equal(states.length);

        var min = Infinity;
        var max = 0;
        var sum = 0;
        var ss = 0;

        const maryland = inserts.filter(data => data.state === "Maryland");

        maryland.forEach(record => {
            if (record.id * 2 < min) min = record.id * 2;
            if (record.id * 2 > max) max = record.id * 2;
            sum += record.id * 2;
            ss += Math.pow(record.id * 2, 2);
        });

        const varp = (ss - (Math.pow(sum, 2)/maryland.length))/maryland.length;
        const vars = (ss - (Math.pow(sum, 2)/maryland.length))/(maryland.length - 1);

        const index = results.data.filter(data => data.index === "Maryland")[0];
        expect(index.count).to.equal(maryland.length);

        const aggregates = results.data.filter(data => data.index === "Maryland")[0].aggregates[0];
        expect(aggregates.data.min).to.equal(min);
        expect(aggregates.data.max).to.equal(max);
        expect(aggregates.data.sum).to.equal(sum);
        expect(aggregates.data.mean).to.equal(sum/maryland.length);
        expect(Math.abs(aggregates.data.varp - varp) < 0.00000000001).to.equal(true);
        expect(Math.abs(aggregates.data.vars - vars) < 0.00000000001).to.equal(true);
        expect(Math.abs(aggregates.data.stdp - Math.sqrt(varp)) < 0.00000000001).to.equal(true);
        expect(Math.abs(aggregates.data.stds - Math.sqrt(vars)) < 0.00000000001).to.equal(true);
    });

    it("Aggregrates some data synchronously", () => {
        const results = db.aggregateSync(function() { return true; }, function(record) { return record.region; });
        const indexed = inserts.filter(record => record.region !== undefined).length;
        expect(results.indexed).to.equal(indexed);
        expect(results.unindexed).to.equal(inserts.length - indexed);
    });

    it("Updates some data synchronously", () => {
        var results;

        results = db.updateSync(function(record) { return record.lastName === "Smith"; }, function(record) { record.lastName = "Smythe"; return record; });
        expect(results.updated).to.equal(updates.length);
        expect(results.unchanged).to.equal(inserts.length - updates.length);
    });

    it("Selects updated data synchronously", async () => {
        const results = db.selectSync(r=>r.lastName==="Smythe");
        expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(updates);
        expect(results.selected).to.equal(updates.length);
        expect(results.ignored).to.equal(inserts.length - updates.length);
    });

    it("Deletes data synchronously", () => {
        const results = db.deleteSync(function(record) { return record.id % 5 === 0; });
        expect(results.deleted).to.equal(deletes.length);
        expect(results.retained).to.equal(inserts.length - deletes.length);
    });

    it("Selects deleted data synchronously", async () => {
        const results = db.selectSync(r=>r.id%5===0);
        expect(results.selected).to.equal(0);
        expect(results.ignored).to.equal(inserts.length - deletes.length);
    });

    it("Grows database synchronously", () => {
        const results = db.growSync();
        expect(results.stores).to.equal(defaults.datastores + 1);
        expect(results.records).to.equal(inserts.length - deletes.length);
    });

    it("Shrinks database synchronously", () => {
        const results = db.shrinkSync();
        expect(results.stores).to.equal(defaults.datastores);
        expect(results.records).to.equal(inserts.length - deletes.length);
    });

    it("Resizes database synchronously", () => {
        const results = db.resizeSync(1);
        expect(results.stores).to.equal(1);
        expect(results.records).to.equal(inserts.length - deletes.length);
    });

    it("Inserts some data from a file synchronously", () => {
        const results = db.insertFileSync(path.join(__dirname, "data.json"));
        expect(results.inserted).to.equal(50);
        expect(results.errors.length).to.equal(1);
        expect(results.blanks).to.equal(4);
    });

    it("Gets statistics about the database synchronously", () => {
        const results = db.statsSync();
        expect(results.stores).to.equal(1);
        expect(results.records).to.equal(inserts.length - deletes.length + 50);
    });

    it("Drops database synchronously", () => {
        const results = db.dropSync();
        expect(results.dropped).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "njodb.properties"))).to.equal(false);
        expect(fs.existsSync(path.join(defaults.root, "data"))).to.equal(false);
        expect(fs.existsSync(path.join(defaults.root, "tmp"))).to.equal(false);
    });

    it("Creates a new NJODB instance with user-supplied properties and then drops it synchronously", () => {
        db = new njodb.Database(__dirname, {datadir: "mydata", datastores: 2, lockoptions: {stale: 1000, retries: {retries: 500}}});
        expect(db.getProperties()).to.deep.equal(userProperties);
        expect(fs.existsSync(path.join(defaults.root, "mydata"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "tmp"))).to.equal(true);

        db.dropSync();
    });

    it("Creates a database in CWD and then drops it synchronously", () => {
        const db = new njodb.Database();
        expect(fs.existsSync(path.join(process.cwd(), "njodb.properties"))).to.equal(true);
        expect(fs.existsSync(path.join(process.cwd(), "data"))).to.equal(true);
        expect(fs.existsSync(path.join(process.cwd(), "tmp"))).to.equal(true);

        db.dropSync();
    });

    it("Creates a database in a directory that doesn't exist and then drops it synchronously", () => {
        const db = new njodb.Database("./new_root");
        expect(fs.existsSync(path.join("./new_root", "njodb.properties"))).to.equal(true);
        expect(fs.existsSync(path.join("./new_root", "data"))).to.equal(true);
        expect(fs.existsSync(path.join("./new_root", "tmp"))).to.equal(true);

        db.dropSync();
        fs.rmdirSync("./new_root");
    });
});

describe("NJODB error tests", () => {

    it("Tries to use bad JSON in properties file", () => {
        fs.writeFileSync(path.join(__dirname, "njodb.properties"), badJSON, "utf8");

        let error = null;

        try {
            db = new njodb.Database(__dirname);
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");

        var properties = Object.assign({}, defaults);
        delete properties.root;
        delete properties.datapath;
        delete properties.temppath;

        fs.writeFileSync(path.join(__dirname, "njodb.properties"), JSON.stringify(properties), "utf8");
    });

    it("Tries to create a database using a bad path", () => {
        let error = null;

        try {
            new njodb.Database(0);
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            new njodb.Database("");
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            new njodb.Database("/path/that/doesnt/exist");
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
    });

    it("Tries to create a database using bad user-supplied properties", () => {
        let error = null;

        try {
            new njodb.Database(__dirname, "test");
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
    });

    it("Tries to set bad properties", () => {
        let error = null;

        db = new njodb.Database(__dirname);

        try {
            db.setProperties("non-object");
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            db.setProperties({datadir: 5});
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            db.setProperties({datadir: "data", dataname: ""});
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            db.setProperties({datadir: "data", dataname: "data", datastores: "five"});
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            db.setProperties({datadir: "data", dataname: "data", datastores: -1});
        } catch(e) {
            error = e;

        }

        expect(error).to.be.an("Error");

        var properties = {};
        Object.keys(db.getProperties()).sort().forEach(p => properties[p] = db.properties[p]);
        expect(properties).to.deep.equal(defaults);
    });

    it("Tries to insert data from a file using bad information}", () => {
        let error = null;

        try {
            db.insertFileSync(0);
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            db.insertFileSync("");
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
        error = null;

        try {
            db.insertFileSync("/path/to/a/nonexistent/file");
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Inserts data synchronously", () => {
        const results = db.insertSync(inserts);
        expect(results.inserted).to.equal(inserts.length);
    });

    it("Inserts more data synchronously and adds a bad record to a datastore file", () => {
        const results = db.insertSync(inserts);
        expect(results.inserted).to.equal(inserts.length);

        const badData = "{\"id\":5,\"firstName\":\"James\",\"lastName:\"Marshall\"}\n";
        fs.appendFileSync(path.join(__dirname, "data", "data.0.json"), badData);
    });

    it("Gets database stats asynchronously and finds bad record", async () => {
        return db.stats().then(results => {
            expect(results.records).to.equal(inserts.length * 2);
            expect(results.errors).to.equal(1);
        });
    })

    it("Gets database stats synchronously and finds bad record", () => {
        const results = db.statsSync();
        expect(results.errors).to.equal(1);
    })

    it("Selects data asynchronously and finds bad record", async () => {
        return db.select(function() { return true; }).then(results => {
            expect(results.errors).to.equal(1);
        });
    });

    it("Selects data synchronously and finds bad record", () => {
        const results = db.selectSync(function() { return true; });
        expect(results.errors).to.equal(1);
    });

    it("Tries to insert data that is not an array", () => {
        let error = null;

        try {
            db.insertSync("test");
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to select records without providing a selecter function", () => {
        let error = null;

        try {
            db.selectSync();
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to select records with a selecter function that doesn't return a boolean", () => {
        let error = null;

        try {
            db.selectSync(()=>{return {};});
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to select records with a selecter function that throws an error", () => {
        const results = db.selectSync(()=>{throw new Error("Test error");});
        expect(results.selected).to.equal(0);
    });

    it("Tries to select records with a projecter function that throws an error", () => {
        const results = db.selectSync(()=>true, ()=>{throw new Error("Test error");});
        results.data.forEach(data => expect(data).to.equal(undefined));
    });

    it("Tries to select records with a projecter function that doesn't return an object", () => {
        let error = null;

        try {
            db.selectSync(()=>true, ()=>[]);
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Aggregates data asynchronously and finds bad record", async () => {
        return db.aggregate(function() { return true; }, function(record) { return record.id; }).then(results => {
            expect(results.errors).to.equal(1);
        });
    });

    it("Aggregates data synchronously and finds bad record", () => {
        const results = db.aggregateSync(function() { return true; }, function(record) { return record.id; });
        expect(results.errors).to.equal(1);
    });

    it("Updates data asynchronously and finds bad record", async () => {
        return db.update(function(record) { return record.lastName === "Smith"; }, function(record) { record.lastName = "Smythe"; return record; }).then(results => {
            expect(results.errors).to.equal(1);
        });
    });

    it("Updates data synchronously and finds bad record", () => {
        const results = db.updateSync(function(record) { return record.lastName === "Smythe"; }, function(record) { record.lastName = "Smithe"; return record; });
        expect(results.errors).to.equal(1);
    });

    it("Tries to update records with an updater function that doesn't return an object", () => {
        let error = null;

        try {
            db.updateSync(()=>true, r=>{r.region==="New Region"; return "New Region"});
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to update records with an updater function that throws an error", () => {
        const results = db.updateSync(()=>true, ()=>{throw new Error("Test error");});
        expect(results.updated).to.equal(0);
    });

    it("Tries to aggregate data using an indexer that doesn't return a value", () => {
        let error = null;

        try {
            db.aggregateSync(record => record.id <= 10, function(record) { record.state !== undefined; });
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to aggregates data with an indexer that throws an error", async () => {
        return db.aggregate(() => true, () => {throw new Error("Test error")}).catch(error => {
            expect(error).to.be.an("Error");
        });
    });

    it("Tries to aggregates data with a projecter that doesn't return an object", async () => {
        let error = null;
        try {
            db.aggregateSync(() => true, r => r.id, () => []);
        } catch(e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to resize database to zero datastores", () => {
        let error = null;

        try {
            db.resizeSync(0);
        } catch (e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to resize database to a non-number value", () => {
        let error = null;

        try {
            db.resizeSync("three");
        } catch (e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Resizes database asynchronously and finds bad record", async () => {
        return db.resize(2).then(results => {
            expect(results.stores).to.equal(2);
            expect(results.records).to.equal(inserts.length*2);
            expect(results.errors.length).to.equal(1);
        });
    });

    it("Resizes database synchronously and finds bad record", () => {
        const results = db.resizeSync(1);
        expect(results.stores).to.equal(1);
        expect(results.records).to.equal(inserts.length*2);
        expect(results.errors.length).to.equal(1);
    });

    it("Tries to shrink database too far asynchronously but is stopped", async () => {
        let error = null;

        try {
            await db.shrink();
        } catch (e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Tries to shrink the database too far synchronously but is stopped", () => {
        let error = null;

        try {
            db.shrinkSync();
        } catch (e) {
            error = e;
        }

        expect(error).to.be.an("Error");
    });

    it("Deletes data synchronously and finds bad record", () => {
        const results = db.deleteSync(function(record) { return record.firstName === "James"; });
        expect(results.errors).to.equal(1);
    });

    it("Deletes data asynchronously and finds bad record", async () => {
        return db.delete(function(record) { return record.lastName === "Williams"; }).then(results => {
            expect(results.errors).to.equal(1);
        });
    });

    it("Drops database synchronously", () => {
        const results = db.dropSync();
        expect(results.dropped).to.equal(true);
    });

});