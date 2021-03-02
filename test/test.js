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
    "tempdir": "tmp",
    "temppath": path.join(__dirname, "tmp")
};

const firstNames = ["James", "John", "Robert", "Michael", "William", "Mary", "Patricia", "Jennifer", "Linda", "Elizabeth"];
const lastNames = ["Smith", "Johsnon", "Williams", "Jones", "Brown", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"];
const states = ["Maryland", "District of Columbia", "Virginia", "California", "Connecticut", "Illinois"];

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
        updates.push(
            {
                id: i,
                firstName: firstName,
                lastName:"Smythe",
                state: state,
                favoriteNumber: i,
            }
        );
    }

    if (i % 5 === 0) {
        deletes.push(data);
    }
}

var db;

describe("NJODB tests", function() {

    it("Constructor", function() {
        db = new njodb.Database(__dirname);
        expect(db.getProperties()).to.deep.equal(defaults);
        expect(fs.existsSync(path.join(defaults.root, "njodb.properties"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "data"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "tmp"))).to.equal(true);
    });

    it("Insert", async () => {
        return db.insert(inserts).then(results => {
            expect(results.inserted).to.equal(inserts.length);
        });
    });

    it("Select", async () => {
        return db.select(function(record) { return record.firstName === "James" && record.lastName !== "Smith"; }).then(results => {
            expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(selects);
            expect(results.selected).to.equal(selects.length);
            expect(results.ignored).to.equal(inserts.length - selects.length);
        });
    });

    it("Select with projection", async () => {
        return db.select(function(record) { return record.firstName === "James" && record.lastName !== "Smith"; }, function(record) { return {id: record.id, fullName: record.firstName + " " + record.lastName, newFavoriteNumber: record.favoriteNumber * 5}; }).then(results => {
            expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(selectsProjection);
            expect(results.selected).to.equal(selectsProjection.length);
            expect(results.ignored).to.equal(inserts.length - selectsProjection.length);
        });
    });

    it("Update", async () => {
        return db.update(function(record) { return record.lastName === "Smith"; }, function(record) { record.lastName = "Smythe"; return record; }).then(results => {
            expect(results.updated).to.equal(updates.length);
            expect(results.unchanged).to.equal(inserts.length - updates.length);
            db.select(function(record) { return record.lastName === "Smythe"; }).then(results => {
                expect(results.data.sort((a, b) => a.id - b.id)).to.deep.equal(updates);
                expect(results.selected).to.equal(updates.length);
                expect(results.ignored).to.equal(inserts.length - updates.length);
            });
        });
    });

    it("Aggregate", async () => {
        return db.aggregate(function() { return true; }, function() { return true; }).then(results => {
            expect(results.data[0].aggregates.length).to.equal(5);
            for (var i = 0; i < results.data[0].aggregates.length; i++) {
                switch(results.data[0].aggregates[i].field) {
                    case "id":
                        expect(results.data[0].aggregates[i].data.min).to.equal(0);
                        expect(results.data[0].aggregates[i].data.max).to.equal(49);
                        expect(results.data[0].aggregates[i].data.count).to.equal(50);
                        expect(results.data[0].aggregates[i].data.sum).to.equal(1225);
                        expect(results.data[0].aggregates[i].data.mean).to.equal(24.5);
                        expect(Math.abs(results.data[0].aggregates[i].data.varp - 208.25) < 0.00000000001).to.equal(true);
                        expect(Math.abs(results.data[0].aggregates[i].data.vars - 212.5) < 0.00000000001).to.equal(true);
                        expect(Math.abs(results.data[0].aggregates[i].data.stdp - Math.sqrt(208.25)) < 0.00000000001).to.equal(true);
                        expect(Math.abs(results.data[0].aggregates[i].data.stds - Math.sqrt(212.5)) < 0.00000000001).to.equal(true);
                        break;
                    case "firstName":
                        expect(results.data[0].aggregates[i].data.min).to.equal(minFirstName);
                        expect(results.data[0].aggregates[i].data.max).to.equal(maxFirstName);
                        expect(results.data[0].aggregates[i].data.count).to.equal(50);
                        expect(results.data[0].aggregates[i].data.sum).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.mean).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.varp).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.vars).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.stdp).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.stds).to.equal(undefined);
                        break;
                    case "lastName":
                        expect(results.data[0].aggregates[i].data.min).to.equal(minLastName);
                        expect(results.data[0].aggregates[i].data.max).to.equal(maxLastName);
                        expect(results.data[0].aggregates[i].data.count).to.equal(50);
                        expect(results.data[0].aggregates[i].data.sum).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.mean).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.varp).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.vars).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.stdp).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.stds).to.equal(undefined);
                        break;
                    case "state":
                        expect(results.data[0].aggregates[i].data.min).to.equal(minState);
                        expect(results.data[0].aggregates[i].data.max).to.equal(maxState);
                        expect(results.data[0].aggregates[i].data.count).to.equal(50);
                        expect(results.data[0].aggregates[i].data.sum).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.mean).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.varp).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.vars).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.stdp).to.equal(undefined);
                        expect(results.data[0].aggregates[i].data.stds).to.equal(undefined);
                        break;
                    case "favoriteNumber":
                        expect(results.data[0].aggregates[i].data.min).to.equal(0);
                        expect(results.data[0].aggregates[i].data.max).to.equal(49);
                        expect(results.data[0].aggregates[i].data.count).to.equal(50);
                        expect(results.data[0].aggregates[i].data.sum).to.equal(1225);
                        expect(results.data[0].aggregates[i].data.mean).to.equal(24.5);
                        expect(Math.abs(results.data[0].aggregates[i].data.varp - 208.25) < 0.00000000001).to.equal(true);
                        expect(Math.abs(results.data[0].aggregates[i].data.vars - 212.5) < 0.00000000001).to.equal(true);
                        expect(Math.abs(results.data[0].aggregates[i].data.stdp - Math.sqrt(208.25)) < 0.00000000001).to.equal(true);
                        expect(Math.abs(results.data[0].aggregates[i].data.stds - Math.sqrt(212.5)) < 0.00000000001).to.equal(true);
                        break;
                }
            }
        });
    });

    it("Aggregate with projection", async () => {
        return db.aggregate(function() { return true; }, function() { return true; }, function(record) { return {id2: record.id * 2}; }).then(results => {
            expect(results.data[0].aggregates.length).to.equal(1);
            expect(results.data[0].aggregates[0].data.min).to.equal(0);
            expect(results.data[0].aggregates[0].data.max).to.equal(98);
            expect(results.data[0].aggregates[0].data.count).to.equal(50);
            expect(results.data[0].aggregates[0].data.sum).to.equal(2450);
            expect(results.data[0].aggregates[0].data.mean).to.equal(49);
            expect(Math.abs(results.data[0].aggregates[0].data.varp - 833) < 0.00000000001).to.equal(true);
            expect(Math.abs(results.data[0].aggregates[0].data.vars - 850) < 0.00000000001).to.equal(true);
            expect(Math.abs(results.data[0].aggregates[0].data.stdp - Math.sqrt(833)) < 0.00000000001).to.equal(true);
            expect(Math.abs(results.data[0].aggregates[0].data.stds - Math.sqrt(850)) < 0.00000000001).to.equal(true);
        });
    });

    it("Delete", async () => {
        return db.delete(function(record) { return record.id % 5 === 0; }).then(results => {
            expect(results.deleted).to.equal(deletes.length);
            expect(results.retained).to.equal(inserts.length - deletes.length);
        });
    })

    it("Grow", async () => {
        return db.grow().then(results => {
            expect(results.stores).to.equal(defaults.datastores + 1);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Resize", async () => {
        return db.resize(3).then(results => {
            expect(results.stores).to.equal(3);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Shrink", async () => {
        return db.shrink().then(results => {
            expect(results.stores).to.equal(2);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Stats", async () => {
        return db.getStats().then(results => {
            expect(results.stores).to.equal(2);
            expect(results.records).to.equal(inserts.length - deletes.length);
        })
    });

    it("Drop", function() {
        return db.drop().then((results) => {
            expect(results.dropped).to.equal(true);
        });
    });

});


