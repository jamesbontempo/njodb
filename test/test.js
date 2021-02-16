const njodb = require("../index");
const fs = require("fs");
const path = require("path");
const lockfile = require("proper-lockfile");
const expect = require("chai").expect;

const defaults = {
    "datadir": "data",
    "dataname": "data",
    "datapath": path.join(__dirname, "data"),
    "datastores": 5,
    "debug": false,
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

const modified = Date.now();

const data = [
    {
        id: 1,
        name: "James",
        nickname: "Good Times",
        modified: modified
    },
    {
        id: 2,
        name: "Steve",
        nickname: "Esteban",
        modified: modified + 1
    }
];

const update = [
    {
        id: 1,
        name: "James",
        nickname: "Bulldog",
        modified: modified
    }
];

const db = new njodb.Database(__dirname);

describe("NJODB Tests", function() {

    it("Constructor", function() {
        expect(db.getProperties()).to.deep.equal(defaults);
        expect(fs.existsSync(path.join(defaults.root, "njodb.properties"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "data"))).to.equal(true);
        expect(fs.existsSync(path.join(defaults.root, "tmp"))).to.equal(true);
    });

    it("Insert", function() {
        db.insert(data).then(results => {
            expect(results.inserted).to.equal(2);
        });
    });

    it("Select", function() {
        db.select(function(record) { return record.id === 1 || record.name === "Steve"}).then(results => {
            expect(results.data).to.deep.equal(data);
            expect(results.selected).to.equal(2);
            expect(results.ignored).to.equal(0);
        });
    });

    it("Update", function() {
        db.update(function(record) { return record.name === "James"}, function(record) { record.nickname = "Bulldog"; return record; }).then(results => {
            expect(results.updated).to.equal(1);
            expect(results.unchanged).to.equal(1);
            db.select(function(record) { return record.id === 1 }).then(results => {
                expect(results.data).to.deep.equal(update);
                expect(results.selected).to.equal(1);
                expect(results.ignored).to.equal(0);
            });
        });
    });

    it("Delete", function() {
        db.delete(function(record) { return record.modified < Date.now()}).then(results => {
            expect(results.deleted).to.equal(2);
            expect(results.retained).to.equal(0);
            db.select(function(record) { return true }).then(results => {
                expect(results.data).to.deep.equal([]);
                expect(results.selected).to.equal(0);
                expect(results.ignored).to.equal(0);
            });
        });
    })

    after(function() {
        fs.unlinkSync(path.join(defaults.root, "njodb.properties"));
        fs.rmdirSync(path.join(defaults.root, "data"), {recursive: true});
        fs.rmdirSync(path.join(defaults.root, "tmp"), {recursive: true});
    });
});

