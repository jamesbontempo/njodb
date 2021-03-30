"use strict";

const utils = require("./utils");

const Randomizer = (data, replacement) => {
    var mutable = [...data];
    if (replacement === undefined || typeof replacement !== "boolean") replacement = true;

    function _next() {
        var selection;
        const index = Math.floor(Math.random()*mutable.length);

        if (replacement) {
            selection = mutable.slice(index, index + 1)[0];
        } else {
            selection = mutable.splice(index, 1)[0];
            if (mutable.length === 0) mutable = [...data];
        }

        return selection;
    }

    return {
        next: _next
    };
};

const Result = (type) => {
    var _result;

    switch (type) {
        case "stats":
            _result = {
                size: 0,
                lines: 0,
                records: 0,
                errors: [],
                blanks: 0,
                created: undefined,
                modified: undefined,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
        case "distribute":
            _result = {
                stores: undefined,
                records: 0,
                errors: [],
                start: Date.now(),
                end: undefined,
                elapsed: undefined
            };
            break;
        case "insert":
            _result = {
                inserted: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
        case "select":
            _result = {
                lines: 0,
                selected: 0,
                ignored: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: [],
            };
            break;
        case "update":
            _result = {
                lines: 0,
                updated: 0,
                unchanged: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: []
            };
            break;
        case "delete":
            _result = {
                lines: 0,
                deleted: 0,
                retained: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: []
            };
            break;
        case "aggregate":
            _result = {
                lines: 0,
                aggregates: {},
                indexed: 0,
                unindexed: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
    }

    return _result;
}

const Reduce = (type) => {
    var _reduce;

    switch (type) {
        case "stats":
            _reduce = Object.assign(Result("stats"), {
                errors: 0,
                stores: 0,
                min: undefined,
                max: undefined,
                mean: undefined,
                var: undefined,
                std: undefined,
                m2: 0
            });
            break;
        case "drop":
            _reduce = {
                dropped: false,
                start: Date.now(),
                end: 0,
                elapsed: 0
            };
            break;
        case "insert":
            _reduce = Result("insert");
            break;
        case "select":
            _reduce = Object.assign(Result("select"), {
                errors: 0
            });
            break;
        case "update":
            _reduce = Object.assign(Result("update"), {
                errors: 0
            });
            break;
        case "delete":
            _reduce = Object.assign(Result("delete"), {
                errors: 0
            });
            break;
        case "aggregate":
            _reduce = Object.assign(Result("aggregate"), {
                errors: 0,
                data: []
            });
            break;
    }

    _reduce.details = undefined;

    return _reduce;
};

const Handler = (type, ...functions) => {
    var _results = Result(type);

    const _next = (record, writer) => {
        record = new Record(record);
        _results.lines++;

        if (record.length === 0) {
            _results.blanks++;
        } else {
            if (record.data) {
                switch (type) {
                    case "stats":
                        statsHandler(record, _results);
                        break;
                    case "select":
                        selectHandler(record, functions[0], functions[1], _results);
                        break;
                    case "update":
                        updateHandler(record, functions[0], functions[1], writer, _results);
                        break;
                    case "delete":
                        deleteHandler(record, functions[0], writer, _results);
                        break;
                    case "aggregate":
                        aggregateHandler(record, functions[0], functions[1], functions[2], _results);
                        break;
                }
            } else {
                _results.errors.push({error: record.error, line: _results.lines, data: record.source});

                if (type === "update" || type === "delete") {
                    if (writer) {
                        writer.write(record.source) + "\n";
                    } else {
                        _results.data.push(record.source);
                    }
                }
            }
        }
    };

    const _return = () => {
        _results.end = Date.now();
        _results.elapsed = _results.end - _results.start;
        return _results;
    }

    return {
        next: _next,
        return: _return
    };
};

const statsHandler = (record, results) => {
    results.records++;
    return results;
};

const selectHandler = (record, selecter, projecter, results) => {
    if (record.select(selecter)) {
        if (projecter) {
            results.data.push(record.project(projecter));
        } else {
            results.data.push(record.data);
        }
        results.selected++;
    } else {
        results.ignored++;
    }
};

const updateHandler = (record, selecter, updater, writer, results) => {
    if (record.select(selecter)) {
        if (record.update(updater)) {
            results.updated++;
        } else {
            results.unchanged++;
        }
    } else {
        results.unchanged++;
    }

    if (writer) {
        writer.write(JSON.stringify(record.data) + "\n");
    } else {
        results.data.push(JSON.stringify(record.data));
    }
};

const deleteHandler = (record, selecter, writer, results) => {
    if (record.select(selecter)) {
        results.deleted++;
    } else {
        results.retained++;

        if (writer) {
            writer.write(JSON.stringify(record.data) + "\n");
        } else {
            results.data.push(JSON.stringify(record.data));
        }
    }
};

const aggregateHandler = (record, selecter, indexer, projecter, results) => {
    if (record.select(selecter)) {
        const index = record.index(indexer);

        if (!index) {
            results.unindexed++;
        } else {
            var projection;
            var fields;

            if (results.aggregates[index]) {
                results.aggregates[index].count++;
            } else {
                results.aggregates[index] = {
                    count: 1,
                    aggregates: {}
                };
            }

            if (projecter) {
                projection = record.project(projecter);
                fields = Object.keys(projection);
            } else {
                projection = record.data;
                fields = Object.keys(record.data);
            }

            for (const field of fields) {
                if (projection[field] !== undefined) {
                    if (results.aggregates[index].aggregates[field]) {
                        accumulateAggregate(results.aggregates[index].aggregates[field], projection[field]);
                    } else {
                        results.aggregates[index].aggregates[field] = {
                            min: projection[field],
                            max: projection[field],
                            count: 1
                        };
                        if (typeof projection[field] === "number") {
                            results.aggregates[index].aggregates[field]["sum"] = projection[field];
                            results.aggregates[index].aggregates[field]["mean"] = projection[field];
                            results.aggregates[index].aggregates[field]["m2"] = 0;
                        }
                    }
                }
            }

            results.indexed++;
        }
    }
}

const accumulateAggregate = (index, projection) => {
    index["min"] = utils.min(index["min"], projection);
    index["max"] = utils.max(index["max"], projection);
    index["count"]++;

    // Welford's algorithm
    if (typeof projection === "number") {
        const delta1 = projection - index["mean"];
        index["sum"] += projection;
        index["mean"] += delta1 / index["count"];
        const delta2 = projection - index["mean"];
        index["m2"] += delta1 * delta2;
    }

    return index;
};

class Record {
    constructor(record) {
        this.source = record.trim();
        this.length = this.source.length
        this.data = {};
        this.error = "";

        try {
            this.data = JSON.parse(this.source)
        } catch(e) {
            this.data = undefined;
            this.error = e.message;
        }
    }
}

Record.prototype.select = function(selecter) {
    var result;

    try {
        result = selecter(this.data);
    } catch {
        return false;
    }

    if (typeof result !== "boolean") {
        throw new TypeError("Selecter must return a boolean");
    } else {
        return result;
    }
};

Record.prototype.update = function(updater) {
    var result;

    try {
        result = updater(this.data);
    } catch {
        return false;
    }

    if (typeof result !== "object") {
        throw new TypeError("Updater must return an object");
    } else {
        this.data = result;
        return true;
    }
}

Record.prototype.project = function(projecter) {
    var result;

    try {
        result = projecter(this.data);
    } catch {
        return undefined;
    }

    if (Array.isArray(result) || typeof result !== "object") {
        throw new TypeError("Projecter must return an object");
    } else {
        return result;
    }
};

Record.prototype.index = function(indexer) {
    try {
        return indexer(this.data);
    } catch {
        return undefined;
    }
};

exports.Randomizer = Randomizer;
exports.Result = Result;
exports.Reduce = Reduce;
exports.Handler = Handler;
