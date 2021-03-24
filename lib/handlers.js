"use strict";

const objects = require("./objects");
const utils = require("./utils");

class StatsHandler {
    constructor() {
        this.results = objects.statsResults();
        return this;
    }

    next(record) {
        record = new Record(record);

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            if (record.data) {
                this.results.records++;
            } else {
                this.results.errors.push({error: record.error, line: this.results.lines, data: record.source});
            }
        }

        return this.results;
    }

    return() {
        return this.results;
    }
}

class SelectHandler {
    constructor(selecter, projecter) {
        this.selecter = selecter;
        this.projecter = projecter;
        this.results = objects.selectResults();
        return this;
    }

    next(record) {
        record = new Record(record);

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            if (record.data) {
                if (record.select(this.selecter)) {
                    if (this.projecter) {
                        this.results.data.push(record.project(this.projecter));
                    } else {
                        this.results.data.push(record.data);
                    }
                    this.results.selected++;
                } else {
                    this.results.ignored++;
                }
            } else {
                this.results.errors.push({error: record.error, line: this.results.lines, data: record.source});
            }
        }
    }

    return() {
        this.results.end = Date.now();
        return this.results;
    }
}

class UpdateHandler {
    constructor(selecter, updater) {
        this.selecter = selecter;
        this.updater = updater;
        this.results = objects.updateResults();
        return this;
    }

    next(record, writer) {
        record = new Record(record);

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            if (record.data) {
                if (record.select(this.selecter)) {
                    if (record.update(this.updater)) {
                        this.results.updated++;
                    } else {
                        this.results.unchanged++;
                    }
                } else {
                    this.results.unchanged++;
                }

                if (writer) {
                    writer.write(JSON.stringify(record.data) + "\n");
                } else {
                    this.results.data.push(JSON.stringify(record.data));
                }
            } else {
                this.results.errors.push({error: record.error, line: this.results.lines, data: record.source});

                if (writer) {
                    writer.write(record.source) + "\n";
                } else {
                    this.results.data.push(record.source);
                }
            }
        }
    }

    return() {
        this.results.end = Date.now();
        return this.results;
    }
}

class DeleteHandler {
    constructor(selecter) {
        this.selecter = selecter;
        this.results = objects.deleteResults();
        return this;
    }

    next(record, writer) {
        record = new Record(record);

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            if (record.data) {
                if (record.select(this.selecter)) {
                    this.results.deleted++;
                } else {
                    this.results.retained++;

                    if (writer) {
                        writer.write(JSON.stringify(record.data) + "\n");
                    } else {
                        this.results.data.push(JSON.stringify(record.data));
                    }
                }
            } else {
                this.results.errors.push({error: record.error, line: this.results.lines, data: record.source});

                if (writer) {
                    writer.write(record.source) + "\n";
                } else {
                    this.results.data.push(record.source);
                }
            }
        }
    }

    return() {
        this.results.end = Date.now();
        return this.results;
    }
}

class AggregateHandler {
    constructor(selecter, indexer, projecter) {
        this.selecter = selecter;
        this.indexer = indexer;
        this.projecter = projecter;
        this.results = objects.aggregateResults();
    }

    next(record) {
        record = new Record(record);

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            if (record.data) {
                if (this.selecter(record)) {
                    const index = record.index(this.indexer);

                    if (!index) {
                        this.results.unindexed++;
                    } else {
                        var projection;
                        var fields;

                        if (this.results.aggregates[index]) {
                            this.results.aggregates[index].count++;
                        } else {
                            this.results.aggregates[index] = {
                                count: 1,
                                aggregates: {}
                            };
                        }

                        if (this.projecter) {
                            projection = record.project(this.projecter);
                            fields = Object.keys(projection);
                        } else {
                            projection = record.data;
                            fields = Object.keys(record.data);
                        }

                        for (const field of fields) {
                            if (projection[field] !== undefined) {
                                if (this.results.aggregates[index].aggregates[field]) {
                                    utils.accumulateAggregate(this.results.aggregates[index].aggregates[field], projection[field]);
                                } else {
                                    this.results.aggregates[index].aggregates[field] = {
                                        min: projection[field],
                                        max: projection[field],
                                        count: 1
                                    };
                                    if (typeof projection[field] === "number") {
                                        this.results.aggregates[index].aggregates[field]["sum"] = projection[field];
                                        this.results.aggregates[index].aggregates[field]["mean"] = projection[field];
                                        this.results.aggregates[index].aggregates[field]["m2"] = 0;
                                    }
                                }
                            }
                        }

                        this.results.indexed++;
                    }
                }
            } else {
                this.results.errors.push({error: record.error, line: this.results.lines, data: record.source});
            }
        }
    }

    return() {
        this.results.end = Date.now();
        return this.results;
    }
}

class Record {
    constructor(record) {
        this.source = record.trim();
        this.length = this.source.length

        try {
            this.data = JSON.parse(this.source)
        } catch(error) {
            this.data = undefined;
            this.error = error.message;
        }
    }

    select(selecter) {
        try {
            const result = selecter(this.data);
            if (typeof result !== "boolean") {
                throw new TypeError("Selecter must return a boolean");
            } else {
                return result;
            }
        } catch {
            return false;
        }
    }

    update(updater) {
        try {
            const result = updater(this.data);
            if (typeof result !== "object") {
                throw new TypeError("Updater must return an object");
            } else {
                this.data = result;
                return true;
            }
        } catch {
            return false;
        }
    }

    project(projecter) {
        try {
            const result = projecter(this.data);
            if (typeof result !== "object") {
                throw new TypeError("Projecter must return an object");
            } else {
                return result;
            }
        } catch {
            return undefined;
        }
    }

    index(indexer) {
        try {
            return indexer(this.data);
        } catch {
            return undefined;
        }
    }
}

exports.StatsHandler = StatsHandler;
exports.SelectHandler = SelectHandler;
exports.UpdateHandler = UpdateHandler;
exports.DeleteHandler = DeleteHandler;
exports.AggregateHandler = AggregateHandler;