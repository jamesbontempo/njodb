const objects = require("./objects");
const utils = require("./utils");

class StatsHandler {
    constructor() {
        this.results = objects.statsResults();
        return this;
    }

    next(record) {
        record = record.trim();

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            try {
                JSON.parse(record);
                this.results.records++;
            } catch {
                this.results.errors.push({line: this.results.lines, data: record});
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
        record = record.trim();

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            try {
                record = JSON.parse(record);
                if (this.selecter(record)) {
                    if (this.projecter) {
                        this.results.data.push(this.projecter(record));
                    } else {
                        this.results.data.push(record);
                    }
                    this.results.selected++;
                } else {
                    this.results.ignored++;
                }
            } catch (error) {
                this.results.errors.push({line: this.results.lines, data: record});
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
        record = record.trim();

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            try {
                record = JSON.parse(record);

                if (this.selecter(record)) {
                    record = this.updater(record)
                    this.results.updated++;
                } else {
                    this.results.unchanged++;
                }

                if (writer) {
                    writer.write(JSON.stringify(record) + "\n");
                } else {
                    this.results.data.push(JSON.stringify(record));
                }
            } catch (error) {
                this.results.errors.push({line: this.results.lines, data: record});

                if (writer) {
                    writer.write(record) + "\n";
                } else {
                    this.results.data.push(record);
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
        record = record.trim();

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            try {
                record = JSON.parse(record);
                if (this.selecter(record)) {
                    this.results.deleted++;
                } else {
                    this.results.retained++;

                    if (writer) {
                        writer.write(JSON.stringify(record) + "\n");
                    } else {
                        this.results.data.push(JSON.stringify(record));
                    }
                }
            } catch (error) {
                this.results.errors.push({line: this.results.lines, data: record});

                if (writer) {
                    writer.write(record) + "\n";
                } else {
                    this.results.data.push(record);
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
        record = record.trim();

        this.results.lines++;

        if (record.length === 0) {
            this.results.blanks++;
        } else {
            try {
                record = JSON.parse(record);

                if (this.selecter(record)) {
                    const index = this.indexer(record);

                    if (index === undefined) {
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
                            projection = this.projecter(record);
                            fields = Object.keys(projection);
                        } else {
                            projection = record;
                            fields = Object.keys(record);
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
            } catch (error) {
                this.results.errors.push({line: this.results.lines, data: record});
            }
        }
    }

    return() {
        this.results.end = Date.now();
        return this.results;
    }
}

exports.StatsHandler = StatsHandler;
exports.SelectHandler = SelectHandler;
exports.UpdateHandler = UpdateHandler;
exports.DeleteHandler = DeleteHandler;
exports.AggregateHandler = AggregateHandler;