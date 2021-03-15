const objects = require("./objects");

class statsHandler {
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

class selectHandler {
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

class updateHandler {
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

class deleteHandler {
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
                this.results.errors.push({line: this.results.retained + 1, data: record});

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

exports.statsHandler = statsHandler;
exports.selectHandler = selectHandler;
exports.updateHandler = updateHandler;
exports.deleteHandler = deleteHandler;