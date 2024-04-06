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
    let result = selecter(this.data);

    if (typeof result !== "boolean") {
        throw new TypeError("Selecter must return a boolean");
    } else {
        return result;
    }
};

Record.prototype.update = function(updater) {
    let result = updater(this.data);

    if (typeof result !== "object") {
        throw new TypeError("Updater must return an object");
    } else {
        this.data = result;
        return true;
    }
}

Record.prototype.project = function(projecter) {
    let result = projecter(this.data);

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

exports.Record = Record;