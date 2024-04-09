"use strict";

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

    select(selecter) {
        let result = selecter(Object.assign({}, this.data));
        const resultType = typeof result;
    
        if (resultType !== "boolean") {
            throw new TypeError("Selector must return a boolean (returned " + resultType + ")");
        } else {
            return result;
        }
    };

    update(updater) {
        let result = updater(Object.assign({}, this.data));
        const resultType = typeof result;
    
        if (resultType !== "object") {
            throw new TypeError("Updater must return an object (returned " + resultType + ")");
        } else {
            this.data = result;
            return true;
        }
    }

    project(projecter) {
        let result = projecter(Object.assign({}, this.data));
        const resultType = typeof result;
    
        if (Array.isArray(result) || typeof result !== "object") {
            throw new TypeError("Projecter must return an array or object (returned " + resultType + ")");
        } else {
            return result;
        }
    };

    index(indexer) {
        return indexer(Object.assign({}, this.data));
    };
}

exports.Record = Record;