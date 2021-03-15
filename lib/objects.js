const path = require("path");

const statsResults = () => {
    return ({
        size: 0,
        lines: 0,
        records: 0,
        errors: [],
        blanks: 0,
        created: undefined,
        modified: undefined,
        start: Date.now(),
        end: undefined
    });
};

const distributeResults = () => {
    return ({
        stores: undefined,
        records: 0,
        errors: [],
        start: Date.now(),
        end: undefined,
        elapsed: undefined
    });
};

const insertResults = () => {
    return ({
        inserted: 0,
        start: Date.now(),
        end: undefined
    });
};

const selectResults = () => {
    return ({
        lines: 0,
        selected: 0,
        ignored: 0,
        errors: [],
        blanks: 0,
        start: Date.now(),
        end: undefined,
        data: [],
    });
};

const aggregateResults = (store) => {
    return ({
        store: path.resolve(store),
        aggregates: {},
        indexed: 0,
        unindexed: 0,
        errors: [],
        start: Date.now(),
        end: undefined
    });
};

const updateResults = () => {
    return ({
        lines: 0,
        updated: 0,
        unchanged: 0,
        errors: [],
        blanks: 0,
        start: Date.now(),
        end: undefined,
        data: []
    });
};

const deleteResults = () => {
    return ({
        lines: 0,
        deleted: 0,
        retained: 0,
        errors: [],
        blanks: 0,
        start: Date.now(),
        end: undefined,
        data: []
    });
}

exports.statsResults = statsResults;
exports.distributeResults = distributeResults;
exports.insertResults = insertResults;
exports.selectResults = selectResults;
exports.aggregateResults = aggregateResults;
exports.updateResults = updateResults;
exports.deleteResults = deleteResults;