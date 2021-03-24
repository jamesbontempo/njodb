"use strict";

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

const statsReduce = () => {
    return (Object.assign(statsResults(),
        {
            errors: 0,
            stores: 0,
            min: undefined,
            max: undefined,
            mean: undefined,
            var: undefined,
            std: undefined,
            m2: 0,
            elapsed: 0,
            details: undefined
        }
    ));
}

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

const dropReduce = () => {
    return ({
        dropped: false,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: undefined
    });
}

const insertResults = () => {
    return ({
        inserted: 0,
        start: Date.now(),
        end: undefined
    });
};

const insertReduce = () => {
    return (Object.assign(insertResults(), {
        elapsed: 0,
        details: undefined
    }));
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

const selectReduce = () => {
    return ({
        lines: 0,
        selected: 0,
        ignored: 0,
        errors: 0,
        blanks: 0,
        start: Date.now(),
        end: undefined,
        elapsed: 0,
        data: [],
        details: undefined
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

const updateReduce = () => {
    return (Object.assign(updateResults(), {
        errors: 0,
        elapsed: 0,
        details: undefined
    }));
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
};

const deleteReduce = () => {
    return (Object.assign(deleteResults(), {
        errors: 0,
        elapsed: 0,
        details: undefined
    }));
}

const aggregateResults = () => {
    return ({
        lines: 0,
        aggregates: {},
        indexed: 0,
        unindexed: 0,
        errors: [],
        blanks: 0,
        start: Date.now(),
        end: undefined
    });
};

const aggregateReduce = () => {
    return (Object.assign(aggregateResults(), {
        errors: 0,
        elapsed: 0,
        data: [],
        details: undefined
    }));
}

exports.statsResults = statsResults;
exports.statsReduce = statsReduce;

exports.distributeResults = distributeResults;

exports.dropReduce = dropReduce;

exports.insertResults = insertResults;
exports.insertReduce = insertReduce;

exports.selectResults = selectResults;
exports.selectReduce = selectReduce;

exports.updateResults = updateResults;
exports.updateReduce = updateReduce;

exports.deleteResults = deleteResults;
exports.deleteReduce = deleteReduce;

exports.aggregateResults = aggregateResults;
exports.aggregateReduce = aggregateReduce;