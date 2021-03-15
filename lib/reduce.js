const utils = require("./utils");

const getStatsReduce = (results) => {
    var reduce = {
        records: 0,
        errors: 0,
        size: 0,
        stores: 0,
        min: undefined,
        max: undefined,
        mean: undefined,
        var: undefined,
        std: undefined,
        m2: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (var i = 0; i < results.length; i++) {

        reduce.stores++;

        reduce.size += results[i].size;
        reduce.records += results[i].records;
        reduce.errors += results[i].errors.length;

        if (i === 0) {
            reduce.min = results[i].records;
            reduce.max = results[i].records;
            reduce.mean = results[i].records;
        } else {
            reduce.min = utils.min(reduce.min, results[i].records);
            reduce.max = utils.max(reduce.max, results[i].records);
            const delta1 = results[i].records - reduce.mean;
            reduce.mean += delta1 / (i + 2);
            const delta2 = results[i].records - reduce.mean;
            reduce.m2 += delta1 * delta2;
        }

        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);

    }

    reduce.size = utils.convertSize(reduce.size);
    reduce.var = reduce.m2/(results.length);
    reduce.std = Math.sqrt(reduce.m2/(results.length));
    reduce.end = Date.now();
    reduce.elapsed = reduce.end - reduce.start;

    delete reduce.m2;

    return reduce;
};

const dropReduce = (results) => {
    var reduce = {
        dropped: false,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (const result of results) {
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.dropped = true;
    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const insertReduce = (results) => {
    var reduce = {
        inserted: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (const result of results) {
        reduce.inserted += result.inserted;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const selectReduce = (results) => {
    var reduce = {
        selected: 0,
        ignored: 0,
        errors: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        data: [],
        details: results
    };

    for (const result of results) {
        reduce.data = reduce.data.concat(result.data);
        reduce.selected += result.selected;
        reduce.ignored += result.ignored;
        reduce.errors += result.errors.length;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const updateReduce = (results) => {
    var reduce = {
        lines: 0,
        updated: 0,
        unchanged: 0,
        errors: 0,
        blanks: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (const result of results) {
        reduce.lines += result.lines;
        reduce.updated += result.updated;
        reduce.unchanged += result.unchanged;
        reduce.errors += result.errors.length;
        reduce.blanks += result.blanks;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const deleteReduce = (results) => {
    var reduce = {
        deleted: 0,
        retained: 0,
        errors: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (const result of results) {
        reduce.deleted += result.deleted;
        reduce.retained += result.retained;
        reduce.errors += result.errors.length;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const aggregateReduce = (results) => {
    var reduce = {
        indexed: 0,
        unindexed: 0,
        errors: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        data: [],
        details: results
    };

    var aggregates = {};

    for (const result of results) {
        const indexes = Object.keys(result.aggregates);

        for (const index of indexes) {
            if (aggregates[index]) {
                aggregates[index].count += result.aggregates[index].count;
            } else {
                aggregates[index] = {
                    count: result.aggregates[index].count,
                    aggregates: {}
                };
            }

            const fields = Object.keys(result.aggregates[index].aggregates);

            for (const field of fields) {
                const aggregateObject = aggregates[index].aggregates[field];
                const resultObject = result.aggregates[index].aggregates[field];

                if (aggregateObject) {
                    utils.reduceAggregates(aggregateObject, resultObject);
                } else {
                    aggregates[index].aggregates[field] = {
                        min: resultObject["min"],
                        max: resultObject["max"],
                        count: resultObject["count"]
                    };

                    if (resultObject["m2"] !== undefined) {
                        aggregates[index].aggregates[field]["sum"] = resultObject["sum"];
                        aggregates[index].aggregates[field]["mean"] = resultObject["mean"];
                        aggregates[index].aggregates[field]["varp"] = resultObject["m2"] / resultObject["count"];
                        aggregates[index].aggregates[field]["vars"] = resultObject["m2"] / (resultObject["count"] - 1);
                        aggregates[index].aggregates[field]["stdp"] = Math.sqrt(resultObject["m2"] / resultObject["count"]);
                        aggregates[index].aggregates[field]["stds"] = Math.sqrt(resultObject["m2"] / (resultObject["count"] - 1));
                        aggregates[index].aggregates[field]["m2"] = resultObject["m2"];
                    }
                }
            }
        }

        reduce.indexed += result.indexed;
        reduce.unindexed += result.unindexed;
        reduce.errors += result.errors.length;

        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    for (const index of Object.keys(aggregates)) {
        var aggregate = {
            index: index,
            count: aggregates[index].count,
            aggregates: []
        };
        for (const field of Object.keys(aggregates[index].aggregates)) {
            delete aggregates[index].aggregates[field].m2;
            aggregate.aggregates.push({field: field, data: aggregates[index].aggregates[field]});
        }
        reduce.data.push(aggregate);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

exports.getStatsReduce = getStatsReduce;
exports.dropReduce = dropReduce;
exports.insertReduce = insertReduce;
exports.selectReduce = selectReduce;
exports.updateReduce = updateReduce;
exports.deleteReduce = deleteReduce;
exports.aggregateReduce = aggregateReduce;