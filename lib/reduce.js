"use strict";

const utils = require("./utils");
const objects = require("./objects");

const statsReduce = (results) => {
    var reduce = objects.Reduce("stats");

    var i = 0;

    for (const result of results) {
        reduce.stores++;

        reduce.lines += result.lines;
        reduce.size += result.size;
        reduce.records += result.records;
        reduce.errors += result.errors.length;
        reduce.blanks += result.blanks;

        reduce.min = utils.min(reduce.min, result.records);
        reduce.max = utils.max(reduce.max, result.records);

        if (reduce.mean === undefined) reduce.mean = result.records;

        const delta1 = result.records - reduce.mean;
        reduce.mean += delta1 / (i + 2);
        const delta2 = result.records - reduce.mean;
        reduce.m2 += delta1 * delta2;

        reduce.created = utils.min(reduce.created, result.created);
        reduce.modified = utils.max(reduce.modified, result.modified);

        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);

        i++;
    }

    reduce.size = utils.convertSize(reduce.size);

    reduce.var = reduce.m2/(results.length);
    reduce.std = Math.sqrt(reduce.m2/(results.length));

    reduce.details = results;

    reduce.end = Date.now();
    reduce.elapsed = reduce.end - reduce.start;

    delete reduce.m2;

    return reduce;
};

const dropReduce = (results) => {
    var reduce = objects.Reduce("drop");

    for (const result of results) {
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.dropped = true;
    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const insertReduce = (results) => {
    var reduce = objects.Reduce("insert");

    for (const result of results) {
        reduce.inserted += result.inserted;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.details = results;
    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const selectReduce = (results) => {
    var reduce = objects.Reduce("select");

    for (const result of results) {
        reduce.lines += result.lines;
        reduce.selected += result.selected;
        reduce.ignored += result.ignored;
        reduce.errors += result.errors.length;
        reduce.blanks += result.blanks;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
        reduce.data = reduce.data.concat(result.data);
        delete result.data;
    }

    reduce.details = results;
    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const updateReduce = (results) => {
    var reduce = objects.Reduce("update");

    for (const result of results) {
        reduce.lines += result.lines;
        reduce.updated += result.updated;
        reduce.unchanged += result.unchanged;
        reduce.errors += result.errors.length;
        reduce.blanks += result.blanks;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.details = results;
    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const deleteReduce = (results) => {
    var reduce = objects.Reduce("delete");

    for (const result of results) {
        reduce.lines += result.lines;
        reduce.deleted += result.deleted;
        reduce.retained += result.retained;
        reduce.errors += result.errors.length;
        reduce.blanks += result.blanks;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);
    }

    reduce.details = results;
    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const aggregateReduce = (results) => {
    var reduce = objects.Reduce("aggregate");

    var aggregates = {};

    for (const result of results) {
        reduce.lines += result.lines;
        reduce.indexed += result.indexed;
        reduce.unindexed += result.unindexed;
        reduce.errors += result.errors.length;
        reduce.blanks += result.blanks;
        reduce.start = utils.min(reduce.start, result.start);
        reduce.end = utils.max(reduce.end, result.end);

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
                    reduceAggregate(aggregateObject, resultObject);
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

        delete result.aggregates;
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

    reduce.details = results;
    reduce.elapsed = reduce.end - reduce.start;
    delete reduce.aggregates;

    return reduce;
};

const reduceAggregate = (aggregate, result) => {
    const n = aggregate["count"] + result["count"];

    aggregate["min"] = utils.min(aggregate["min"], result["min"]);
    aggregate["max"] = utils.max(aggregate["max"], result["max"]);

    // Parallel version of Welford's algorithm
    if (result["m2"] !== undefined) {
        const delta = result["mean"] - aggregate["mean"];
        const m2 = aggregate["m2"] + result["m2"] + (Math.pow(delta, 2) * ((aggregate["count"] * result["count"]) / n));
        aggregate["m2"] = m2;
        aggregate["varp"] = m2 / n;
        aggregate["vars"] = m2 / (n - 1);
        aggregate["stdp"] = Math.sqrt(m2 / n);
        aggregate["stds"] = Math.sqrt(m2 / (n - 1));
    }

    if (result["sum"] !== undefined) {
        aggregate["mean"] = (aggregate["sum"] + result["sum"]) / n;
        aggregate["sum"] += result["sum"];
    }

    aggregate["count"] = n;

    return aggregate;
};

exports.statsReduce = statsReduce;
exports.dropReduce = dropReduce;
exports.insertReduce = insertReduce;
exports.selectReduce = selectReduce;
exports.updateReduce = updateReduce;
exports.deleteReduce = deleteReduce;
exports.aggregateReduce = aggregateReduce;