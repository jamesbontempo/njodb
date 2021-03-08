const fs = require("fs");
const path = require("path");
const utils = require("./utils");

const getStatsReduce = (results) => {
    var reduce = {
        size: 0,
        stores: 0,
        records: 0,
        errors: 0,
        min: undefined,
        max: undefined,
        mean: undefined,
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

    for (var i = 0; i < results.length; i++) {
        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);
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

    for (var i = 0; i < results.length; i++) {
        reduce.inserted += results[i].inserted;
        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const selectReduce = (results) => {
    var reduce = {
        data: [],
        selected: 0,
        ignored: 0,
        errors: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (var i = 0; i < results.length; i++) {
        reduce.data = reduce.data.concat(results[i].data);
        reduce.selected += results[i].selected;
        reduce.ignored += results[i].ignored;
        reduce.errors += results[i].errors.length;
        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const updateReduce = (results) => {
    var reduce = {
        updated: 0,
        unchanged: 0,
        errors: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    for (var i = 0; i < results.length; i++) {
        reduce.updated += results[i].updated;
        reduce.unchanged += results[i].unchanged;
        reduce.errors += results[i].errors.length;
        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);
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

    for (var i = 0; i < results.length; i++) {
        reduce.deleted += results[i].deleted;
        reduce.retained += results[i].retained;
        reduce.errors += results[i].errors.length;
        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);
    }

    reduce.elapsed = reduce.end - reduce.start;

    return reduce;
};

const aggregateReduce = (results) => {
    var reduce = {
        data: [],
        indexed: 0,
        unindexed: 0,
        errors: 0,
        start: Date.now(),
        end: 0,
        elapsed: 0,
        details: results
    };

    var aggregates = {};

    for (var i = 0; i < results.length; i++) {
        const indexes = Object.keys(results[i].aggregates);

        for (var j = 0; j < indexes.length; j++) {
            const fields = Object.keys(results[i].aggregates[indexes[j]]);
            if (!aggregates[indexes[j]]) aggregates[indexes[j]] = {};

            for (var k = 0; k < fields.length; k++) {
                const aggregateObject = aggregates[indexes[j]][fields[k]];
                const resultObject = results[i].aggregates[indexes[j]][fields[k]];

                if (aggregateObject) {
                    if (resultObject["min"] < aggregateObject["min"]) aggregateObject["min"] = resultObject["min"];
                    if (resultObject["max"] > aggregateObject["max"]) aggregateObject["max"] = resultObject["max"];

                    if (resultObject["m2"] !== undefined) {
                        const n = aggregateObject["count"] + resultObject["count"];
                        const delta = resultObject["mean"] - aggregateObject["mean"];
                        const m2 = aggregateObject["m2"] + resultObject["m2"] + (Math.pow(delta, 2) * ((aggregateObject["count"] * resultObject["count"]) / n));
                        aggregateObject["m2"] = m2;
                        aggregateObject["varp"] = m2 / n;
                        aggregateObject["vars"] = m2 / (n - 1);
                        aggregateObject["stdp"] = Math.sqrt(m2 / n);
                        aggregateObject["stds"] = Math.sqrt(m2 / (n - 1));
                    }

                    if (resultObject["sum"] !== undefined) {
                        aggregateObject["mean"] = (aggregateObject["sum"] + resultObject["sum"]) / ( aggregateObject["count"] + resultObject["count"]);
                        aggregateObject["sum"] += resultObject["sum"];
                    }

                    aggregateObject["count"] += resultObject["count"];
                } else {
                    aggregates[indexes[j]][fields[k]] = {
                        min: resultObject["min"],
                        max: resultObject["max"],
                        count: resultObject["count"],
                        sum: resultObject["sum"],
                        mean: resultObject["mean"],
                        varp: undefined,
                        vars: undefined,
                        stdp: undefined,
                        stds: undefined,
                        m2: undefined
                    };

                    if (resultObject["m2"] !== undefined) {
                        aggregates[indexes[j]][fields[k]]["varp"] = resultObject["m2"] / resultObject["count"];
                        aggregates[indexes[j]][fields[k]]["vars"] = resultObject["m2"] / (resultObject["count"] - 1);
                        aggregates[indexes[j]][fields[k]]["stdp"] = Math.sqrt(resultObject["m2"] / resultObject["count"]);
                        aggregates[indexes[j]][fields[k]]["stds"] = Math.sqrt(resultObject["m2"] / (resultObject["count"] - 1));
                        aggregates[indexes[j]][fields[k]]["m2"] = resultObject["m2"];
                    }
                }
            }
        }

        reduce.indexed += results[i].indexed;
        reduce.unindexed += results[i].unindexed;
        reduce.errors += results[i].errors.length;

        reduce.start = utils.min(reduce.start, results[i].start);
        reduce.end = utils.max(reduce.end, results[i].end);
    }

    reduce.data = Object.keys(aggregates).map(index => {return {
        index: index,
        aggregates: Object.keys(aggregates[index]).map(field => {delete aggregates[index][field].m2; return {
            field: field,
            data: aggregates[index][field]}; })}; });

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