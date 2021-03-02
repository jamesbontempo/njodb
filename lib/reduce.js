const fs = require("fs");
const path = require("path");
const utils = require("./utils");

const getStatsReduce = (results) => {
    var stats = {
        size: 0,
        stores: 0,
        records: 0,
        min: undefined,
        max: undefined,
        mean: undefined,
        m2: 0
    };

    var start = Date.now();
    var end = 0;

    for (var i = 0; i < results.length; i++) {

        stats.stores++;

        if (results[i].size) stats.size += results[i].size;
        if (results[i].records) stats.records += results[i].records;

        if (i === 0) {
            stats.min = results[i].records;
            stats.max = results[i].records;
            stats.mean = results[i].records;
        } else {
            if (results[i].records < stats.min) stats.min = results[i].records;
            if (results[i].records > stats.max) stats.max = results[i].records;
            const delta1 = results[i].records - stats.mean;
            stats.mean += delta1 / (i + 2);
            const delta2 = results[i].records - stats.mean;
            stats.m2 += delta1 * delta2;
        }

        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;

    }

    return (
        {
            size: utils.convertSize(stats.size),
            stores: stats.stores,
            records: stats.records,
            min: stats.min,
            max: stats.max,
            mean: stats.mean,
            var: stats.m2/(results.length),
            std: Math.sqrt(stats.m2/(results.length)),
            start: start,
            end: (end !== 0) ? end : Date.now(),
            details: results
        }
    );
};

const dropReduce = (results) => {
    var start = Date.now();
    var end = 0;

    for (var j = 0; j < results.length; j++) {
        if (results[j].start < start) start = results[j].start;
        if (results[j].end > end) end = results[j].end;
    }

    return (
        {
            dropped: true,
            start: start,
            end: (end !== 0) ? end : Date.now()
        }
    );
};

const insertReduce = (results) => {
    var inserted = 0;
    var start = Date.now();
    var end = 0;

    for (var i = 0; i < results.length; i++) {
        inserted += results[i].inserted;
        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;
    }

    return (
        {
            inserted: inserted,
            start: start,
            end: (end !== 0) ? end : Date.now(),
            details: results
        }
    );
};

const selectReduce = (results) => {
    var data = [];
    var selected = 0;
    var ignored = 0;
    var start = Date.now();
    var end = 0;

    for (var i = 0; i < results.length; i++) {
        data = data.concat(results[i].data);
        selected += results[i].selected;
        ignored += results[i].ignored;
        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;
    }

    return (
        {
            data: data,
            selected: selected,
            ignored: ignored,
            start: start,
            end: (end !== 0) ? end : Date.now(),
            details: results
        }
    );
};

const updateReduce = (results) => {
    var updated = 0;
    var unchanged = 0;
    var start = Date.now();
    var end = 0;

    for (var i = 0; i < results.length; i++) {
        updated += results[i].updated;
        unchanged += results[i].unchanged;
        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;
    }

    return (
        {
            updated: updated,
            unchanged: unchanged,
            start: start,
            end: (end !== 0) ? end : Date.now(),
            details: results
        }
    );
};

const deleteReduce = (results) => {
    var deleted = 0;
    var retained = 0;
    var start = Date.now();
    var end = 0;

    for (var i = 0; i < results.length; i++) {
        deleted += results[i].deleted;
        retained += results[i].retained;
        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;
    }

    return (
        {
            deleted: deleted,
            retained: retained,
            start: start,
            end: (end !== 0) ? end : Date.now(),
            details: results
        }
    );
};

const aggregateReduce = (results) => {
    var aggregates = {};
    var start = Date.now();
    var end = 0;

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

        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;
    }

    return (
        {
            data: Object.keys(aggregates).map(index => {return {
                index: index,
                aggregates: Object.keys(aggregates[index]).map(field => {delete aggregates[index][field].m2; return {
                    field: field,
                    data: aggregates[index][field]}; })}; }),
            start: start,
            end: (end !== 0) ? end : Date.now(),
            details: results
        }
    );
};

const indexReduce = (field, results, datapath) => {
    var index = {};
    var start = Date.now();
    var end = 0;

    for (var i = 0; i < results.length; i++) {
        const keys = Object.keys(results[i].index).sort((a, b) => {if (a < b) return -1; if (a > b) return 1; return 0; });

        for (var j = 0; j < keys.length; j++) {
            if (index[keys[j]]) {
                index[keys[j]].push(
                    {
                        store: results[i].store,
                        lines: results[i].index[keys[j]]
                    }
                );
            } else {
                index[keys[j]] = [
                    {
                        store: results[i].store,
                        lines: results[i].index[keys[j]]
                    }
                ];
            }
        }

        if (results[i].start < start) start = results[i].start;
        if (results[i].end > end) end = results[i].end;
    }

    const indexpath = path.join(datapath, ["index", field, "json"].join("."));

    fs.writeFile(indexpath, JSON.stringify(index), (error) => {
        if (error) throw error;
        return (
            {
                field: field,
                path: indexpath,
                size: Object.keys(index).length,
                start: start,
                end: (end !== 0) ? end : Date.now(),
                details: results
            }
        );
    });
};

exports.getStatsReduce = getStatsReduce;
exports.dropReduce = dropReduce;
exports.insertReduce = insertReduce;
exports.selectReduce = selectReduce;
exports.updateReduce = updateReduce;
exports.deleteReduce = deleteReduce;
exports.aggregateReduce = aggregateReduce;
exports.indexReduce = indexReduce;