const { Reduce } = require("./reduce");

const {
    convertSize,
    max,
    min
} = require("./utils");

const Reducer = (type, results) => {
    var _reduce = Reduce(type);

    var i = 0;
    var aggregates = {};

    for (const result of results) {
        switch(type) {
            case "stats":
                statsReducer(_reduce, result, i);
                break;
            case "insert":
                insertReducer(_reduce, result);
                break;
            case "select":
                selectReducer(_reduce, result);
                break;
            case "update":
                updateReducer(_reduce, result);
                break;
            case "delete":
                deleteReducer(_reduce, result);
                break;
            case "aggregate":
                aggregateReducer(_reduce, result, aggregates);
                break
        }

        if (type === "stats") {
            _reduce.stores++;
            i++;
        }

        if (type === "drop") {
            _reduce.dropped = true;
            _reduce.end = Date.now();
        } else if (type !== "insert") {
            _reduce.lines += result.lines;
            _reduce.errors = _reduce.errors.concat(result.errors);
            _reduce.blanks += result.blanks;
        }

        _reduce.start = min(_reduce.start, result.start);
        _reduce.end = max(_reduce.end, result.end);
    }

    if (type === "stats") {
        _reduce.size = convertSize(_reduce.size);
        _reduce.var = _reduce.m2/(results.length);
        _reduce.std = Math.sqrt(_reduce.m2/(results.length));
        delete _reduce.m2;
    } else if (type === "aggregate") {
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
            _reduce.data.push(aggregate);
        }
        delete _reduce.aggregates;
    }

    _reduce.elapsed = _reduce.end - _reduce.start;
    _reduce.details = results;

    return _reduce;
};

const statsReducer = (reduce, result, i) => {
    reduce.size += result.size;
    reduce.records += result.records;
    reduce.min = min(reduce.min, result.records);
    reduce.max = max(reduce.max, result.records);
    if (reduce.mean === undefined) reduce.mean = result.records;
    const delta1 = result.records - reduce.mean;
    reduce.mean += delta1 / (i + 2);
    const delta2 = result.records - reduce.mean;
    reduce.m2 += delta1 * delta2;
    reduce.created = min(reduce.created, result.created);
    reduce.modified = max(reduce.modified, result.modified);
};

const insertReducer = (reduce, result) => {
    reduce.inserted += result.inserted;
};

const selectReducer = (reduce, result) => {
    reduce.selected += result.selected;
    reduce.ignored += result.ignored;
    reduce.data = reduce.data.concat(result.data);
    delete result.data;
};

const updateReducer = (reduce, result) => {
    reduce.selected += result.selected;
    reduce.updated += result.updated;
    reduce.unchanged += result.unchanged;
};

const deleteReducer = (reduce, result) => {
    reduce.selected += result.selected;
    reduce.deleted += result.deleted;
    reduce.retained += result.retained;
};

const aggregateReducer = (reduce, result, aggregates) => {
    reduce.indexed += result.indexed;
    reduce.unindexed += result.unindexed;

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
};

const reduceAggregate = (aggregate, result) => {
    const n = aggregate["count"] + result["count"];

    aggregate["min"] = min(aggregate["min"], result["min"]);
    aggregate["max"] = max(aggregate["max"], result["max"]);

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
};

exports.Reducer = Reducer;