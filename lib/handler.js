const { Result } = require("./result");

const { Record } = require("./record");

const {
    min,
    max
} = require("./utils")

const Handler = (type, ...functions) => {
    var _results = Result(type);

    const _next = (record, writer) => {
        record = new Record(record);
        _results.lines++;

        if (record.length === 0) {
            _results.blanks++;
        } else {
            if (record.data) {
                switch (type) {
                    case "stats":
                        statsHandler(record, _results);
                        break;
                    case "select":
                        selectHandler(record, functions[0], functions[1], _results);
                        break;
                    case "update":
                        updateHandler(record, functions[0], functions[1], writer, _results);
                        break;
                    case "delete":
                        deleteHandler(record, functions[0], writer, _results);
                        break;
                    case "aggregate":
                        aggregateHandler(record, functions[0], functions[1], functions[2], _results);
                        break;
                }
            } else {
                _results.errors.push({error: record.error, line: _results.lines, data: record.source});

                if (type === "update" || type === "delete") {
                    if (writer) {
                        writer.write(record.source + "\n");
                    } else {
                        _results.data.push(record.source);
                    }
                }
            }
        }
    };

    const _return = () => {
        _results.end = Date.now();
        _results.elapsed = _results.end - _results.start;
        return _results;
    }

    return {
        next: _next,
        return: _return
    };
};

const statsHandler = (record, results) => {
    results.records++;
    return results;
};

const selectHandler = (record, selecter, projecter, results) => {
    if (record.select(selecter)) {
        results.selected++;
        if (projecter) {
            results.data.push(record.project(projecter));
        } else {
            results.data.push(record.data);
        }
    } else {
        results.ignored++;
    }
};

const updateHandler = (record, selecter, updater, writer, results) => {
    if (record.select(selecter)) {
        results.selected++;
        if (record.update(updater)) {
            results.updated++;
        } else {
            results.unchanged++;
        }
    } else {
        results.unchanged++;
    }

    if (writer) {
        writer.write(JSON.stringify(record.data) + "\n");
    } else {
        results.data.push(JSON.stringify(record.data));
    }
};

const deleteHandler = (record, selecter, writer, results) => {
    if (record.select(selecter)) {
        results.selected++;
        results.deleted++;
    } else {
        results.retained++;

        if (writer) {
            writer.write(JSON.stringify(record.data) + "\n");
        } else {
            results.data.push(JSON.stringify(record.data));
        }
    }
};

const aggregateHandler = (record, selecter, indexer, projecter, results) => {
    if (record.select(selecter)) {
        const index = record.index(indexer);

        if (!index) {
            results.unindexed++;
        } else {
            var projection;
            var fields;

            if (results.aggregates[index]) {
                results.aggregates[index].count++;
            } else {
                results.aggregates[index] = {
                    count: 1,
                    aggregates: {}
                };
            }

            if (projecter) {
                projection = record.project(projecter);
                fields = Object.keys(projection);
            } else {
                projection = record.data;
                fields = Object.keys(record.data);
            }

            for (const field of fields) {
                if (projection[field] !== undefined) {
                    if (results.aggregates[index].aggregates[field]) {
                        accumulateAggregate(results.aggregates[index].aggregates[field], projection[field]);
                    } else {
                        results.aggregates[index].aggregates[field] = {
                            min: projection[field],
                            max: projection[field],
                            count: 1
                        };
                        if (typeof projection[field] === "number") {
                            results.aggregates[index].aggregates[field]["sum"] = projection[field];
                            results.aggregates[index].aggregates[field]["mean"] = projection[field];
                            results.aggregates[index].aggregates[field]["m2"] = 0;
                        }
                    }
                }
            }

            results.indexed++;
        }
    }
}

const accumulateAggregate = (index, projection) => {
    index["min"] = min(index["min"], projection);
    index["max"] = max(index["max"], projection);
    index["count"]++;

    // Welford's algorithm
    if (typeof projection === "number") {
        const delta1 = projection - index["mean"];
        index["sum"] += projection;
        index["mean"] += delta1 / index["count"];
        const delta2 = projection - index["mean"];
        index["m2"] += delta1 * delta2;
    }

    return index;
};

exports.Handler = Handler;