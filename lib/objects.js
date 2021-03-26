"use strict";

const Result = (type) => {
    var result;

    switch (type) {
        case "stats":
            result = {
                size: 0,
                lines: 0,
                records: 0,
                errors: [],
                blanks: 0,
                created: undefined,
                modified: undefined,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
        case "distribute":
            result = {
                stores: undefined,
                records: 0,
                errors: [],
                start: Date.now(),
                end: undefined,
                elapsed: undefined
            };
            break;
        case "insert":
            result = {
                inserted: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
        case "select":
            result = {
                lines: 0,
                selected: 0,
                ignored: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: [],
            };
            break;
        case "update":
            result = {
                lines: 0,
                updated: 0,
                unchanged: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: []
            };
            break;
        case "delete":
            result = {
                lines: 0,
                deleted: 0,
                retained: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: []
            };
            break;
        case "aggregate":
            result = {
                lines: 0,
                aggregates: {},
                indexed: 0,
                unindexed: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
    }

    return result;
}

const Reduce = (type) => {
    var reduce;

    switch (type) {
        case "stats":
            reduce = Object.assign(Result("stats"), {
                errors: 0,
                stores: 0,
                min: undefined,
                max: undefined,
                mean: undefined,
                var: undefined,
                std: undefined,
                m2: 0
            });
            break;
        case "drop":
            reduce = {
                dropped: false,
                start: Date.now(),
                end: 0,
                elapsed: 0
            };
            break;
        case "insert":
            reduce = Result("insert");
            break;
        case "select":
            reduce = Object.assign(Result("select"), {
                errors: 0
            });
            break;
        case "update":
            reduce = Object.assign(Result("update"), {
                errors: 0
            });
            break;
        case "delete":
            reduce = Object.assign(Result("delete"), {
                errors: 0
            });
            break;
        case "aggregate":
            reduce = Object.assign(Result("aggregate"), {
                errors: 0,
                data: []
            });
            break;
    }

    reduce.details = undefined;

    return reduce;
};

exports.Result = Result;
exports.Reduce = Reduce;
