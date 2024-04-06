"use strict";

const Result = (type) => {
    var _result;

    switch (type) {
        case "stats":
            _result = {
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
        case "drop":
            _result = {
                dropped: false,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            }
            break;
        case "distribute":
            _result = {
                stores: undefined,
                records: 0,
                errors: [],
                start: Date.now(),
                end: undefined,
                elapsed: undefined
            };
            break;
        case "insert":
            _result = {
                inserted: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0
            };
            break;
        case "insertFile":
            _result = {
                lines: 0,
                inserted: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined
            };
            break;
        case "select":
            _result = {
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
            _result = {
                lines: 0,
                selected: 0,
                updated: 0,
                unchanged: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: [],
                records: []
            };
            break;
        case "delete":
            _result = {
                lines: 0,
                selected: 0,
                deleted: 0,
                retained: 0,
                errors: [],
                blanks: 0,
                start: Date.now(),
                end: undefined,
                elapsed: 0,
                data: [],
                records: []
            };
            break;
        case "aggregate":
            _result = {
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

    return _result;
}

exports.Result = Result;