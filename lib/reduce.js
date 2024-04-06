"use strict";

const { Result } = require("./result");

const Reduce = (type) => {
    var _reduce;

    switch (type) {
        case "stats":
            _reduce = Object.assign(Result("stats"), {
                stores: 0,
                min: undefined,
                max: undefined,
                mean: undefined,
                var: undefined,
                std: undefined,
                m2: 0
            });
            break;
        case "update":
        case "delete":
            _reduce = Result(type);
            delete _reduce.data;
            delete _reduce.records;
            break;
        case "aggregate":
            _reduce = Object.assign(Result("aggregate"), {
                data: []
            });
            break;
        default:
            _reduce = Result(type);
            break;
    }

    _reduce.details = undefined;

    return _reduce;
};

exports.Reduce = Reduce;