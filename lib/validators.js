const fs = require("fs");

const validatePath = (p) => {
    if (typeof p !== "string") {
        throw new TypeError("Path must be a string");
    } else if (/^\W*$/.test(p.trim())) {
        throw new Error("Path must be a non-blank string");
    } else if (!fs.existsSync(p)) {
        throw new Error("Path does not exist");
    }

    return p;
};

const validateObject = (o) => {
    if (typeof o !== "object") {
        throw new TypeError("Not an object");
    }

    return o;
};

const validateSize = (s) => {
    if (typeof s !== "number") {
        throw new TypeError("Size must be a number");
    } else if (s <= 0) {
        throw new RangeError("Size must be greater than zero");
    }

    return s;
};

const validateArray = (a) => {
    if (!Array.isArray(a)) {
        throw new TypeError("Not an array");
    }

    return a;
};

const validateFunction = (f) => {
    if (typeof f !== "function") {
        throw new TypeError("Not a function")
    } else {
        const fString = f.toString();
        if (/\s*function/.test(fString) && !/\W+return\W+/.test(fString)) throw new Error("Function must return a value");
    }
}

exports.validatePath = validatePath;
exports.validateObject = validateObject;
exports.validateSize = validateSize;
exports.validateArray = validateArray;
exports.validateFunction = validateFunction;