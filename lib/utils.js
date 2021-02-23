const checkValue = (name, value, exists, type) => {
    if (exists && !value) throw new Error(name + " must be defined");

    if (value && type) {
        if (type === "array") {
            if (!Array.isArray(value)) throw new Error(name + " must be an array");
        } else {
            if (typeof value !== type) throw new Error(name + " must be a " + type);
        }
    }
};

const convertSize = (size) => {
    if (size < 1024) return size + " bytes";
    if (size < Math.pow(1024, 2)) return Math.round(((size / 1024) + Number.EPSILON) * 100) / 100 + " KB";
    if (size < Math.pow(1024, 3)) return Math.round(((size / Math.pow(1024, 2)) + Number.EPSILON) * 100) / 100 + " MB";
    return Math.round(((size / Math.pow(1024, 3)) + Number.EPSILON) * 100) / 100 + " GB";
};

exports.checkValue = checkValue;
exports.convertSize = convertSize;