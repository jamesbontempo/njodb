const Randomizer = (data, replacement) => {
    var mutable = [...data];
    if (replacement === undefined || typeof replacement !== "boolean") replacement = true;

    function _next() {
        var selection;
        const index = Math.floor(Math.random()*mutable.length);

        if (replacement) {
            selection = mutable.slice(index, index + 1)[0];
        } else {
            selection = mutable.splice(index, 1)[0];
            if (mutable.length === 0) mutable = [...data];
        }

        return selection;
    }

    return {
        next: _next
    };
};

exports.Randomizer = Randomizer;