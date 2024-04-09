"use strict";

class Sleeper {
    constructor(time = 100) {
        this.time = time;
    }

    async sleep() {
        return new Promise((resolve) => {
            setTimeout(resolve, this.time);
        });
    }

    sleepSync() {
        const start = new Date().getTime();
        let done = false;
        while (!done) {
            if (new Date().getTime() >= start + this.time) done = true;
        }
    }
}

exports.Sleeper = Sleeper;