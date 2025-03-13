"use strict";

const chain = require("chain");

class MessageQueue {
    constructor(levelDb) {
        this.levelDb = levelDb;
        this.index = this._getMaxIndex() + BigInt(1);
    }

    _getMaxIndex() {
        let maxIndex = BigInt(0);
        try {
            let key = this.levelDb.lastKey();
            if (key) {
                maxIndex = key.readBigUInt64BE();
            }
        } catch (e) {
            console.error("Failed to get max index:", e);
        }
        return maxIndex;
    }

    encodeInt64(num) {
        const buf = Buffer.alloc(8);
        buf.writeBigUInt64BE(num); 
        return buf;
    }

    emitter() {
        chain.load("emitter");

        chain.on({
            transaction: (trx) => {
                this.levelDb.set(this.encodeInt64(this.index++), JSON.stringify({type: "transaction", data: trx}));
            },
            block: (bk) => {
                this.levelDb.set(this.encodeInt64(this.index++), JSON.stringify({type: "block", data: bk}));
            },
            irreversible_block: (blk) => {
                this.levelDb.set(this.encodeInt64(this.index++), JSON.stringify({type: "irreversible", data: blk}));
            },
        });
    }

    async read(startIndex = 1, count = 100) {
        let input_key = [];
        for (let i = startIndex; i < startIndex + count; i++)
            input_key.push(this.encodeInt64(i));
        const messages = this.levelDb.mget(input_key);
        return messages.filter(item => item !== null);
    }
}

module.exports = MessageQueue;