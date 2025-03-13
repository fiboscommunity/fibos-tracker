"use strict";

const MessageQueue = require("./queue");

function startEmitter(LevelDB) {
    console.notice("==========chain-emitter==========\n\nLevelDB: ${Config.LevelDB}\n\n==========chain-emitter==========");
    const messageQueue = new MessageQueue(LevelDB);
    messageQueue.emitter();
}

module.exports = startEmitter;