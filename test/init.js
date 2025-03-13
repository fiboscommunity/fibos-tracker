"use strict";
const http = require("http");
const fibos = require("chain");
const db = require("db");
const Tracker = require("../");
const mock_db = require("./mock_db.json");

if (process.env.TEST_USE_MYSQL)
	Tracker.Config.DBconnString = process.env.TEST_USE_MYSQL.startsWith('mysql://') ? process.env.TEST_USE_MYSQL : "mysql://root:123456@127.0.0.1/fibos_chain";

const LevelDB = db.openLevelDB('./test.db');
Tracker.Tracker.Config.LevelDB = LevelDB;
const tracker = new Tracker.Tracker();

fibos.on = (tr) => {
	tr.transaction(mock_db.transaction);
	tr.block(mock_db.block);
}

Tracker.QueueEmitter(LevelDB);
tracker.emitter();

const port = require('../port')

let httpServer = new http.Server(port, [
	(req) => {
		req.session = {};
	}, {
		'^/ping': (req) => {
			req.response.write("pong");
		},
		'/1.0/app': tracker.app,
		"*": [function(req) { }]
	},
	function(req) { }
]);

httpServer.crossDomain = true;
httpServer.start();