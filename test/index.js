"use strict";

const fs = require("fs");
const test = require('test');
const path = require("path");
const coroutine = require('coroutine');
const child_process = require('child_process');

["", "\-shm", "\-wal"].forEach(function(k) {
	if (fs.exists("./fibos_chain.db" + k)) fs.unlink("./fibos_chain.db" + k);
});

if(fs.exists("./test.db")){
	child_process.run('rm', ['-rf', `./test.db`], { stdio: "inherit" });
}

require("./init.js");
require("../graphql.js");

coroutine.sleep(1000)
if (process.argv.length === 3) {
	run(`./case/${process.argv[2]}`)
} else {
	fs.readdir(path.join(__dirname, "./case"))
		.filter(f => f.slice(-3) == ".js")
		.forEach(f => run(`./case/${f}`));
}

test.run(console.INFO);

process.exit();