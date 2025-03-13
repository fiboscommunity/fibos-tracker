"use strict";

const App = require('fib-app');
const Config = require("./conf/conf.json");
const util = require("util");
const fs = require("fs");
const chain = require("chain");
const cache = require("fib-cache");
const coroutine = require('coroutine');
const MessageQueue = require("./queue");

BigInt.prototype.toJSON = function() { return this.toString(); }

let block_caches = new cache.LRU({max: 2000, ttl: 1000 * 60 * 60 * 24});

function Tracker() {
	console.notice(`==========fibos-tracker==========\n\nDBconnString: ${Config.DBconnString.replace(/:[^:]*@/, ":*****@")}\n\n==========fibos-tracker==========`);
	let chain_name = chain.name || "eosio";
	let hookEvents = {};
	let sys_bn, nore_bn;
	let is_running = true;
	let app = new App(Config.DBconnString);
	let messageQueue = null;
	if(Config.LevelDB)
		messageQueue = new MessageQueue(Config.LevelDB);
	
	app.db.use(require('./defs'));

    let checkBlockNum = (block_num, type) => {
		block_num = Number(block_num);
		let check_num = sys_bn;
		if (type && type == "irreversible") check_num = nore_bn;
		if (check_num >= block_num) {
			console.warn("sys block_num(%s) >= node block_num(%s)", check_num, block_num);
			return false;
		}

		return true;
	}

	this.app = app;

	this.use = (model) => {
		if (!model) throw new Error("use:function(model)");

		if (!model.defines || !model.hooks) throw new Error("model define error: Array(defines) JSON(hooks)");

		let defines = model.defines;
		let hooks = model.hooks;

		app.db.use(util.isArray(defines) ? defines : [defines]);

		for (let f in hooks) {
			hookEvents[f] = hookEvents[f] || [];
			hookEvents[f].push(hooks[f]);
		}
	};

	function dealData(db, msg, event) {
		let messages = {};
		event = event ? event + ":" : "";
		let collectMessage = (_at) => {
			function _c(f) {
				if (hookEvents[f]) {
					messages[f] = messages[f] || [];
					messages[f].push(_at);
				}
			}

			if (_at.receipt.receiver !== _at.act.account) return;
			_c(event + _at.act.account);

			_c(event + _at.act.account + "/" + _at.act.name);
		}

		function execActions(at, parent) {
			if (parent) {
				let _parent = parent;
				delete _parent.inline_traces;
				at.parent = _parent;
			}

			collectMessage(at);

			if (at.inline_traces)
				at.inline_traces.forEach((_at) => {
					execActions(_at, at);
				});
		}

		execActions(msg);

		for (let f in messages) {
			let ats = messages[f];
			let hooks = hookEvents[f];
			if (hooks) hooks.forEach((hook) => {
				try {
					hook(db, ats)
				} catch (e) {
					console.error("[%s]", f, ats, e.stack);
				}
			});
		}
	}

	function cleanTrans(trx) {
		trx.action_traces.forEach(at => {
			delete at.receipt.act_digest;
			delete at.receipt.auth_sequence;
			delete at.act.data;
			delete at.act.hex_data;
			delete at.act.authorization;
			delete at.account_ram_deltas;
			delete at.account_disk_deltas;
			delete at.return_value_hex_data;
		});
	}

	function cleanBlock(blk) {
		blk.transactions.forEach(t => {
			cleanTrans(t.rawData);
		});
	}

	function updateStartIndex(file_path, newIndex) {
		try {
			const jsonData = { startIndex: newIndex };
			fs.writeFileSync(file_path, JSON.stringify(jsonData, null, 4), 'utf8');
		} catch (error) {
			console.error('Error updating JSON file:', error);
		}
	}
	
	this.emitter = async (startIndex = 1, handler_counts = 100) => {
		sys_bn = app.db(db => {
			return db.models.fibos_blocks.get_sys_last();
		});

		nore_bn = app.db(db => {
			return db.models.fibos_blocks.get_final_irreversible_block();
		});
        
		while (true) {
			const messages = await messageQueue.read(startIndex, handler_counts);
			for (const message of messages) {
				let msg = JSON.parse(message);
				switch (msg.type) {
					case 'transaction': {
						handleTransaction(msg.data);
						break;
					}
					case 'block': {
						handleBlock(msg.data);
						break;
					}
					case 'irreversible': {
						handleIrreversible(msg.data);
						break;
					}
				}
			}
			
			startIndex += messages.length;
			if(!is_running){
				console.log(`closing tracker process, and the next time you should get data from the current index ${startIndex}`);
				if(Config.StartIndexFile)
					updateStartIndex(Config.StartIndexFile, startIndex);
				if(Config.LevelDB)
					Config.LevelDB.close();
				process.exit(0);
			}
			if (messages.length === handler_counts) {
				console.log(`cureent index: ${startIndex}`);
			} else {
				console.log(`The message queue has been reached, and the next time you should get data from the current index ${startIndex}`);
				coroutine.sleep(1000 * 60);
			}
		}
	}

	function handleTransaction(trx) {
		let block_num = trx.block_num.toString();
		let producer_block_id = trx.producer_block_id;

		if (!producer_block_id || !checkBlockNum(block_num) || !trx.action_traces || !trx.action_traces.length) {
			return;
		}

		let contract_action = trx.action_traces[0].act.account + "/" + trx.action_traces[0].act.name;
		if (contract_action == `${chain_name}/onblock`) return;

		app.db(db => {
			let Transactions = db.models.fibos_transactions;
			let t = Transactions.oneSync({
				trx_id: trx.id,
				producer_block_id: trx.producer_block_id,
			});

			if (t) return;

			db.trans(() => {
				let transaction = Transactions.createSync({
					trx_id: trx.id,
					producer_block_id: trx.producer_block_id,
					rawData: trx,
					contract_action: contract_action
				});

				trx.action_traces.forEach(m => { saveActions(m); });

				function saveActions(m, p_id) {
					let _m = m;
					delete _m.inline_traces;
					let _p_id;

					if (_m.receipt.receiver == _m.act.account) {
						_p_id = db.driver.execQuerySync(`insert into fibos_actions(trx_id,global_sequence,contract_action,rawData,parent_id,transaction_id) values(?,?,?,?,?,?)`, [_m.trx_id, _m.receipt.global_sequence, _m.act.account + "/" + _m.act.name, JSON.stringify(_m), p_id, transaction.id]).insertId;
					}

					if (m.inline_traces)
						m.inline_traces.forEach(_m => { saveActions(_m, _p_id); });
				}
			});
		});

		cleanTrans(trx);
        block_caches.get(producer_block_id, (id) => { return { transactions: [ { rawData: trx } ] } });
	}

	function handleBlock(bk) {
		let block_num = bk.block_num.toString();

		if (!checkBlockNum(block_num) || !bk.block) {
			return;
		}

		let _trxs = block_caches.get(bk.id);
		let now_block = {
			producer_block_id: bk.id,
			previous: bk.block.previous,
			block_num: bk.block_num,
			producer: bk.block.producer,
			block_time: bk.block.timestamp,
			transactions: !!_trxs ? _trxs.transactions : [],
			status: "pending"
		};

		let c_block = now_block;
		cleanBlock(now_block);
		block_caches.set(now_block.producer_block_id, now_block);

		app.db(db => {
			let Blocks = db.models.fibos_blocks;
			db.trans(() => {
				if (Blocks.get(bk.id)) {
					console.warn("Reentrant block id:", bk.id);
					return;
				}

				let f_block = Blocks.createSync({
					block_num: c_block.block_num,
					block_time: c_block.block_time,
					producer: c_block.producer,
					producer_block_id: c_block.producer_block_id,
					previous: c_block.previous,
					status: "pending"
				});

				c_block.transactions.forEach((trx) => {
					db.driver.execQuerySync(`update fibos_transactions set block_id = ? where producer_block_id =?`, [f_block.id, c_block.producer_block_id]);
					trx.rawData.action_traces.forEach((msg) => { dealData(db, msg, 'pending'); });
				});
			});
		});
	}

	function handleIrreversible(blk) {
		let block_num = blk.block_num.toString();
		if (!checkBlockNum(block_num, 'irreversible')) return;

		let producer_block_id = blk.id;
		app.db(db => {
			let _block = db.models.fibos_blocks.oneSync({
				producer_block_id: producer_block_id
			});

			if (!_block) return;

			let _transactions = db.models.fibos_transactions.find({ producer_block_id: producer_block_id }).order("id").runSync();
			if (!_transactions || !_transactions.length) return;

			let block = {
				producer_block_id: _block.producer_block_id,
				previous: _block.previous,
				block_num: _block.block_num,
				producer: _block.producer,
				block_time: _block.block_time,
				transactions: _transactions,
				status: _block.status
			};

			db.trans(() => {
				if (block.status === 'pending') {
					block.transactions.forEach(trx => { trx.rawData.action_traces.forEach(msg => { dealData(db, msg); }); });
				}
				block.status = "irreversible";
				block.transactions.forEach(trx => { trx.rawData.action_traces.forEach(msg => { dealData(db, msg, 'irreversible'); }); });
				db.driver.execQuerySync(`update fibos_blocks set status = 'irreversible' where producer_block_id = ?`, [producer_block_id]);
			});
		});
	}

	this.diagram = () => fs.writeTextFile(process.cwd() + '/diagram.svg', app.diagram());

	this.stop = () => {
		console.log("close tracker");
		is_running = false;
	}
}

Tracker.Config = Config;
module.exports = {Tracker, QueueEmitter: require('./emitter')};