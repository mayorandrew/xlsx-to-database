const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;
const XlsxStreamReader = require('./xlsx-stream-reader');
const config = require('./config.js');
const LineByLineReader = require('line-by-line');

let filename = 'data/СПО_db_2013.txt';
let sheetName = 'ages_2013';

let doubleQuote = n => `"${n}"`;

let pool = new pg.Pool(config);
let poolPromise = pool.connect();

poolPromise.then(client => {
	let tableName = 'temp_' + sheetName;
	let tableFields = null;
	let tableFieldsNames = null;
	let insertValues = [];
	let numInsert = 0;

	function doInsert(insertValues) {
		numInsert++;
		let numRows = insertValues.length;
		console.log(`inserting ${numRows} values to ${tableName} [${numInsert}]`);
		let copyStream = client.query(copyFrom(`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER '|' )`));

		insertValues.forEach((row, iRow) => {
			if (row != '') {
				copyStream.write(row);
				copyStream.write('\n');
			}	
		});

		let promise = new Promise((resolve) => {
			copyStream.on('end', resolve);
		});

		copyStream.end();

		return promise;
	}

	let readStream = new LineByLineReader(filename);

	readStream.on('line', line => {
		if (tableFields == null) {
			tableFields = line.split('|');
			tableFieldsNames = tableFields.map(doubleQuote).join(',');
			let tableFieldsCreation = tableFields.map(f => `"${f}" text`);

			readStream.pause();
			client.query(`DROP TABLE IF EXISTS ${tableName}`)
				.then(() => {
					console.log(`table ${tableName} dropped`);
					return client.query(`CREATE TABLE ${tableName} ( ${tableFieldsCreation} )`);
				})
				.then(() => {
					console.log(`table ${tableName} created`);
					readStream.resume();
				});
		} else {
			insertValues.push(line);
			if (insertValues.length > 10000) {
				readStream.pause();
				doInsert(insertValues).then(() => {
					readStream.resume();
				});
				insertValues = [];
			}
		}
	});

	readStream.on('end', () => {
		doInsert(insertValues).then(() => {
			client.end()
		});
		insertValues = [];
	});

});

