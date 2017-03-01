const XlsxStreamReader = require('xlsx-stream-reader');
const fs = require('fs');
const database = require('./database');

if (process.argv.length < 3) {
	console.log('Usage: node index.js [config] [filename]');
	return;
}

const config = require('./' + process.argv[2] + '.js');
const filename = process.argv[3];

const quote = n => "'" + n + "'";

let client = new database.Client(config);

client.connect().then(err => {
	if (err) throw err;
	let bookReader = new XlsxStreamReader();

	bookReader.on('error', error => {
		throw(error);
	});

	bookReader.on('end', () => {
		client.end();
	});

	bookReader.on('worksheet', sheetReader => {
		let sheetName = sheetReader.name;
		let tableName = 'temp_' + sheetName;
		let insertValues = [];
		let numInsert = 0;
		let inserter = null;

		function doInsert(insertValues) {
			numInsert++;
			let numRows = insertValues.length;
			console.log(`inserting ${numRows} values to ${tableName} [${numInsert}]`);
			return inserter.insertBatch(insertValues);
		}

		console.log('\nSheet ' + quote(sheetName));
		sheetReader.on('end', () => {
			if (insertValues.length > 0) {
				sheetReader.pause();
				doInsert(insertValues).then(() => {
					sheetReader.resume();
				});
				insertValues = [];
			}
		});

		sheetReader.on('row', row => {
			if (row.attributes.r == 1) {
				let tableFields = row.values.slice(1);
				inserter = client.inserter(tableName, tableFields);

				sheetReader.pause();
				client.dropTable(tableName)
					.then(() => console.log(`table ${tableName} dropped`))
					.then(() => client.createTable(tableName, tableFields))
					.then(() => console.log(`table ${tableName} created`))
					.then(() => sheetReader.resume());
			} else {
				insertValues.push(row.values.slice(1));
				if (insertValues.length > 1000) {
					sheetReader.pause();
					doInsert(insertValues)
						.then(() => sheetReader.resume());
					insertValues = [];
				}	
			}
		});

		sheetReader.process();
	});


	let fileStream = fs.createReadStream(filename);
	fileStream.pipe(bookReader);

});

