const csv = require('csv');
const path = require('path');
const XlsxStreamReader = require('xlsx-stream-reader');
const fs = require('fs');
const firstline = require('firstline');

const dbConnector = require('./dbConnector');
const {quote, doubleQuote, quoteCsv} = require('./util');

const argv = require('yargs')
	.option('config', {
		alias: 'c',
		required: true
	})
	.option('filename', {
		alias: 'f',
		required: true
	})
	.option('drop', {
		alias: 'd',
		default: false,
		type: 'boolean'
	})
	.option('table', {
		alias: 't',
		default: [],
		type: 'array'
	})
	.option('prefix', {
		alias: 'p',
		default: ''
	})
	.option('separator', {
		alias: 's',
		default: ','
	})
	.option('batch_size', {
		alias: 'b',
		default: 2000
	})
	.argv;

const sheetNamePrefix = argv.prefix;
const csvSeparator = argv.separator;
const config = require('./' + argv.config + '.js');

const db = dbConnector(config);

const filename = argv.filename;
const extension = path.extname(filename);
const drop = argv.drop;
const tables = argv.table;
const batchSize = argv.batch_size;

db.connect()
	.then(() => {

		function closeConnection() {
			db.close();
		}

		function recreateTable(tableName, tableFields) {
			let tableFieldsCreation = tableFields.map(f => `"${f}" text`);
			if (drop) {
				return db.dropTable(tableName)
					.then(() => {
						console.log(`table ${tableName} dropped`);
						console.log(`Creating table ${tableName}(${tableFieldsCreation})`);
						return db.createTable(tableName, tableFields);
					})
					.then(() => {
						console.log(`table ${tableName} created`);
					});
			}
			console.log(`Creating table ${tableName}(${tableFieldsCreation})`);
			return db.createTable(tableName, tableFields, true)
				.then(() => {
					console.log(`table ${tableName} created`);
				});
		}

		if (extension == '.csv' || extension == '.txt') {
			console.log(`Using direct copy from csv`);

			if (!tables || tables.length == 0) {
				console.log(`Table name is required in csv mode`);
				return closeConnection();
			}

			let tableName = sheetNamePrefix + tables[0];
			let tableFields = null;
			firstline(filename)
				.then((header) => {
					return new Promise((resolve, reject) => {
						csv.parse(header, {
							delimiter: csvSeparator
						}, (err, data) => {
							if (err) return reject(err);
							resolve(data[0]);
						});
					})
						.then((tableFields_) => {
							tableFields = tableFields_;
							return recreateTable(tableName, tableFields);
						});
				})
				.then(() => {
					let fileStream = fs.createReadStream(filename);
					db.readCsvFileStream(tableName, tableFields, fileStream, csvSeparator)
						.then(() => closeConnection());
				});

		} else if (extension == '.xlsx') {

			console.log(`Using xlsx stream reader`);

			let bookReader = new XlsxStreamReader();

			bookReader.on('error', error => {
				throw(error);
			});

			bookReader.on('end', () => {
				closeConnection();
			});

			bookReader.on('worksheet', sheetReader => {
				let sheetName = sheetReader.name;
				let tableName = sheetNamePrefix + sheetName;
				let tableFields = null;
				let tableFieldsNames = null;
				let valuesQuery = null;
				let insertValues = [];
				let numInsert = 0;

				function doInsert(insertValues) {
					numInsert++;
					let numRows = insertValues.length;
					console.log(`inserting ${numRows} values to ${tableName} [${numInsert}]`);
					return db.insertValues(tableName, tableFields, insertValues);
				}

				sheetReader.on('end', () => {
					if (insertValues.length > 0) {
						sheetReader.pause();
						doInsert(insertValues).then(() => {
							sheetReader.resume();
						});
						insertValues = [];
					}
				});

				if (tables.length == 0 || tables.indexOf(sheetName) >= 0) {
					sheetReader.on('row', row => {
						if (row.attributes.r == 1) {
							tableFields = row.values.slice(1);

							while (tableFields.length > 0 && (!tableFields[tableFields.length - 1] || !tableFields[tableFields.length - 1].trim())) {
								tableFields = tableFields.slice(0, tableFields.length - 1);
							}

							if (tableFields.length == 0) {
								let error = `Empty table header discovered for table ${tableName}`;
								console.log(error);
								throw new Error(error);
							}

							tableFieldsNames = tableFields.map(doubleQuote).join(',');
							valuesQuery = '(' + tableFields.map((f, i) => `$${i + 1}::text`).join(',') + ')';

							sheetReader.pause();

							recreateTable(tableName, tableFields)
								.then(() => sheetReader.resume());
						} else {
							insertValues.push(row.values.slice(1));
							if (insertValues.length >= batchSize) {
								sheetReader.pause();
								doInsert(insertValues).then(() => {
									sheetReader.resume();
								});
								insertValues = [];
							}
						}
					});
					console.log('\nSheet ' + quote(sheetName));
					sheetReader.process();
				} else {
					console.log('\nIgnoring sheet ' + quote(sheetName));
					sheetReader.skip();
				}
			});


			let fileStream = fs.createReadStream(filename);
			fileStream.pipe(bookReader);

		} else {

			console.log(`Unknown extension ${extension}`);
			closeConnection();

		}

	});

