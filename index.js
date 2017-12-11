const pg = require('pg');
const path = require('path');
const copyFrom = require('pg-copy-streams').from;
const XlsxStreamReader = require('xlsx-stream-reader');
const fs = require('fs');
const firstline = require('firstline');

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
		default: '|'
	})
	.option('batch_size', {
		alias: 'b',
		default: 2000
	})
	.argv;

const sheetNamePrefix = argv.prefix;
const csvSeparator = argv.separator;
const config = require('./' + argv.config + '.js');
const dbSchema = config.schema;
const filename = argv.filename;
const extension = path.extname(filename);
const drop = argv.drop;
const tables = argv.table;
const batchSize = argv.batch_size;

const quote = n => "'" + n + "'";
const doubleQuote = n => `"${n}"`;
const quoteCsv = n => '"' + n.replace(/\"/g, '\"\"').replace(/[\r\n]/g, '') + '"'

let pool = new pg.Pool(config);
let poolPromise = pool.connect();

poolPromise
    .then(client =>
        client.query(`set search_path to ${dbSchema}`).then(() => client)
    )
    .then(client => {

        function closeConnection() {
			client.end(error => {
				if (error) throw error;
			});
        }

        function recreateTable(tableName, tableFields) {
			let tableFieldsCreation = tableFields.map(f => `"${f}" text`);
			if (drop) {
				return client.query(`DROP TABLE IF EXISTS ${tableName}`)
					.then(() => {
						console.log(`table ${tableName} dropped`);
						console.log(`Creating table ${tableName}(${tableFieldsCreation})`);
						return client.query(`CREATE TABLE ${tableName} ( ${tableFieldsCreation} )`);
					})
					.then(() => {
						console.log(`table ${tableName} created`);
					});
			}
			console.log(`Creating table ${tableName}(${tableFieldsCreation})`);
			return client.query(`CREATE TABLE IF NOT EXISTS ${tableName} ( ${tableFieldsCreation} )`)
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
					tableFields = header.split(csvSeparator).map(t => t.trim());
					return recreateTable(tableName, tableFields);
				})
				.then(() => {
					let fileStream = fs.createReadStream(filename);
					let tableFieldsNames = tableFields.map(doubleQuote).join(',');
					let copyStream = client.query(copyFrom(`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER '${csvSeparator}', HEADER )`));
					copyStream.on('end', () => closeConnection());
					fileStream.pipe(copyStream);
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
					let copyStream = client.query(copyFrom(`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER ',' )`));
					// let copyStream = fs.createWriteStream(`${tableName}.csv`);
					// copyStream.write(tableFieldsNames);
					// copyStream.write('\n');
					insertValues.forEach((row, iRow) => {
						tableFields.forEach((col, iCol) => {
							let v = row[iCol];
							if (v != null) {
								copyStream.write(quoteCsv(v.trim()));
							}
							if (iCol < tableFields.length - 1) {
								copyStream.write(',');
							}
						});
						if (iRow < numRows - 1) {
							copyStream.write('\n');
						}
					});

					let promise = new Promise((resolve) => {
						copyStream.on('end', resolve);
					});

					copyStream.end();

					return promise;
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

