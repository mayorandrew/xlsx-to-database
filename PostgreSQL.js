const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;
const {doubleQuote, quoteCsv} = require('./util');

class PostgreSQL {
	constructor(config) {
		this.schema = config.schema;
		this.database = config.database;
		this.user = config.user;
		this.password = config.password;
    this.host = config.host;
    this.port = config.port;
	}

	connect() {
		this.pool = new pg.Pool({
			database: this.database,
			user: this.user,
			password: this.password,
			host: this.host,
			port: this.port
		});

		return this.pool.connect()
			.then((client) => {
				this.client = client;
				return client.query(`set search_path to ${this.schema}`);
			});
	}

	close() {
		this.client.end(error => {
			if (error) throw error;
		});
	}

	dropTable(tableName) {
		return this.client.query(`DROP TABLE IF EXISTS ${tableName}`);
	}

	createTable(tableName, tableFields, ifNotExists = false) {
		const tableFieldsCreation = tableFields.map(f => `"${f}" text`);
		const ifNotExistsQuery = ifNotExists ? 'IF NOT EXISTS': '';
		return this.client.query(`CREATE TABLE ${ifNotExistsQuery} ${tableName} ( ${tableFieldsCreation} )`);
	}

	readCsvFileStream(tableName, tableFields, fileStream, csvSeparator) {
		let tableFieldsNames = tableFields.map(doubleQuote).join(',');
		let copyStream = this.client.query(
			copyFrom(
				`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER '${csvSeparator}', HEADER )`
			)
		);
		return new Promise((resolve, reject) => {
			copyStream.on('end', () => resolve());
			fileStream.pipe(copyStream);
		});
	}

	insertValues(tableName, tableFields, insertValues) {
		let numRows = insertValues.length;
		let tableFieldsNames = tableFields.map(doubleQuote).join(',');
		let copyStream = this.client.query(
			copyFrom(
				`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER ',' )`
			)
		);

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

}

module.exports = PostgreSQL;
