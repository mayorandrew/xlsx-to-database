const mysql = require('mysql');
const csv = require('csv');
const {tickQuote, doubleQuote, quoteCsv, readStreamToArray} = require('./util');

class MySQL {

	constructor(config) {
		this.schema = config.schema;
		this.database = config.database;
		this.user = config.user;
		this.password = config.password;
		this.host = config.host;
	}

	connect() {
		this.connection = mysql.createConnection({
			host: this.host,
			user: this.user,
			password: this.password,
			database: this.database
		});

		return new Promise((resolve, reject) => {
			this.connection.connect((err) => {
				if (err) return reject(err);
				resolve();
			});
		});
	}

	close() {
		return new Promise((resolve, reject) => {
			this.connection.end((err) => {
				if (err) return reject(err);
				resolve();
			});
		});
	}

	queryPromise(query, params = []) {
		return new Promise((resolve, reject) => {
			this.connection.query(query, params, (err, results, fields) => {
				if (err) return reject(err);
				resolve({ results, fields });
			});
		});
	}

	dropTable(tableName) {
		return this.queryPromise(`DROP TABLE IF EXISTS ${tableName}`);
	}

	createTable(tableName, tableFields, ifNotExists = false) {
		const tableFieldsCreation = tableFields.map(f => `\`${f}\` text`).join(', ');
		const ifNotExistsQuery = ifNotExists ? 'IF NOT EXISTS': '';
		const query = `CREATE TABLE ${ifNotExistsQuery} ${tableName} ( ${tableFieldsCreation} ) character set 'utf8' collate 'utf8_general_ci'`;
		return this.queryPromise(query);
	}

	readCsvFileStream(tableName, tableFields, fileStream, csvSeparator) {
		const stream = fileStream.pipe(csv.parse({
			delimiter: csvSeparator
		}));

		return readStreamToArray(stream)
			.then((csvData) => {
				const [header, ...data] = csvData;
				const tableFieldsNames = tableFields.map(f => tickQuote(f)).join(',');
				const query = `INSERT INTO ${tableName} (${tableFieldsNames}) VALUES ?`;
				return this.queryPromise(query, [data]);
			});
	}

	insertValues(tableName, tableFields, insertValues) {
		const tableFieldsNames = tableFields.map(f => tickQuote(f)).join(',');
		const query = `INSERT INTO ${tableName} (${tableFieldsNames}) VALUES ?`;
		return this.queryPromise(query, [insertValues]);
	}

}

module.exports = MySQL;
