const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;

const doubleQuote = n => `"${n}"`;
const quoteCsv = n => '"' + n.replace(/\"/g, '\"\"').replace(/[\r\n]/g, '') + '"';

class Inserter {
	constructor(client, tableName, tableFields) {
		this.client = client;
		this.tableName = tableName;
		this.tableFields = tableFields;
		this.tableFieldsNames = tableFields.map(doubleQuote).join(',');
		this.valuesQuery = '(' + tableFields.map((f, i) => `$${i+1}::text`).join(',') + ')';
	}

	insertBatch(insertValues) {
		return this.client.insertBatch(this, insertValues);
	}
}

class Client {
	constructor(config) {
		super();
		this.config = config;
	}

	connect() {
		this.client = new pg.Client(this.config);
		return this.client.connect();
	}

	end() {
		return this.client.end();
	}

	insertBatch({tableName, tableFieldsNames, tableFields}, insertValues) {
		let copyStream = this.client.query(copyFrom(`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER ',' )`));

		insertValues.forEach((row, iRow) => {
			tableFields.forEach((col, iCol) => {
				let v = row[iCol];
				if (v != null) {
					copyStream.write(quoteCsv(v));
				}
				if (iCol < this.tableFields.length - 1) {
					copyStream.write(',');
				}
			});
			if (iRow < numRows - 1) {
				copyStream.write('\n');
			}
		});

		let promise = new Promise((resolve) => copyStream.on('end', resolve));
		copyStream.end();
		return promise;
	}

	inserter(tableName, tableFields) {
		return new Inserter(this, tableName, tableFields);
	}

	dropTable(tableName) {
		return client.query(`DROP TABLE IF EXISTS ${tableName}`);
	}

	createTable(tableName, tableFields) {
		let tableFieldsCreation = tableFields.map(f => `"${f}" text`);
		return client.query(`CREATE TABLE ${tableName} ( ${tableFieldsCreation} )`);
	}

}

module.exports.Client = Client;
