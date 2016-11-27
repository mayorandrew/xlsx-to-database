const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;
const xlsx = require('xlsx');
const fs = require('fs');
const async = require('async');

let filename = 'graduate.edu.ru_ВО_2013+2014_20161112.xlsb';
let quote = n => "'" + n + "'";
let doubleQuote = n => `"${n}"`;
let quoteCsv = n => '"' + n.replace(/\"/g, '\"\"').replace(/[\r\n]/g, '') + '"'

let config = {
	user: 'biroot',
	database: 'bi2',
	password: '12345678',
	host: 'localhost'
};

let pool = new pg.Pool(config);
let poolPromise = pool.connect();

poolPromise.then(client => {

	console.log('parsing xlsx file');
	let book = xlsx.readFile(filename);
	let current = new Promise((resolve, reject) => { resolve(); });

	book.SheetNames.forEach((sheetName) => {
		console.log('> ', sheetName);
		let sheet = book.Sheets[sheetName];
		let tableName = 'temp_' + sheetName;

		if (!sheet) {
			console.log(`skipping ${tableName}`)
			return current;
		}

		let range = xlsx.utils.decode_range(sheet['!ref']);

		console.log('extracting header');
		let tableFields = [];
		for (let x = range.s.c; x <= range.e.c; x++) {
			tableFields.push(sheet[
				xlsx.utils.encode_cell({c:x, r:range.s.r})
			].v);
		}
		let tableFieldsNames = tableFields.map(f => `"${f}"`).join(',');

		range.s.r++;
		range.e.r = Math.max(range.e.r, range.s.r);
		sheet['!ref'] = xlsx.utils.encode_range(range);

		let tableFieldsCreation = tableFields.map(f => `"${f}" text`);

		current = current
			.then(() => {
				console.log(`dropping table ${tableName}`);
				return client.query(`DROP TABLE IF EXISTS ${tableName}`);
			})
			.then(() => {
				console.log(`table ${tableName} dropped`);
				return client.query(`CREATE TABLE ${tableName} ( ${tableFieldsCreation} )`);
			})
			.then(() => {
				return new Promise((resolve, reject) => {
					console.log(`table ${tableName} created`);
					let copyStream = client.query(copyFrom(`COPY ${tableName}(${tableFieldsNames}) FROM STDIN WITH ( FORMAT 'csv', DELIMITER ',' )`));
					// let copyStream = fs.createWriteStream(`${tableName}.csv`);
					// copyStream.on('finish', resolve);

					copyStream.on('end', resolve);
					

					var txt = "", qreg = /"/g;
					var o = {};
					if(sheet == null || sheet["!ref"] == null) return "";
					var r = xlsx.utils.decode_range(sheet["!ref"]);
					var FS = o.FS !== undefined ? o.FS : ",", fs = FS.charCodeAt(0);
					var RS = o.RS !== undefined ? o.RS : "\n", rs = RS.charCodeAt(0);
					var row = "", rr = "", cols = [];
					var i = 0, cc = 0, val;
					var R = 0, C = 0;
					for(C = r.s.c; C <= r.e.c; ++C) cols[C] = xlsx.utils.encode_col(C);
					for(R = r.s.r; R <= r.e.r; ++R) {
						row = "";
						rr = xlsx.utils.encode_row(R);
						for(C = r.s.c; C <= r.e.c; ++C) {
							val = sheet[cols[C] + rr];
							txt = val !== undefined ? ''+xlsx.utils.format_cell(val) : "";
							for(i = 0, cc = 0; i !== txt.length; ++i) if((cc = txt.charCodeAt(i)) === fs || cc === rs || cc === 34) {
								txt = "\"" + txt.replace(qreg, '""') + "\""; break; }
							row += (C === r.s.c ? "" : FS) + txt;
						}
						copyStream.write(row + RS);
					}



					copyStream.end();
					return copyStream;
				});	
			})
			.then(() => {
				console.log(`table ${tableName} filled`);
			});

		return current;
	})

	current.then(() => {
		client.end();
	});

});




