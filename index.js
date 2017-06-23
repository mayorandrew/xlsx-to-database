const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;
const XlsxStreamReader = require('./xlsx-stream-reader');
const fs = require('fs');

if (process.argv.length < 4) {
    console.log('Usage: node index.js [config] [filename] [drop/nodrop]');
    console.log('Example: node index.js config-vo data/20170623/temp_headers.xlsx nodrop');
    console.log('Example: node index.js config-vo data/20170623/temp_info_head.xlsx drop');
    return;
}

const config = require('./' + process.argv[2] + '.js');
const filename = process.argv[3];
const drop = process.argv[4] == 'drop';

const quote = n => "'" + n + "'";
const doubleQuote = n => `"${n}"`;
const quoteCsv = n => '"' + n.replace(/\"/g, '\"\"').replace(/[\r\n]/g, '') + '"'

let pool = new pg.Pool(config);
let poolPromise = pool.connect();

poolPromise.then(client => {

    let bookReader = new XlsxStreamReader();

    bookReader.on('error', error => {
        throw(error);
    });

    bookReader.on('end', () => {
        client.end(error => {
            if (error) throw error;
        });
    });

    bookReader.on('worksheet', sheetReader => {
        let sheetName = sheetReader.name;
        let tableName = 'temp_' + sheetName;
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
                        copyStream.write(quoteCsv(v));
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
                tableFields = row.values.slice(1);
                tableFieldsNames = tableFields.map(doubleQuote).join(',');
                valuesQuery = '(' + tableFields.map((f, i) => `$${i + 1}::text`).join(',') + ')';

                let tableFieldsCreation = tableFields.map(f => `"${f}" text`);

                if (drop) {
                    sheetReader.pause();
                    client.query(`DROP TABLE IF EXISTS ${tableName}`)
                        .then(() => {
                            console.log(`table ${tableName} dropped`);
                            return client.query(`CREATE TABLE ${tableName} ( ${tableFieldsCreation} )`);
                        })
                        .then(() => {
                            console.log(`table ${tableName} created`);
                            sheetReader.resume();
                        });
                }
            } else {
                insertValues.push(row.values.slice(1));
                if (insertValues.length > 1000) {
                    sheetReader.pause();
                    doInsert(insertValues).then(() => {
                        sheetReader.resume();
                    });
                    insertValues = [];
                }
            }
        });

        sheetReader.process();
    });


    let fileStream = fs.createReadStream(filename);
    fileStream.pipe(bookReader);

});

