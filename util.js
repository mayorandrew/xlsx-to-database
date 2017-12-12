const quote = n => "'" + n + "'";
const doubleQuote = n => `"${n}"`;
const quoteCsv = n => '"' + n.replace(/\"/g, '\"\"').replace(/[\r\n]/g, '') + '"';
const tickQuote = n => `\`${n}\``;

function readStreamToArray(stream) {
	return new Promise((resolve, reject) => {
		let result = [];

		stream.on('readable', () => {
			let record;
			while (record = stream.read()) {
				result.push(record);
			}
		});

		stream.on('finish', () => {
			resolve(result);
		});
	});
}

module.exports = {
	quote,
	doubleQuote,
	quoteCsv,
	tickQuote,
	readStreamToArray
};
