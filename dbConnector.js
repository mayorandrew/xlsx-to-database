const PostgreSQL = require('./PostgreSQL');
const MySQL = require('./MySQL');

function dbConnector(config) {
	if (config.type == 'PostgreSQL') {
		return new PostgreSQL(config);
	} else if (config.type == 'MySQL') {
		return new MySQL(config);
	}
}

module.exports = dbConnector;
