'use strict';

const ConnectionConfig = require('mysql/lib/ConnectionConfig');
const Pool             = require('../../Pool');
const PoolConnection   = require('./PoolConnection');

class PoolMock extends Pool {
    $createConnection () {
		let config           = this._connectionConfig,
			connectionConfig = new ConnectionConfig(config);

		connectionConfig.clientFlags   = config.clientFlags;
		connectionConfig.maxPacketSize = config.maxPacketSize;

		const connection = new PoolConnection(this, {
			config : connectionConfig
		});

		this._connections.push(connection);

		return connection;
	}

    $connectConnection (connection) {
        return new Promise(resolve => {
            this._busyConnections.push(connection);

            setTimeout(() => {
                this.$releaseConnection(connection);

                resolve(connection);
            }, 0);
        });
    }
}

module.exports = PoolMock;
