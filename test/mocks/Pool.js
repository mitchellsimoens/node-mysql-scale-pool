'use strict';

const ConnectionConfig = require('mysql/lib/ConnectionConfig');
const Pool             = require('../../Pool');
const PoolConnection   = require('./PoolConnection');

class PoolMock extends Pool {
    constructor (config = {}) {
        config.connectionClass = PoolConnection;

        super(config);
    }

    $createConnection () {
		let config           = this.$connectionConfig,
			connectionConfig = new ConnectionConfig(config);

		connectionConfig.clientFlags   = config.clientFlags;
		connectionConfig.maxPacketSize = config.maxPacketSize;

		const connection = new this.connectionClass(this, {
			config : connectionConfig
		});

        this.$add(this.$connections, connection);

		return connection;
	}

    $connectConnection (connection) {
        return new Promise(resolve => {
            this.$add(this.$busyConnections, connection);

            /**
             * connection is async, use setTimeout to fake
             */
            setTimeout(() => {
                this.$releaseConnection(connection);

                resolve(connection);
            }, 0);
        });
    }
}

module.exports = PoolMock;
