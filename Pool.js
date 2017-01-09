'use strict';

const Connection       = require('mysql/lib/Connection');
const ConnectionConfig = require('mysql/lib/ConnectionConfig');
const PoolConnection   = require('mysql/lib/PoolConnection');

const configDefaults = {
	acquireTimeout     : 10000, //10 seconds
	connectionConfig   : {},
	maxConnectionLimit : 10,
	minConnectionLimit : 0,
	queueLimit         : 0
};

class Pool {
	constructor (config) {
		this.isPool = true;

		Object.assign(this, configDefaults, config);

		this._connectionConfig = new ConnectionConfig(this.connectionConfig);

		//TODO maybe use Map?
		/**
		 * Holds all connections that have been created.
		 */
		this._connections     = [];
		/**
		 * Holds connections that are busy. This is connection either
		 * connecting or querying.
		 */
		this._busyConnections = [];
		/**
		 * Holds connections that are free to use meaning the connection
		 * has connected but is not querying.
		 */
		this._freeConnections = [];
		/**
		 * Holds queries that are queued when there are no free connections.
		 */
		this._queryQueue = [];
	}

	end () {
		return Promise
			.all(
				this._connections.map(
					this.$endConnection.bind(this)
				)
			)
			.then(this.$onEnd.bind(this));
	}

	getConnection () {
		return new Promise((resolve, reject) => {
			if (this._closed) {
				return reject(new Error('This pool is closed'));
			}

			if (this._freeConnections.length) {
				let connection = this._freeConnections.shift();

				resolve(connection);
			} else {
				if (this._connections.length < this.maxConnectionLimit) {
					const connection = this.$createConnection();

					this
						.$connectConnection(connection)
						.then(resolve)
						.catch((error) => {
							this.$removeConnection(connection);

							reject(error);
						});
				} else {
					reject(new Error('No connections available'));
				}
			}
		});
	}

	query (sql, values) {
		return new Promise((resolve, reject) => {
			if (this._closed) {
				reject(new Error('This pool is closed'));
			} else if (this._freeConnections.length === 0 && this._connections.length >= this.maxConnectionLimit) {
				if (!this.queueLimit || this._queryQueue.length < this.queueLimit) {
					this._queryQueue.push({
						reject,
						resolve,
						sql,
						values
					});
				} else {
					reject(new Error('Query queue is full'));
				}
			} else {
				this
					.getConnection()
					.then(connection => this.$query(connection, sql, values))
					.then(resolve)
					.catch(reject);
			}
		});
	}

	releaseConnection (connection) {
		return new Promise(resolve => resolve(this.$releaseConnection(connection)));
	}

	_purgeConnection (connection) {
		return new Promise(resolve => resolve(this.$removeConnection(connection)));
	}

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
		return new Promise((resolve, reject) => {
			this._busyConnections.push(connection);

			connection.connect(
				{
					timeout : this.acquireTimeout
				},
				(error) => {
					this.$releaseConnection(connection);

					if (this._closed) {
						reject(new Error('This pool is closed'));
					} else if (error) {
						reject(error);
					} else {
						resolve(connection);
					}
				}
			);
		});
	}

	$onEnd (arg) {
		this._closed = true;

		this._connectionConfig    =
			this._connections     =
			this._busyConnections =
			this._freeConnections =
			this._queryQueue      =
			null;

		return arg;
	}

	$endConnection (connection) {
		return connection
			.release()
			.then(connection => {
				connection.destroy();

				return connection;
			});
	}

	$createQuery (sql, values, callback) {
		const query = Connection.createQuery(sql, values, callback);

		if (!(typeof sql === 'object' && 'typeCast' in sql)) {
			query.typeCast = this.connectionConfig.typeCast;
		}

		if (this.connectionConfig.trace) {
			// Long stack trace support
			query._callSite = new Error();
		}

		return query;
	}

	$query (connection, sql, values) {
		return new Promise((resolve, reject) => {
			const query = this.$createQuery(sql, values, (error, results) => {
				if (error) {
					reject(error);
				} else {
					resolve(results);
				}
			});

			this._busyConnections.push(connection);

			//release the connection when a query ends
			query.once('end', this.$releaseConnection.bind(this, connection));

			connection.query(query);
		});
	}

	$releaseConnection (connection) {
		if (!this._closed) {
			this.$remove(this._busyConnections, connection);

			if (this._freeConnections.indexOf(connection) < 0) {
				this._freeConnections.push(connection);
			}

			if (this._freeConnections.length > 0 && this._queryQueue.length) {
				const item = this._queryQueue.shift();

				this
					.query(item.sql, item.values)
					.then(item.resolve, item.reject);
			}
		}

		return connection;
	}

	$removeConnection (connection) {
		if (!this._closed) {
			this.$remove(this._busyConnections, connection);
			this.$remove(this._freeConnections, connection);
			this.$remove(this._connections,     connection);
		}

		return connection;
	}

	$remove (arr, connection) {
		const idx = arr.indexOf(connection);

		if (idx >= 0) {
			arr.splice(idx, 1);
		}
	}
}

module.exports = Pool;
