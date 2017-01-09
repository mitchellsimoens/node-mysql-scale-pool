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

		/**
		 * Holds all connections that have been created.
		 */
		this._connections     = new Set();
		/**
		 * Holds connections that are busy. This is connection either
		 * connecting or querying.
		 */
		this._busyConnections = new Set();
		/**
		 * Holds connections that are free to use meaning the connection
		 * has connected but is not querying.
		 */
		this._freeConnections = new Set();
		/**
		 * Holds queries that are queued when there are no free connections.
		 */
		this._queryQueue = new Set();
	}

	end () {
		let promises = [];

		for (let connection of this._connections) {
			promises.push(this.$endConnection(connection));
		}

		return Promise
			.all(promises)
			.then(this.$onEnd.bind(this));
	}

	destroy () {
		let promises = [];

		for (let connection of this._connections) {
			promises.push(this.$destroyConnection(connection));
		}

		return Promise
			.all(promises)
			.then(this.$onEnd.bind(this));
	}

	getConnection () {
		return new Promise((resolve, reject) => {
			if (this._closed) {
				return reject(new Error('This pool is closed'));
			}

			if (this._freeConnections.size) {
				let connection = this.$first(this._freeConnections);

				resolve(connection);
			} else {
				if (this._connections.size < this.maxConnectionLimit) {
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
			} else if (this._freeConnections.size === 0 && this._connections.size >= this.maxConnectionLimit) {
				if (!this.queueLimit || this._queryQueue.size < this.queueLimit) {
					this.$add(this._queryQueue, {
						reject,
						resolve,
						sql,
						values
					}, true);
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

		this.$add(this._connections, connection);

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

		this
			.$clear(this._connections)
			.$clear(this._busyConnections)
			.$clear(this._freeConnections)
			.$clear(this._queryQueue);

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
				if (typeof connection.end !== 'function') {
					console.log(connection);
				}
				connection.end();

				return connection;
			});
	}

	$destroyConnection (connection) {
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

			this.$add(this._busyConnections, connection);

			//release the connection when a query ends
			query.once('end', this.$releaseConnection.bind(this, connection));

			connection.query(query);
		});
	}

	$releaseConnection (connection) {
		if (!this._closed) {
			this.$remove(this._busyConnections, connection)
				.$add(this._freeConnections, connection);

			if (this._freeConnections.size > 0 && this._queryQueue.size) {
				const item = this.$first(this._queryQueue);

				this
					.query(item.sql, item.values)
					.then(item.resolve, item.reject);
			}
		}

		return connection;
	}

	$removeConnection (connection) {
		if (!this._closed) {
			this.$remove(this._busyConnections, connection)
				.$remove(this._freeConnections, connection)
				.$remove(this._connections,     connection);
		}

		return connection;
	}

	$first (set, remove = true) {
		const values = set.values();
        const item   = values.next().value;

		if (remove) {
			set.delete(item);
		}

		return item;
	}

	$add (set, connection, skipCheck = false) {
		if (skipCheck || !set.has(connection)) {
			set.add(connection);
		}

		return this;
	}

	$clear (set) {
		set.clear();

		return this;
	}

	$remove (set, connection) {
		set.delete(connection);

		return this;
	}
}

module.exports = Pool;
