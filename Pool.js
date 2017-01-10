'use strict';

const Connection       = require('mysql/lib/Connection');
const ConnectionConfig = require('mysql/lib/ConnectionConfig');
const PoolConnection   = require('mysql/lib/PoolConnection');

const configDefaults = {
	acquireTimeout     : 10000, // 10 seconds
	bufferOnConstruct  : true,
	connectionBuffer   : 5,
	connectionConfig   : {},
	connectionDecay    : 300000, // 5 minutes
	maxConnectionLimit : 10,
	minConnectionLimit : 0,
	queueLimit         : 0,
	scaleInterval      : 300000 // 5 minutes
};

class Pool {
	constructor (config) {
		this.isPool = true;

		Object.assign(this, configDefaults, config);

		this.$connectionConfig = new ConnectionConfig(this.connectionConfig);

		/**
		 * Holds all connections that have been created.
		 */
		this.$connections     = new Set();
		/**
		 * Holds connections that are busy. This is connection either
		 * connecting or querying.
		 */
		this.$busyConnections = new Set();
		/**
		 * Holds connections that are free to use meaning the connection
		 * has connected but is not querying.
		 */
		this.$freeConnections = new Set();
		/**
		 * Holds queries that are queued when there are no free connections.
		 */
		this.$queryQueue = new Set();

		if (this.scaleInterval && this.connectionDecay) {
			this.$scaleInterval = setInterval(this.$onScaleInterval.bind(this), this.scaleInterval);
		}

		if (this.bufferOnConstruct) {
			this.$maybeBufferConnection();
		}
	}

	end () {
		let promises = [];

		for (let connection of this.$connections) {
			promises.push(this.$endConnection(connection));
		}

		return Promise
			.all(promises)
			.then(this.$onEnd.bind(this));
	}

	destroy () {
		let promises = [];

		for (let connection of this.$connections) {
			promises.push(this.$destroyConnection(connection));
		}

		return Promise
			.all(promises)
			.then(this.$onEnd.bind(this));
	}

	getConnection (fromBuffer = false) {
		return new Promise((resolve, reject) => {
			if (this.$closed) {
				return reject(new Error('This pool is closed'));
			}

			if (this.$freeConnections.size) {
				/**
				 * We have free connections, use one.
				 */
				let connection = this.$first(this.$freeConnections);

				resolve(connection);
			} else if (this.$connections.size < this.maxConnectionLimit) {
				/**
				 * We have no free connections and we haven't reached
				 * the maxConnectionLimit so create and connect one.
				 */
				const connection = this.$createConnection();

				this
					.$connectConnection(connection)
					.then(resolve)
					.catch((error) => {
						this.$removeConnection(connection);

						reject(error);
					});
			} else {
				/**
				 * We reached the maxConnectionLimit so we cannot
				 * create a new one.
				 */
				reject(new Error('No connections available'));
			}
		});
	}

	query (sql, values) {
		return new Promise((resolve, reject) => {
			if (this.$closed) {
				reject(new Error('This pool is closed'));
			} else if (this.$freeConnections.size === 0 && this.$connections.size >= this.maxConnectionLimit) {
				/**
				 * We have no free connections to work with and have reached the
				 * maxConnectionLimit so we have to see if we can queue the query.
				 */
				if (!this.queueLimit || this.$queryQueue.size < this.queueLimit) {
					/**
					 * Great, we can queue the query. This will then be run
					 * and resolved/rejected when a connection is released.
					 */
					this.$add(this.$queryQueue, {
						reject,
						resolve,
						sql,
						values
					}, true);
				} else {
					/**
					 * Hopefully this never happens. We cannot queue a query and will fail
					 * the attempt.
					 */
					reject(new Error('Query queue is full'));
				}
			} else {
				/**
				 * All is good in the world, let's get a connection and execute the query.
				 */
				this
					.getConnection()
					.then(this.$useConnection.bind(this))
					.then((connection) => {
						this.$maybeBufferConnection();

						return connection;
					})
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
		let config           = this.$connectionConfig,
			connectionConfig = new ConnectionConfig(config);

		connectionConfig.clientFlags   = config.clientFlags;
		connectionConfig.maxPacketSize = config.maxPacketSize;

		const connection = new PoolConnection(this, {
			config : connectionConfig
		});

		this.$add(this.$connections, connection);

		return connection;
	}

	$connectConnection (connection) {
		return new Promise((resolve, reject) => {
			this.$add(this.$busyConnections, connection);

			connection.connect(
				{
					timeout : this.acquireTimeout
				},
				(error) => {
					this.$releaseConnection(connection);

					if (this.$closed) {
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
		this.$closed = true;

		this
			.$clear(this.$connections)
			.$clear(this.$busyConnections)
			.$clear(this.$freeConnections)
			.$clear(this.$queryQueue);

		if (this.$scaleInterval) {
			clearInterval(this.$scaleInterval);
		}

		this.$connectionConfig    =
			this.$connections     =
			this.$busyConnections =
			this.$freeConnections =
			this.$queryQueue      =
			this.$scaleInterval   =
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

	$destroyConnection (connection) {
		return connection.destroy();
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
				this.$releaseConnection(connection);

				if (error) {
					reject(error);
				} else {
					resolve(results);
				}
			});

			connection.$lastQuery = new Date().getTime();

			connection.query(query);
		});
	}

	$releaseConnection (connection) {
		if (!this.$closed) {
			this.$remove(this.$busyConnections, connection)
				.$add(this.$freeConnections, connection);

			if (this.$freeConnections.size > 0 && this.$queryQueue.size) {
				const item = this.$first(this.$queryQueue);

				this
					.query(item.sql, item.values)
					.then(item.resolve, item.reject);
			}
		}

		return connection;
	}

	$useConnection (connection) {
		if (!this.$closed) {
			this.$remove(this.$freeConnections, connection)
				.$add(this.$busyConnections, connection);
		}

		return connection;
	}

	$removeConnection (connection) {
		if (!this.$closed) {
			this.$remove(this.$busyConnections, connection)
				.$remove(this.$freeConnections, connection)
				.$remove(this.$connections,     connection);
		}

		return connection;
	}

	$maybeBufferConnection () {
		let buffer = this.connectionBuffer;

		if (buffer && this.$connections.size  < this.maxConnectionLimit && this.$freeConnections.size < buffer) {
			buffer = this.$connections.size + buffer > this.maxConnectionLimit ?
				this.maxConnectionLimit - this.$connections.size : // buffer would go over the maxConnectionLimit
				this.$freeConnections.size ?
					buffer - this.$freeConnections.size : // we have some freeConnections, subtract from buffer
					buffer;

			if (buffer > 0) {
				const promises = [];

				for (let i = 0; i < buffer; i++) {
					promises.push(this.getConnection(true));
				}

				Promise.all(promises).catch(() => {});
			}
		}
	}

	$onScaleInterval () {
		const decay = this.connectionDecay;

		if (decay) {
			const purgable = [];
			const now      = new Date().getTime();

			this.$freeConnections.forEach(connection => {
				const lastQuery = connection.$lastQuery;

				if (!lastQuery || now - lastQuery >= decay) {
					purgable.push(connection);
				}
			});

			const num = purgable.length;

			if (num) {
				if (this.$connections.size - num < this.minConnectionLimit) {
					/**
					 * This would have caused the number of connections to be
					 * below the minConnectionLimit. We need to purge only
					 * the number of connections to get us to the minConnectionLimit.
					 */
					const newNum = this.$connections.size - this.minConnectionLimit;

					purgable.sort((a, b) => a.$lastQuery - b.$lastQuery);

					purgable.splice(
						newNum,
						purgable.length - newNum
					);
				}

				if (purgable.length) {
					purgable.forEach((connection) => {
						this.$removeConnection(connection);

						connection.destroy();
					});
				}
			}
		}
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
