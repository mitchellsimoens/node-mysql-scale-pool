'use strict';

const Connection       = require('mysql/lib/Connection');
const ConnectionConfig = require('mysql/lib/ConnectionConfig');

const configDefaults = {
	/**
	 * @cfg {Number} [acquireTimeout=10000] The number of milliseconds
	 * for a connection to connect to the database till it timesout.
	 */
	acquireTimeout     : 10000, // 10 seconds
	/**
	 * @cfg {Boolean} [bufferOnConstruct=true] Whether connections should
	 * be buffered when the pool is constructing.
	 *
	 * This would allow connections to be ready and connected when queries
	 * are executing instead of connecting when a query executes.
	 *
	 * Please see {@link #connectionBuffer} for the number of connections
	 * that will be created.
	 */
	bufferOnConstruct  : true,
	/**
	 * @cfg {Number} [connectionBuffer=5] The number of connections to have
	 * buffered to be available for a query.
	 */
	connectionBuffer   : 5,
	/**
	 * @cfg {PoolConnection} [connectionClass=PoolConnection] The connection
	 * class to use to create new connections with.
	 */
	connectionClass    : require('mysql/lib/PoolConnection'),
	/**
	 * @Cfg {Object} [connectionConfig={}] The connection configurations
	 * passed to the PoolConnection. For valid options, please see the
	 * [mysql](https://www.npmjs.com/package/mysql#connection-options) module.
	 */
	connectionConfig   : {},
	/**
	 * @cfg {Number} [connectionDecay=300000] The number of milliseconds since
	 * the last query a connection has executed when the number of connections
	 * is scaling down. If a query has not executed within this period, it is deemed
	 * unnecessary and will be closed.
	 *
	 * Please see {@link #scaleInterval} for the interval this will be checked.
	 */
	connectionDecay    : 300000, // 5 minutes
	/**
	 * @cfg {Number} [maxConnectionLimit=10] The number of connections to be the
	 * max number of connections that can be created. Buffering or querying will
	 * never create a connection once this limit has been reached. If no limit is
	 * wanted (not recommended), this can be set to `Infinity`.
	 */
	maxConnectionLimit : 10,
	/**
	 * @cfg {Number} [minConnectionLimit=0] The minimum umber of connections that
	 * should be created. This is only used when connections are being scaled down.
	 */
	minConnectionLimit : 0,
	/**
	 * @cfg {Number} [queueLimit=Infinity] The maximum number of queries that can be queued.
	 */
	queueLimit         : Infinity,
	/**
	 * @cfg {Number} [scaleInterval=300000] The number of milliseconds to check the
	 * number of connections in order to scale down connections that have not been
	 * queried for a while.
	 *
	 * Please see {@link #connectionDecay} for the timeframe a connection is deemed
	 * unnecessary.
	 */
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

	/**
	 * Gracefully closes and removes all connections from this pool.
	 *
	 * For non-graceful closing, please see the {@link #destroy} method.
	 *
	 * @returns {Promise}
	 */
	end () {
		let promises = [];

		for (let connection of this.$connections) {
			promises.push(this.$endConnection(connection));
		}

		return Promise
			.all(promises)
			.then(this.$onEnd.bind(this));
	}

	/**
	 * Forces all connections to be closed from this pool.
	 *
	 * For graceful closing, please see the {@link #end} method.
	 *
	 * @returns {Promise}
	 */
	destroy () {
		let promises = [];

		for (let connection of this.$connections) {
			promises.push(this.$destroyConnection(connection));
		}

		return Promise
			.all(promises)
			.then(this.$onEnd.bind(this));
	}

	/**
	 * Returns a connection that is currently free of any queries.
	 * If no connections are free and the {@link #maxConnectionLimit}
	 * has not been reached, a connection will be created and connected to.
	 *
	 * @returns {Promise}
	 */
	getConnection () {
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

	/**
	 * @param {String} sql The SQL statement to run.
	 * @param {Array} values The values to replace in the placeholders
	 * in the SQL statement.
	 *
	 * See [Performing Queries](https://www.npmjs.com/package/mysql#performing-queries)
	 * for more on querying.
	 *
	 * @returns {Promise}
	 */
	query (sql, values) {
		return new Promise((resolve, reject) => {
			if (this.$closed) {
				reject(new Error('This pool is closed'));
			} else if (this.$freeConnections.size === 0 && this.$connections.size >= this.maxConnectionLimit) {
				/**
				 * We have no free connections to work with and have reached the
				 * maxConnectionLimit so we have to see if we can queue the query.
				 */
				if (this.$queryQueue.size < this.queueLimit) {
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

	/**
	 * @private
	 * Release a connection. This is executed when a connection is ended.
	 *
	 * @param {PoolConnection} connection The connection that has been ended.
	 *
	 * @returns {Promise}
	 */
	releaseConnection (connection) {
		return new Promise(resolve => resolve(this.$releaseConnection(connection)));
	}

	/**
	 * @private
	 * Removes a connection when it has been destroyed.
	 *
	 * @param {PoolConnection} connection The connection that has been ended.
	 *
	 * @returns {Promise}
	 */
	_purgeConnection (connection) {
		return new Promise(resolve => resolve(this.$removeConnection(connection)));
	}

	/**
	 * @private
	 * Create a connection using {@link #connectionConfig}.
	 *
	 * @returns {PoolConnection}
	 */
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

	/**
	 * @private
	 * Triggers a connection to open a socket to the database.
	 *
	 * @returns {PoolConnection}
	 */
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

	/**
	 * Handler to cleanup when a pool has been ended either from the
	 * {@link #end} or {@link #destroy} methods from being executed.
	 *
	 * @param {*} arg Any argument possibly from a promise.
	 *
	 * @returns {*} The argument that was passed to this method.
	 */
	$onEnd (arg) {
		this.$closed = true;

		this
			.$clear(this.$connections)
			.$clear(this.$busyConnections)
			.$clear(this.$freeConnections)
			.$clear(this.$queryQueue);

		this.$scaleInterval && clearInterval(this.$scaleInterval);

		this.$connectionConfig    =
			this.$connections     =
			this.$busyConnections =
			this.$freeConnections =
			this.$queryQueue      =
			this.$scaleInterval   =
			this.connectionClass  =
			null;

		return arg;
	}

	/**
	 * @private
	 * Gracefully releases and then destroys the connection.
	 *
	 * @returns {Promise}
	 */
	$endConnection (connection) {
		return connection
			.release()
			.then(connection => {
				connection.destroy();

				return connection;
			});
	}

	/**
	 * @private
	 * Non-gracefully destroys a connection.
	 *
	 * @returns {Promise}
	 */
	$destroyConnection (connection) {
		return connection.destroy();
	}

	/**
	 * @private
	 * Creates the `Query` instance that will be executed by the `PoolConnection`.
	 *
	 * @param {String} sql The SQL statement to run.
	 * @param {Array} values The values to replace in the placeholders
	 * in the SQL statement.
	 * @param {Function} callback The callback function when the query has been executed.
	 *
	 * @returns {Query}
	 */
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

	/**
	 * @private
	 * Do the actual querying.
	 *
	 * @param {PoolConnection} connection The connection that will execute the query.
	 * @param {String} sql The SQL statement to run.
	 * @param {Array} values The values to replace in the placeholders
	 * in the SQL statement.
	 *
	 * @returns {Promise}
	 */
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

	/**
	 * Releases a connection from being busy to being free.
	 *
	 * This will also check if there is a queued query.
	 *
	 * @param {PoolConnection} connection
	 * @returns {PoolConnection}
	 */
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

	/**
	 * @private
	 * Marks a connection as being busy either from a query or
	 * the connection is connecting.
	 *
	 * @param {PoolConnection} connection
	 * @returns {PoolConnection}
	 */
	$useConnection (connection) {
		if (!this.$closed) {
			this.$remove(this.$freeConnections, connection)
				.$add(this.$busyConnections, connection);
		}

		return connection;
	}

	/**
	 * @private
	 * Removes a connection from this pool.
	 *
	 * @param {PoolConnection} connection
	 * @returns {PoolConnection}
	 */
	$removeConnection (connection) {
		if (!this.$closed) {
			this.$remove(this.$busyConnections, connection)
				.$remove(this.$freeConnections, connection)
				.$remove(this.$connections,     connection);
		}

		return connection;
	}

	/**
	 * @private
	 * Determines if connections can be made to be buffered. This means
	 * connections will be free for the next query execution.
	 *
	 * The number of connections will be determined from the {@link #connectionBuffer}
	 * config along with making sure the number of connections does not go above
	 * the {@link #maxConnectionLimit} and checking the number of connections
	 * already free.
	 */
	$maybeBufferConnection () {
		let buffer = this.connectionBuffer;

		if (buffer && this.$connections.size  < this.maxConnectionLimit && this.$freeConnections.size < buffer) {
			buffer = this.$connections.size + buffer > this.maxConnectionLimit ?
				this.maxConnectionLimit - this.$connections.size : // buffer would go over the maxConnectionLimit
				this.$freeConnections.size ?
					buffer - this.$freeConnections.size : // we have some freeConnections, subtract from buffer
					buffer;

			/**
			 * TODO
			 * the issue here is if there are multiple requests from a client
			 * app, connections being created for this buffering will be busy
			 * and so a 2nd request will also attempt to buffer connections
			 * until the total number of connections hits the maxConnectionLimit.
			 * Connections being buffered needs to be tracked separately and
			 * be used in the above determination on the actual number of connections
			 * to be buffered.
			 */

			const promises = [];

			for (let i = 0; i < buffer; i++) {
				promises.push(this.getConnection(true));
			}

			/**
			 * Capture any connection rejections in the case a connection
			 * could not connect to the database. We could turn around and
			 * execute this $maybeBufferConnection method to try again, however,
			 * this could end up in an endless loop if a database is down.
			 */
			Promise.all(promises).catch(() => {});
		}
	}

	/**
	 * @private
	 * Check to see if any connections are old and will be deemed unnecessary
	 * due to the last time a query was executed on the connection and the
	 * {@link #connectionDecay} timeframe. This allows for scaling down.
	 *
	 * The number of connections that will be scaled down will not go below
	 * the {@link minConnectionLimit} even if the number of connections is
	 * under that limit.
	 */
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
		} else {
			this.$scaleInterval && clearInterval(this.$scaleInterval);

			this.$scaleInterval = null;
		}
	}

	/**
	 * @private
	 * Gets the first connection from the set and optionally removes
	 * it from the set. This is like `Array.prototype.shift` but since
	 * we use `Set`, this acts as a convenience method.
	 *
	 * @param {Set} set The set to get the first connection from.
	 * @param {Boolean} [remove=true] Whether to remove the conection
	 * from the set.
	 * @returns {PoolConnection}
	 */
	$first (set, remove = true) {
		const values = set.values();
        const item   = values.next();

		if (item) {
			if (remove) {
				set.delete(item);
			}

			return item.value;
		}
	}

	/**
	 * @private
	 * Adds a connection to the set.
	 *
	 * Can optionally skip checking if the set has the connection
	 * if the calling method knows the connection is in the set.
	 * This check is done to skip re-adding a connection to the set
	 * for performance.
	 *
	 * @param {Set} set The set to add the connection to.
	 * @param {PoolConnection} connection The connection to add.
	 * @param {Boolean} [skipCheck=false] If the connection being in the
	 * set is known, skipping the check if the set has the connection
	 * can yield a bit faster performance.
	 * @returns {Pool}
	 */
	$add (set, connection, skipCheck = false) {
		if (skipCheck === true || !set.has(connection)) {
			set.add(connection);
		}

		return this;
	}

	/**
	 * @private
	 * Clears the set.
	 *
	 * @param {Set} set The set to clear.
	 * @param {Boolean} [skipCheck=false] Can skip the check if
	 * the set has connections added to it.
	 * @returns {Pool}
	 */
	$clear (set, skipCheck = false) {
		if (skipCheck === true || set.size) {
			set.clear();
		}

		return this;
	}

	/**
	 * @private
	 * Removes a connection from a set.
	 *
	 * Can optionally skip checking if the set has the connection
	 * if the calling method knows the connection is in the set.
	 * This check is done to skip removing a connection when the
	 * connection has not been added to the set.
	 *
	 * @param {Set} set The set to remove the connection from.
	 * @param {PoolConnection} connection The connection to remove.
	 * @param {Boolean} [skipCheck=false] If the connection being in the
	 * set is known, skipping the check if the set has the connection
	 * can yield a bit faster performance.
	 * @returns {Pool}
	 */
	$remove (set, connection, skipCheck = false) {
		if (skipCheck === true || set.has(connection)) {
			set.delete(connection);
		}

		return this;
	}
}

module.exports = Pool;
