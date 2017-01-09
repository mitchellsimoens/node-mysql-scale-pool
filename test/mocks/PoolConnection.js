'use strict';

class PoolConnectionMock {
    constructor (pool) {
        if (!pool) {
            throw new Error('This mock PoolConnection requires a pool to be passed');
        }

        this._pool = pool;
    }

    end () {
        this.release();
    }

    destroy () {
        this._pool._purgeConnection(this);
    }

    release () {
        return this._pool.releaseConnection(this);
    }

    query (query) {
        setTimeout(() => query._callback(null, []), 0);
    }
}

module.exports = PoolConnectionMock;
