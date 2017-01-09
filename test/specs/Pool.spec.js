'use strict';

const chai   = require('chai');
const expect = chai.expect;

const Pool               = require('../mocks/Pool');
const PoolConnectionMock = require('../mocks/PoolConnection');

describe('Pool', function () {
    let instance;

    afterEach(function () {
        let temp = instance;

        instance = null;

        if (temp && !temp._closed) {
            return temp.end();
        }
    });

    describe('initialization', function () {
        it('should be a pool', function () {
            instance = new Pool();

            expect(instance.isPool).to.be.true;
        });

        it('should apply defaults', function () {
            instance = new Pool();

            expect(instance).to.have.property('acquireTimeout',     10000);
            expect(instance).to.have.property('maxConnectionLimit', 10);
            expect(instance).to.have.property('minConnectionLimit', 0);
            expect(instance).to.have.property('queueLimit',         0);

            expect(instance.connectionConfig).to.be.an('object');
            expect(instance.connectionConfig).to.be.empty;
        });

        it('should merge config and defaults', function () {
            instance = new Pool({
                maxConnectionLimit : 20,
                minConnectionLimit : 5
            });

            expect(instance).to.have.property('acquireTimeout',     10000);
            expect(instance).to.have.property('maxConnectionLimit', 20);
            expect(instance).to.have.property('minConnectionLimit', 5);
            expect(instance).to.have.property('queueLimit',         0);

            expect(instance.connectionConfig).to.be.an('object');
            expect(instance.connectionConfig).to.be.empty;
        });

        it('should create ConnectionConfig instance', function () {
            instance = new Pool();

            expect(instance._connectionConfig).to.not.be.undefined;
        });

        it('should create connection arrays', function () {
            instance = new Pool();

            expect(instance._connections).to.be.a('set');
            expect(instance._connections).to.be.empty;

            expect(instance._busyConnections).to.be.a('set');
            expect(instance._busyConnections).to.be.empty;

            expect(instance._freeConnections).to.be.a('set');
            expect(instance._freeConnections).to.be.empty;
        });
    });

    describe('end', function () {
        it('should cleanup properties', function () {
            instance = new Pool();

            return instance
                .end()
                .then(() => {
                    expect(instance._closed).to.be.true;
                    expect(instance._connectionConfig).to.be.null;
                    expect(instance._connections).to.be.null;
                    expect(instance._busyConnections).to.be.null;
                    expect(instance._freeConnections).to.be.null;
                    expect(instance._queryQueue).to.be.null;
                });
        });

        it('should remove connections', function () {
            instance = new Pool();

            const mock = new PoolConnectionMock(instance);

            const connections     = instance._connections;
            const freeConnections = instance._freeConnections;

            instance.$add(connections,     mock);
            instance.$add(freeConnections, mock);

            return instance
                .end()
                .then(() => {
                    expect(connections.has(mock)).to.be.false;
                    expect(freeConnections.has(mock)).to.be.false;
                });
        });
    });

    describe('destroy', function () {
        it('should cleanup properties', function () {
            instance = new Pool();

            return instance
                .destroy()
                .then(() => {
                    expect(instance._closed).to.be.true;
                    expect(instance._connectionConfig).to.be.null;
                    expect(instance._connections).to.be.null;
                    expect(instance._busyConnections).to.be.null;
                    expect(instance._freeConnections).to.be.null;
                    expect(instance._queryQueue).to.be.null;
                });
        });

        it('should remove connections', function () {
            instance = new Pool();

            const mock = new PoolConnectionMock(instance);

            const connections     = instance._connections;
            const freeConnections = instance._freeConnections;

            instance.$add(connections,     mock);
            instance.$add(freeConnections, mock);

            return instance
                .destroy()
                .then(() => {
                    expect(connections.has(mock)).to.be.false;
                    expect(freeConnections.has(mock)).to.be.false;
                });
        });
    });

    describe('getConnection', function () {
        it('should create a new connection', function * () {
            instance = new Pool();

            const stub       = this.sandbox.stub(instance, '$connectConnection').resolves(new PoolConnectionMock(instance));
            const connection = yield instance.getConnection();

            expect(connection).to.be.ok;
            expect(stub).to.be.called;
        });

        it('should handle when a pool is closed', function () {
            instance = new Pool();

            instance._closed = true;

            const promise = instance.getConnection();

            return promise.catch(error => {
                expect(error).to.be.an('error');
            });
        });

        it('should get a free connection', function * () {
            instance = new Pool();

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance._connections,     mock);
            instance.$add(instance._freeConnections, mock);

            const stub       = this.sandbox.stub(instance, '$connectConnection').resolves(new PoolConnectionMock(instance));
            const connection = yield instance.getConnection();

            expect(connection).to.be.ok;
            expect(stub).to.not.be.called;
        });

        it('should return error if maxConnectionLimit is hit', function () {
            instance = new Pool({
                maxConnectionLimit : 1
            });

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance._connections,     mock);
            instance.$add(instance._busyConnections, mock);

            const promise = instance.getConnection();

            return promise.catch(error => {
                expect(error).to.be.an('error');
            });
        });
    });

    describe('query', function () {
        it('should create a new connection', function () {
            instance = new Pool();

            return instance
                .query('SELECT 1;')
                .then(() => {
                    expect(instance._connections.size).to.be.above(0);
                    expect(instance._freeConnections.size).to.be.above(0);
                });
        });

        it('should mark connection as busy', function () {
            instance = new Pool();

            const promise = instance.query('SELECT 1;');

            expect(instance._connections.size).to.be.equal(1);
            expect(instance._busyConnections.size).to.be.equal(1);
            expect(instance._freeConnections.size).to.be.equal(0);

            return promise;
        });

        it('should return db result', function * () {
            instance = new Pool();

            const mock   = this.sandbox.stub(instance, '$query').resolves([{}]);
            const result = yield instance.query('SELECT 1;');

            expect(mock).to.be.calledOnce;
            expect(result).to.be.an('array');
            expect(result).to.not.be.empty;
            expect(result[0]).to.be.an('object');
        });

        it('should return an error', function () {
            instance = new Pool();

            const mock = this.sandbox.stub(instance, '$query').rejects(new Error('foo'));

            return instance
                .query('SELECT 1;')
                .catch(error => {
                    expect(mock).to.be.calledOnce;
                    expect(error).to.be.an('error');
                });
        });

        it('should queue the query', function () {
            instance = new Pool({
                maxConnectionLimit : 2
            });

            const promises = [];

            promises.push(instance.query('SELECT 1;'));
            promises.push(instance.query('SELECT 2;'));
            promises.push(instance.query('SELECT 3;'));

            expect(instance._queryQueue.size).to.be.equal(1);

            return Promise.all(promises);
        });

        it('should throw an error if queue is full', function () {
            instance = new Pool({
                queueLimit         : 1,
                maxConnectionLimit : 1
            });

            const promises = [];

            promises.push(instance.query('SELECT 1;'));
            promises.push(instance.query('SELECT 2;'));
            promises.push(instance.query('SELECT 3;'));

            expect(instance._queryQueue.size).to.be.equal(1);

            return Promise
                .all(promises)
                .catch(error => {
                    expect(error).to.be.an('error');
                });
        });
    });

    describe('releaseConnection', function () {
        it('should release the connection', function () {
            instance = new Pool();

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance._connections,     mock);
            instance.$add(instance._freeConnections, mock);

            return instance
                .releaseConnection(mock)
                .then(() => {
                    expect(instance._connections.size).to.be.equal(1);
                    expect(instance._busyConnections.size).to.be.equal(0);
                    expect(instance._freeConnections.size).to.be.equal(1);
                });
        });
    });

    describe('_purgeConnection', function () {
        it('should purge the connection', function () {
            instance = new Pool();

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance._connections,     mock);
            instance.$add(instance._freeConnections, mock);

            return instance
                ._purgeConnection(mock)
                .then(() => {
                    expect(instance._connections.size).to.be.equal(0);
                    expect(instance._busyConnections.size).to.be.equal(0);
                    expect(instance._freeConnections.size).to.be.equal(0);
                });
        });
    });
});
