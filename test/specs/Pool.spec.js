'use strict';

const chai   = require('chai');
const expect = chai.expect;

const PoolMock           = require('../mocks/Pool');
const PoolConnectionMock = require('../mocks/PoolConnection');

describe('Pool', function () {
    let instance;

    afterEach(function () {
        let temp = instance;

        instance = null;

        if (temp && !temp.$closed) {
            return temp.destroy();
        }
    });

    describe('initialization', function () {
        it('should be a pool', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            expect(instance.isPool).to.be.true;
        });

        it('should apply defaults', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            expect(instance).to.have.property('acquireTimeout',     10000);
            expect(instance).to.have.property('maxConnectionLimit', 10);
            expect(instance).to.have.property('minConnectionLimit', 0);
            expect(instance).to.have.property('queueLimit',         Infinity);

            expect(instance.connectionConfig).to.be.an('object');
            expect(instance.connectionConfig).to.be.empty;
        });

        it('should merge config and defaults', function () {
            instance = new PoolMock({
                bufferOnConstruct  : false,
                maxConnectionLimit : 20,
                minConnectionLimit : 5
            });

            expect(instance).to.have.property('acquireTimeout',     10000);
            expect(instance).to.have.property('maxConnectionLimit', 20);
            expect(instance).to.have.property('minConnectionLimit', 5);
            expect(instance).to.have.property('queueLimit',         Infinity);

            expect(instance.connectionConfig).to.be.an('object');
            expect(instance.connectionConfig).to.be.empty;
        });

        it('should create ConnectionConfig instance', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            expect(instance.$connectionConfig).to.not.be.undefined;
        });

        it('should create connection arrays', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            expect(instance.$connections).to.be.a('set');
            expect(instance.$connections).to.be.empty;

            expect(instance.$busyConnections).to.be.a('set');
            expect(instance.$busyConnections).to.be.empty;

            expect(instance.$freeConnections).to.be.a('set');
            expect(instance.$freeConnections).to.be.empty;
        });
    });

    describe('end', function () {
        it('should cleanup properties', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            return instance
                .end()
                .then(() => {
                    expect(instance.$closed).to.be.true;
                    expect(instance.$connectionConfig).to.be.null;
                    expect(instance.$connections).to.be.null;
                    expect(instance.$busyConnections).to.be.null;
                    expect(instance.$freeConnections).to.be.null;
                    expect(instance.$queryQueue).to.be.null;
                });
        });

        it('should remove connections', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            const mock = new PoolConnectionMock(instance);

            const connections     = instance.$connections;
            const freeConnections = instance.$freeConnections;

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
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            return instance
                .destroy()
                .then(() => {
                    expect(instance.$closed).to.be.true;
                    expect(instance.$connectionConfig).to.be.null;
                    expect(instance.$connections).to.be.null;
                    expect(instance.$busyConnections).to.be.null;
                    expect(instance.$freeConnections).to.be.null;
                    expect(instance.$queryQueue).to.be.null;
                    expect(instance.$scaleInterval).to.be.null;
                });
        });

        it('should remove connections', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            const mock = new PoolConnectionMock(instance);

            const connections     = instance.$connections;
            const freeConnections = instance.$freeConnections;

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
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            const stub       = this.sandbox.stub(instance, '$connectConnection').resolves(new PoolConnectionMock(instance));
            const connection = yield instance.getConnection();

            expect(connection).to.be.ok;
            expect(stub).to.be.called;
        });

        it('should handle when a pool is closed', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            instance.$closed = true;

            const promise = instance.getConnection();

            return promise.catch(error => {
                expect(error).to.be.an('error');
            });
        });

        it('should get a free connection', function * () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance.$connections,     mock);
            instance.$add(instance.$freeConnections, mock);

            const stub       = this.sandbox.stub(instance, '$connectConnection').resolves(new PoolConnectionMock(instance));
            const connection = yield instance.getConnection();

            expect(connection).to.be.ok;
            expect(stub).to.not.be.called;
        });

        it('should return error if maxConnectionLimit is hit', function () {
            instance = new PoolMock({
                bufferOnConstruct  : false,
                maxConnectionLimit : 1
            });

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance.$connections,     mock);
            instance.$add(instance.$busyConnections, mock);

            const promise = instance.getConnection();

            return promise.catch(error => {
                expect(error).to.be.an('error');
            });
        });
    });

    describe('query', function () {
        it('should create a new connection', function () {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionBuffer  : 0
            });

            return instance
                .query('SELECT 1;')
                .then(() => {
                    expect(instance.$connections.size).to.be.equal(1);
                    expect(instance.$busyConnections.size).to.be.equal(0);
                    expect(instance.$freeConnections.size).to.be.equal(1);
                });
        });

        it('should mark connection as busy', function () {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionBuffer  : 0
            });

            const promise = instance.query('SELECT 1;');

            expect(instance.$connections.size).to.be.equal(1);
            expect(instance.$busyConnections.size).to.be.equal(1);
            expect(instance.$freeConnections.size).to.be.equal(0);

            return promise;
        });

        it('should return db result', function * () {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionBuffer  : 0
            });

            const mock   = this.sandbox.stub(instance, '$query').resolves([{}]);
            const result = yield instance.query('SELECT 1;');

            expect(mock).to.be.calledOnce;
            expect(result).to.be.an('array');
            expect(result).to.not.be.empty;
            expect(result[0]).to.be.an('object');
        });

        it('should return an error', function () {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionBuffer  : 0
            });

            const mock = this.sandbox.stub(instance, '$query').rejects(new Error('foo'));

            return instance
                .query('SELECT 1;')
                .catch(error => {
                    expect(mock).to.be.calledOnce;
                    expect(error).to.be.an('error');
                });
        });

        it('should queue the query', function () {
            instance = new PoolMock({
                bufferOnConstruct  : false,
                connectionBuffer   : 0,
                maxConnectionLimit : 2
            });

            const promises = [];

            promises.push(instance.query('SELECT 1;'));
            promises.push(instance.query('SELECT 2;'));
            promises.push(instance.query('SELECT 3;'));

            expect(instance.$queryQueue.size).to.be.equal(1);

            return Promise.all(promises);
        });

        it('should throw an error if queue is full', function () {
            instance = new PoolMock({
                bufferOnConstruct  : false,
                connectionBuffer   : 0,
                maxConnectionLimit : 1,
                queueLimit         : 1
            });

            const promises = [];

            promises.push(instance.query('SELECT 1;'));
            promises.push(instance.query('SELECT 2;'));
            promises.push(instance.query('SELECT 3;'));

            expect(instance.$queryQueue.size).to.be.equal(1);

            return Promise
                .all(promises)
                .catch(error => {
                    expect(error).to.be.an('error');
                });
        });
    });

    describe('releaseConnection', function () {
        it('should release the connection', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance.$connections,     mock);
            instance.$add(instance.$freeConnections, mock);

            return instance
                .releaseConnection(mock)
                .then(() => {
                    expect(instance.$connections.size).to.be.equal(1);
                    expect(instance.$busyConnections.size).to.be.equal(0);
                    expect(instance.$freeConnections.size).to.be.equal(1);
                });
        });
    });

    describe('_purgeConnection', function () {
        it('should purge the connection', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance.$connections,     mock);
            instance.$add(instance.$freeConnections, mock);

            return instance
                ._purgeConnection(mock)
                .then(() => {
                    expect(instance.$connections.size).to.be.equal(0);
                    expect(instance.$busyConnections.size).to.be.equal(0);
                    expect(instance.$freeConnections.size).to.be.equal(0);
                });
        });
    });

    describe('$onScaleInterval', function () {
        it('should scale down connection has not been queried', function (done) {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionDecay   : 10,
                scaleInterval     : 20
            });

            const mock = new PoolConnectionMock(instance);

            instance.$add(instance.$connections,     mock);
            instance.$add(instance.$freeConnections, mock);

            expect(instance.$connections.size).to.be.equal(1);
            expect(instance.$freeConnections.size).to.be.equal(1);

            setTimeout(() => {
                expect(instance.$connections.size).to.be.equal(0);
                expect(instance.$busyConnections.size).to.be.equal(0);
                expect(instance.$freeConnections.size).to.be.equal(0);

                done();
            }, 30);
        });

        it('should scale down connection that has been queried', function (done) {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionDecay   : 10,
                scaleInterval     : 20
            });

            const mock = new PoolConnectionMock(instance);

            mock.$lastQuery = new Date().getTime();

            instance.$add(instance.$connections,     mock);
            instance.$add(instance.$freeConnections, mock);

            expect(instance.$connections.size).to.be.equal(1);
            expect(instance.$freeConnections.size).to.be.equal(1);

            setTimeout(() => {
                expect(instance.$connections.size).to.be.equal(0);
                expect(instance.$busyConnections.size).to.be.equal(0);
                expect(instance.$freeConnections.size).to.be.equal(0);

                done();
            }, 30);
        });

        it('should not scale down connection', function (done) {
            instance = new PoolMock({
                bufferOnConstruct : false,
                connectionDecay   : 10,
                scaleInterval     : 20
            });

            const mock1 = new PoolConnectionMock(instance);
            const mock2 = new PoolConnectionMock(instance);

            mock1.$lastQuery = new Date().getTime();
            mock2.$lastQuery = new Date().getTime() + 20;

            instance.$add(instance.$connections,     mock1);
            instance.$add(instance.$freeConnections, mock1);
            instance.$add(instance.$connections,     mock2);
            instance.$add(instance.$freeConnections, mock2);

            expect(instance.$connections.size).to.be.equal(2);
            expect(instance.$freeConnections.size).to.be.equal(2);

            setTimeout(() => {
                expect(instance.$connections.size).to.be.equal(1);
                expect(instance.$busyConnections.size).to.be.equal(0);
                expect(instance.$freeConnections.size).to.be.equal(1);

                done();
            }, 30);
        });
    });

    describe.only('$maybeBufferConnection', function () {
        it('should buffer connections on construction', function (done) {
            instance = new PoolMock();

            setTimeout(() => {
                //hack to allow the connections to connect
                expect(instance.$connections.size).to.be.equal(5);
                expect(instance.$busyConnections.size).to.be.equal(0);
                expect(instance.$freeConnections.size).to.be.equal(5);

                done();
            }, 0);
        });

        it('should buffer 3 connections on construction', function (done) {
            instance = new PoolMock({
                connectionBuffer : 3
            });

            setTimeout(() => {
                //hack to allow the connections to connect
                expect(instance.$connections.size).to.be.equal(3);
                expect(instance.$busyConnections.size).to.be.equal(0);
                expect(instance.$freeConnections.size).to.be.equal(3);

                done();
            }, 0);
        });

        it('should not buffer connections on construction', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            expect(instance.$connections.size).to.be.equal(0);
            expect(instance.$busyConnections.size).to.be.equal(0);
            expect(instance.$freeConnections.size).to.be.equal(0);
        });

        it('should buffer connections on query', function () {
            instance = new PoolMock({
                bufferOnConstruct : false
            });

            return instance
                .query('SELECT 1;')
                .then(() => {
                    /**
                     * 6 connections should be created. 1 for the query
                     * and 5 to be the buffer number due to connectionBuffer
                     * config.
                     */
                    expect(instance.$connections.size).to.be.equal(6);
                    expect(instance.$busyConnections.size).to.be.equal(0);
                    expect(instance.$freeConnections.size).to.be.equal(6);
                });
        });
    });
});
