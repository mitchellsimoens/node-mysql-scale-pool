# node-mysql-scale-pool

The purpose of this module is to add additional abilities to the Pool. These
abilities center around scaling number of open connections based on use.

Much like server instances can be scaled up and down based on certain parameters
(say amount of network traffic) in order to handle the increase workload, the
number of connections in a pool could scale up and down as queries are executed.

You may be thinking, "Who cares?" Having unnecessary sockets open to the database
is "bad" just like having servers always running, it's unnecessary. During peak
hours, allowing number of connections to the database scale up and back down during
off-peak hours is desirable.

## Usage

Using the pool with the native [mysql][] module, you would create a pool like this:

    const mysql = require('mysql');
    const pool  = mysql.createPool({
        connectionLimit : 10,
        host            : 'example.org',
        user            : 'bob',
        password        : 'secret',
        database        : 'my_db'
    });

With this pool, it really just as simple:

    const { Pool } = require('mysql-scale-pool');

    const pool = new Pool({
        connectionConfig : {
            connectionLimit : 10,
            host            : 'example.org',
            user            : 'bob',
            password        : 'secret',
            database        : 'my_db'
        }
    });

The slight change is to move the connection configurations into the `connectionConfig`
object. This is because this pool has additional configurations to control the scaling
of connections. Here are the additional configurations:

- `bufferOnConstruct` Automatically buffer connections when the pool is constructed.
- `connectionBuffer` The number of connections to have available for queries.
- `connectionDecay` The number of milliseconds from the last time a connection has been queried until it will be deemed stale.
- `maxConnectionLimit` The maximum number of connections that can be created.
- `minConnectionLimit` The minimum number of connections that will be created (only applicable during down scaling).
- `queueLimit` The maximum number of queries that can be queued waiting on a connection to become free.
- `scaleInterval` The frequency (in milliseconds) connection decay will be checked.

## Scaling

Connection handling in the native pool is primitive. It will create connections
and hold onto them but that's it. However, controlling the number of connections
that are connected based on usage can be a great feature for many reasons.

This pool attempts to control the connections by scaling the number of connections
up and down based on when queries are being executed. When queries are being executed
frequently, we need to scale up the number of connections until we reach a limit (the
`maxConnectionLimit`). At an interval (`scaleInterval`), the pool checks when the last
query was executed on the connections and if over a certain decay time (`connectionDecay`)
then the connection is deemed unnecessary and closes the connection and discards.

## Buffering

Along with scaling, this pool attempts to have connections actively ready for further queries.
This is a measure within the scaling up abilities to have connections ready for a query.
When a query is being executed, this pool checks to see if it should create some connections to
get ready for subsequent querying. This can be controled via the `connectionBuffer` config. When
this pool is first constructed, by default it will create connections instead of waiting on a
query to be executed in order to have connections ready.

 [mysql]: https://www.npmjs.com/package/mysql "mysql"
