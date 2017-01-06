# node-mysql-scale-pool

The purpose of this module is to add additional abilities to the Pool. These
abilities center around scaling number of open connections based on use.

Much like server instances can be scaled up and down based on certain parameters
(say amount of network traffic) in order to handle the increase workload, the
number of connections in a pool could scale up and down as queries are executed.

You may be thinking, "Who cares?" Having unnecessary sockets open to the database
is "bad" just like having servers always running, it's unnecessary.
