# aerospike_redis_php

This a drop in replacement for the Redis module, using Aerospike as Database.

# Installation

Register the redis.lua module into Aerospike

````
 register module 'redis.lua'
````

You must have the [aerospike php module](https://github.com/aerospike/aerospike-client-php).

In your PHP code, instead of creating a Redis class, create an Aeropsike Redis class :
````php
  $config = array("hosts" => array(array("addr" => $host, "port" => 3000)));
  $db = new Aerospike($config, false);
  $r = new AerospikeRedis($db, "namespace", "set");
````
`$r` can be used as an Redis object.

````php
$r->get("toto");
`````

The namespace must exists in Aerospike config.

# Implemented functions

* get / set / setex
* del / delete
* ttl / setTimeout
* lpush / push / rpop / lpop
* lSize
* ltrim / lRange
* flushdb (using scan, poor performance)
* hSet / hGet / hDel (keys length must be < 14 chars, bin names limitation in aerospike)
* hmSet / hmGet
* hGetAll (key order is different from the redis implementation)

# Tests

[`test.php`](test.php) can be run against Redis or Aerospike.

Against Redis :
````
USE_REDIS=1 php tests.php
````

Against Aerospike :
````
php tests.php
````
or
````
HOST=my_aerospike_ip php tests.php
````
