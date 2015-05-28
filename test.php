<?php

require './aerospike_to_redis.php';

if (isset($_ENV['USE_REDIS'])) {
  echo "Using Redis !!!!\n";
  $r = new Redis();
}
else {
  $host = isset($_ENV['HOST']) ? ($_ENV['HOST']) : 'localhost';
  echo "Using Aerospike on " . $host . "\n";
  $config = array("hosts" => array(array("addr" => $host, "port" => 3000)));
  $db = new Aerospike($config, false);
  $r = new AerospikeRedis($db, "test", "redis");
}

function dump($a) {
  ob_start();
  var_dump($a);
  $aa = ob_get_contents();
  ob_clean();
  return trim($aa);
}
function compare($a, $b) {
  if ($a !== $b) {
    throw new Exception("Assert failed : <".dump($a)."> != <".dump($b).">");
  }
}

echo("Connect\n");
compare($r->connect('127.0.0.1', 6379), true);

echo("Get Set\n");

$r->del('myKey');

compare($r->get('myKey'), false);
compare($r->set('myKey', "a"), true);
compare($r->get('myKey'), "a");
compare($r->set('myKey', 12), true);
compare($r->get('myKey'), "12");
compare($r->set('myKey2', 13), true);
compare($r->get('myKey'), "12");
compare($r->get('myKey2'), "13");
compare($r->del('myKey'), 1);
compare($r->del('myKey'), 0);
compare($r->del('myKey2'), 1);
compare($r->get('myKey'), false);
compare($r->get('myKey2'), false);

echo("Get Set binary\n");

$r->del('myKey');

compare($r->get('myKey'), false);
compare($r->set('myKey', "toto\r\ntiti"), true);
compare($r->get('myKey'), "toto\r\ntiti");

compare($r->set('myKey', "toto\x00\x01\x02tata"), true);
compare($r->get('myKey'), "toto\x00\x01\x02tata");

$json = file_get_contents('big_json.json');

echo("Get Set big data " . strlen($json)."\n");

$r->del('myKey');
compare($r->get('myKey'), false);
compare($r->set('myKey', $json), true);
compare($r->get('myKey'), $json);
compare($r->del('myKey'), 1);
compare($r->rpush('myKey', $json), 1);
compare($r->rpop('myKey'), $json);

$bin = gzcompress($json);

echo("Get Set big data binary " . strlen($bin)."\n");

$r->del('myKey');
compare($r->get('myKey'), false);
compare($r->set('myKey', $bin), true);
compare(gzuncompress($r->get('myKey')), $json);
compare($r->del('myKey'), 1);
compare($r->rpush('myKey', $bin), 1);
compare(gzuncompress($r->rpop('myKey')), $json);

echo("Flush\n");
compare($r->set('myKey1', "a"), true);
compare($r->set('myKey2', "b"), true);
compare($r->flushdb(), true);
compare($r->get('myKey1'), false);
compare($r->get('myKey2'), false);

echo("Array\n");

$r->del('myKey');
compare($r->rpop('myKey'), false);
compare($r->lpop('myKey'), false);
compare($r->lsize('myKey'), 0);
compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->rpush('myKey', 12), 4);
compare($r->lsize('myKey'), 4);
compare($r->rpop('myKey'), '12');
compare($r->lsize('myKey'), 3);
compare($r->rpush('myKey', 'e'), 4);
compare($r->lsize('myKey'), 4);
compare($r->rpop('myKey'), 'e');
compare($r->lsize('myKey'), 3);
compare($r->lpop('myKey'), 'a');
compare($r->lsize('myKey'), 2);
compare($r->rpop('myKey'), 'c');
compare($r->lsize('myKey'), 1);
compare($r->lpush('myKey', 'z'), 2);
compare($r->lsize('myKey'), 2);
compare($r->rpop('myKey'), 'b');
compare($r->lsize('myKey'), 1);
compare($r->rpop('myKey'), 'z');
compare($r->lsize('myKey'), 0);
compare($r->rpop('myKey'), false);

echo("Array Ltrim\n");

$r->del('myKey');
compare($r->rpush('myKey', 'a'), 1);
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', 0, 0), true);
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', 0, -1), true);
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', -1, -1), true);
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', -1, 0), true);
compare($r->lsize('myKey'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->ltrim('myKey', 0, 12), true);
compare($r->lsize('myKey'), 3);
compare($r->ltrim('myKey', 2, 2), true);
compare($r->rpop('myKey'), 'c');
compare($r->lsize('myKey'), 0);

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->ltrim('myKey', 0, -2), true);
compare($r->lsize('myKey'), 2);
compare($r->rpop('myKey'), 'b');
compare($r->rpop('myKey'), 'a');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->rpush('myKey', 'd'), 4);
compare($r->rpush('myKey', 'e'), 5);
compare($r->rpush('myKey', 'f'), 6);
compare($r->ltrim('myKey', 2, 4), true);
compare($r->lsize('myKey'), 3);
compare($r->lpop('myKey'), 'c');
compare($r->lpop('myKey'), 'd');
compare($r->lpop('myKey'), 'e');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->ltrim('myKey', -3, 0), true);
compare($r->lsize('myKey'), 1);
compare($r->lpop('myKey'), 'a');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->ltrim('myKey', -3, -2), true);
compare($r->lsize('myKey'), 2);
compare($r->lpop('myKey'), 'a');
compare($r->lpop('myKey'), 'b');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->ltrim('myKey', -2, -3), true);
compare($r->lsize('myKey'), 0);

echo("Exec/Multi\n");

$r->del('myKey');
compare($r->multi(), $r);
compare($r->exec(), array());
compare($r->multi(), $r);
compare($r->get('myKey'), $r);
compare($r->set('myKey', 'toto2'), $r);
compare($r->get('myKey'), $r);
compare($r->del('myKey'), $r);
compare($r->rpush('myKey', 'a'), $r);
compare($r->rpop('myKey'), $r);
compare($r->exec(), array(false, true, 'toto2', 1, 1, "a"));

echo("Pipeline\n");

$r->del('myKey');
compare($r->pipeline(), $r);
compare($r->exec(), array());
compare($r->multi(), $r);
compare($r->get('myKey'), $r);
compare($r->set('myKey', 'toto2'), $r);
compare($r->get('myKey'), $r);
compare($r->del('myKey'), $r);
compare($r->rpush('myKey', 'a'), $r);
compare($r->rpop('myKey'), $r);
compare($r->exec(), array(false, true, 'toto2', 1, 1, "a"));

echo("SetNx\n");
$r->del('myKey');
compare($r->setnx('myKey', 'a'), true);
compare($r->setnx('myKey', 'b'), false);
compare($r->get('myKey'), 'a');

if (method_exists($r, 'setnxex')) {
  echo("SetNxEx\n");
  $r->del('myKey');
  compare($r->setnxex('myKey', 2, 'a'), true);
  compare($r->setnxex('myKey', 2, 'b'), false);
  compare($r->get('myKey'), 'a');
  sleep(3);
  compare($r->get('myKey'), false);
}

echo("SetEx\n");

$r->del('myKey');
compare($r->setex('myKey', 2, 'a'), true);
compare($r->get('myKey'), "a");
compare($r->ttl('myKey'), 2);
sleep(1);
compare($r->ttl('myKey'), 1);
sleep(3);
compare($r->get('myKey'), false);

compare($r->set('myKey', 'a'), true);
sleep(3);
compare($r->get('myKey'), "a");
compare($r->setTimeout('myKey', 2), true);
sleep(1);
compare($r->ttl('myKey'), 1);
sleep(2);
compare($r->get('myKey'), false);

echo("OK\n");
