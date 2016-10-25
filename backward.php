<?php

$redis = new Redis();

require './aerospike_to_redis.php';

$host = isset($_ENV['HOST']) ? ($_ENV['HOST']) : 'localhost';
echo "Using Aerospike on " . $host . "\n";
$config = array("hosts" => array(array("addr" => $host, "port" => 3000)));
$db = new Aerospike($config, false);
if (isset($_ENV['EXPANDED_MAP'])) {
  $r = new AerospikeRedisExpandedMap($db, "test", "redis");
}
else {
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

function upper($a, $b) {
  if ($a < $b) {
    throw new Exception("Must ".dump($a)." >= ".dump($b));
  }
}

function lower($a, $b) {
  if ($a > $b) {
    throw new Exception("Must ".dump($a)." <= ".dump($b));
  }
}

function compare_map($a, $b) {
  ksort($a);
  ksort($b);
  compare($a, $b);
}

$json = file_get_contents('big_json.json');
$bin = gzcompress($json);

compare($r->connect('127.0.0.1', 6379), true);
compare($redis->connect('127.0.0.1', 6379), true);

$r->del('myKey');
$r->set('myKey', 'a');
compare($r->get('myKey'), 'a');
compare($redis->get('myKey'), 'a');

$r->del('myKey');
$r->set('myKey', 1);
compare($r->get('myKey'), '1');
compare($redis->get('myKey'), '1');

$r->del('myKey');
$r->set('myKey', $bin);
compare($r->get('myKey'), $bin);
compare($redis->get('myKey'), $bin);

$r->del('myKey');
$r->rpush('myKey', 1);
$r->rpush('myKey', 1);
compare($r->rpop('myKey'), '1');
compare($redis->rpop('myKey'), '1');

$r->del('myKey');
$r->rpush('myKey', 'a');
$r->rpush('myKey', 'a');
compare($r->rpop('myKey'), 'a');
compare($redis->rpop('myKey'), 'a');

$r->del('myKey');
$r->rpush('myKey', $bin);
$r->rpush('myKey', $bin);
compare($r->rpop('myKey'), $bin);
compare($redis->rpop('myKey'), $bin);

$r->del('myKey');
$r->hSet('myKey', 'a', 'b');
compare($r->hGet('myKey', 'a'), 'b');
compare($r->hGetAll('myKey'), array('a' => 'b'));
compare($r->hmGet('myKey', array('a')), array('a' => 'b'));
compare($redis->hGet('myKey', 'a'), 'b');
compare($redis->hGetAll('myKey'), array('a' => 'b'));
compare($redis->hmGet('myKey', array('a')), array('a' => 'b'));

$r->del('myKey');
$r->hSet('myKey', 'a', 42);
compare($r->hGet('myKey', 'a'), '42');
compare($r->hGetAll('myKey'), array('a' => '42'));
compare($r->hmGet('myKey', array('a')), array('a' => '42'));
compare($redis->hGet('myKey', 'a'), '42');
compare($redis->hGetAll('myKey'), array('a' => '42'));
compare($redis->hmGet('myKey', array('a')), array('a' => '42'));

$r->del('myKey');
$r->hSet('myKey', 'a', $bin);
compare($r->hGet('myKey', 'a'), $bin);
compare($r->hGetAll('myKey'), array('a' => $bin));
compare($r->hmGet('myKey', array('a')), array('a' => $bin));
compare($redis->hGet('myKey', 'a'), $bin);
compare($redis->hGetAll('myKey'), array('a' => $bin));
compare($redis->hmGet('myKey', array('a')), array('a' => $bin));

echo("Done\n");