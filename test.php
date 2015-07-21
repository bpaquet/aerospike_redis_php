<?php

require './aerospike_to_redis.php';


class ProxyCounter {

  public function __construct($target) {
    $this->_counter = 0;
    $this->_calls = array();
    $this->_target = $target;
  }

  public function __call($name, $args) {
    $this->_counter ++;
    if (! isset($this->_calls[$name])) {
      $this->_calls[$name] = 0;
    }
    $this->_calls[$name] ++;
    $res = call_user_func_array(array($this->_target, $name), $args);
    return $res === $this->_target ? $this : $res;
  }

}

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

$r = new ProxyCounter($r);

$start = microtime(true);

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

function compare_map($a, $b) {
  ksort($a);
  ksort($b);
  compare($a, $b);
}

echo("Connect\n");
compare($r->connect('127.0.0.1', 6379), true);

echo("Get Set\n");

$r->delete('myKey');

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

echo("Incr / Decr\n");

$r->delete('myKey');
compare($r->get('myKey'), false);
compare($r->incr('myKey'), 1);
compare($r->get('myKey'), "1");
compare($r->incrby('myKey', 2), 3);
compare($r->get('myKey'), "3");
compare($r->decr('myKey'), 2);
compare($r->get('myKey'), "2");
compare($r->decrby('myKey', 5), -3);
compare($r->get('myKey'), "-3");

$r->delete('myKey');
compare($r->set('myKey', "a"), true);
compare($r->incr('myKey'), false);

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

compare($r->lpush('myKey', $bin), 1);
compare($r->rpop('myKey'), $bin);
compare($r->rpush('myKey', $bin), 1);
compare($r->rpop('myKey'), $bin);

echo("Array Ltrim lRange\n");

$r->del('myKey');
compare($r->lRange('myKey', 0, 0), array());
compare($r->rpush('myKey', 'a'), 1);
compare($r->lsize('myKey'), 1);
compare($r->lRange('myKey', 0, 0), array(0 => 'a'));
compare($r->ltrim('myKey', 0, 0), true);
compare($r->lRange('myKey', 0, 0), array(0 => 'a'));
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', 0, -1), true);
compare($r->lRange('myKey', 0, -1), array(0 => 'a'));
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', -1, -1), true);
compare($r->lRange('myKey', -1, -1), array(0 => 'a'));
compare($r->lsize('myKey'), 1);
compare($r->ltrim('myKey', -1, 0), true);
compare($r->lRange('myKey', -1, 0), array(0 => 'a'));
compare($r->lsize('myKey'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->lRange('myKey', 0, 12), array(0 => 'a', 1 => 'b', 2 => 'c'));
compare($r->ltrim('myKey', 0, 12), true);
compare($r->lsize('myKey'), 3);
compare($r->lRange('myKey', 2, 2), array(0 => 'c'));
compare($r->ltrim('myKey', 2, 2), true);
compare($r->rpop('myKey'), 'c');
compare($r->lsize('myKey'), 0);
compare($r->lRange('myKey', 2, 2), array());

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->lRange('myKey', 0, -2), array(0 => 'a', 1 => 'b'));
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
compare($r->lRange('myKey', -2, 8), array(0 => 'e', 1 => 'f'));
compare($r->lRange('myKey', 0, 18), array(0 => 'a', 1 => 'b', 2 => 'c', 3 => 'd', 4 => 'e', 5 => 'f'));
compare($r->lRange('myKey', 2, 4), array(0 => 'c', 1 => 'd', 2 => 'e'));
compare($r->ltrim('myKey', 2, 4), true);
compare($r->lsize('myKey'), 3);
compare($r->lpop('myKey'), 'c');
compare($r->lpop('myKey'), 'd');
compare($r->lpop('myKey'), 'e');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->lRange('myKey', -3, 0), array(0 => 'a'));
compare($r->ltrim('myKey', -3, 0), true);
compare($r->lsize('myKey'), 1);
compare($r->lpop('myKey'), 'a');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->lRange('myKey', -3, -2), array(0 => 'a', 1 => 'b'));
compare($r->ltrim('myKey', -3, -2), true);
compare($r->lsize('myKey'), 2);
compare($r->lpop('myKey'), 'a');
compare($r->lpop('myKey'), 'b');

compare($r->rpush('myKey', 'a'), 1);
compare($r->rpush('myKey', 'b'), 2);
compare($r->rpush('myKey', 'c'), 3);
compare($r->lRange('myKey', -2, -3), array());
compare($r->ltrim('myKey', -2, -3), true);
compare($r->lsize('myKey'), 0);
compare($r->lRange('myKey', 0, 200), array());

echo("hSet hGet hDel\n");
$r->del('myKey');
compare($r->hSet('myKey', "a", 2), 1);
compare($r->hGet('myKey', "z"), false);
compare($r->hGet('myKey', "a"), '2');
compare($r->hSet('myKey', "a", 1), 0);
compare($r->hSet('myKey', "b", "a"), 1);
compare($r->hGet('myKey', "a"), '1');
compare($r->hGet('myKey', "b"), "a");
compare($r->hSet('myKey', "a",0), 0);
compare($r->hGet('myKey', "a"), '0');
compare($r->hDel('myKey', "a"), 1);
compare($r->hDel('myKey', "a"), 0);
compare($r->hGet('myKey', "a"), false);
compare($r->hGet('myKey', "c"), false);
compare($r->hSet('myKey', "a",1), 1);
compare($r->del('myKey'), 1);
compare($r->del('myKey'), 0);
compare($r->hGet('myKey', "a"), false);
$r->del('myKey');
compare($r->del('myKey'), 0);
compare($r->hSet('myKey', "b", $bin), 1);
compare($r->hGet('myKey', "b"), $bin);

compare($r->hSet('myKey', "veryverylongke", "toto"), 1);
compare($r->hGet('myKey', "veryverylongke"), "toto");


echo("hmSet hmGet\n");
$r->del('myKey');
compare($r->hmGet('myKey', array('a','b','c')), array('a' => false, 'b'  => false, 'c' => false));
compare($r->hSet('myKey', "b", 2), 1);
compare($r->hmGet('myKey', array('a','b','c')), array('a' => false, 'b'  => '2', 'c' => false));
compare($r->hmSet('myKey', array('a' => 1,'b' => 1,'c' => 1)), true);
compare($r->hmGet('myKey', array('a','b','c')), array('a' => '1','b'  => '1','c' => '1'));
compare($r->hDel('myKey', 'a'), 1);
compare($r->hmGet('myKey', array('a','b','c')), array('a'=>false,'b'=> '1','c'=> '1'));
compare($r->hmGet('myKey', array('b','c')), array('b' => '1','c' => '1'));

compare($r->multi(), $r);
compare($r->hmGet('myKey', array('a','b','c')), $r);
compare($r->hmGet('myKey', array('b','c')), $r);
compare($r->exec(), array(array('a'=>false,'b'=> '1','c'=> '1'), array('b' => '1','c' => '1')));

echo("hGetAll\n");
$r->del('myKey');
compare($r->hGetAll('myKey'), array());
compare($r->hmSet('myKey', array('b' => 3, 'a' => 1, 1 => 4, 'toto' => 2)),  true);
compare_map($r->hGetAll('myKey'), array('b' => '3', 'a' => '1', 1 => '4', 'toto' => '2'));
compare($r->hDel('myKey', 'a'), 1);
compare_map($r->hGetAll('myKey'), array('b' => '3', 1 => '4', 'toto' => '2'));
compare($r->hDel('myKey', 1), 1);
compare_map($r->hGetAll('myKey'), array('b' => '3', 'toto' => '2'));
compare($r->hmSet('myKey', array("b" => $bin)), true);
compare($r->hGet('myKey', "b"), $bin);

echo("hIncrBy\n");
$r->del('myKey');
compare($r->hIncrBy('myKey', 'a', 1), 1);
compare($r->hIncrBy('myKey', 'a', 10), 11);
compare($r->hIncrBy('myKey', 'a', -15), -4);
compare($r->hIncrBy('myKey', 'a', 0), -4);
compare($r->del('myKey'), 1);
compare($r->hIncrBy('myKey', 'a', 0), 0);

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

compare($r->setTimeout('not_existing key', 10), false);
compare($r->ttl('not_existing key'), -2);

$r->del('myKey');
compare($r->setex('myKey', 2, 'a'), true);
compare($r->get('myKey'), "a");
upper($r->ttl('myKey'), 1);
sleep(1);
compare($r->get('myKey'), "a");
upper($r->ttl('myKey'), 1);
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

echo("Lot of keys\n");
for($i = 0; $i < 500; $i ++) {
  compare($r->set('mykey'.$i, $i), true);
}

for($i = 0; $i < 500; $i ++) {
  compare($r->get('mykey'.$i), ''.$i);
}

echo("Batch\n");

$r->del('myKey');
$r->del('myKey2');
compare($r->batch('myKey2', array('setTimeout' => 200)), true);
compare($r->ttl('myKey2'), -2);


compare($r->batch('myKey', array('hIncrBy' => array('key' => 1, 'key2' => 5), 'setTimeout' => 10)), true);
compare_map($r->hGetAll('myKey'), array('key' => '1', 'key2' => '5'));
compare($r->batch('myKey', array('hIncrBy' => array('key2' => 6), 'setTimeout' => 200)), true);
upper($r->ttl('myKey'), 100);
compare_map($r->hGetAll('myKey'), array('key' => '1', 'key2' => '11'));
compare($r->batch('myKey', array('hIncrBy' => array('key3' => 12), 'setTimeout' => 2)), true);
compare_map($r->hGetAll('myKey'), array('key' => '1', 'key2' => '11', 'key3' => '12'));
sleep(5);
compare_map($r->hGetAll('myKey'), array('key' => '1', 'key2' => '11', 'key3' => '12'));

echo("OK\n");

$delay = microtime(true) - $start;

echo("Number of calls ".$r->_counter.", delay ".$delay."\n");
