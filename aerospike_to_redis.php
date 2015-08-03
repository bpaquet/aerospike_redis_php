<?php

class AerospikeRedis {

  const BIN_NAME = "r";

  public function __construct($db, $ns, $set, $read_options = array(Aerospike::OPT_POLICY_CONSISTENCY => Aerospike::POLICY_CONSISTENCY_ONE, Aerospike::OPT_POLICY_REPLICA => Aerospike::POLICY_REPLICA_ANY), $write_options = array(Aerospike::OPT_POLICY_COMMIT_LEVEL => Aerospike::POLICY_COMMIT_LEVEL_MASTER)) {
    $this->db = $db;
    $this->ns = $ns;
    $this->set = $set;
    $this->on_multi = false;
    $this->read_options = $read_options;
    $this->write_options = $write_options;
    $this->setex_options = array();
    $this->operate_options = array();
    foreach(array_keys($write_options) as $k) {
      $this->setex_options[$k] = $write_options[$k];
      $this->operate_options[$k] = $write_options[$k];
    }
    foreach(array_keys($read_options) as $k) {
      $this->operate_options[$k] = $read_options[$k];
    }
    $this->setex_options[Aerospike::OPT_POLICY_EXISTS] = Aerospike::POLICY_EXISTS_CREATE;
  }

  protected function format_key($key) {
    return $this->db->initKey($this->ns, $this->set, $key);
  }

  protected function check_result($status) {
    if ($status != Aerospike::OK) {
      throw new Exception("Aerospike error : ".$this->db->error());
    }
  }

  protected function out($v) {
    if ($this->on_multi === false) {
      return $v;
    }
    else {
      array_push($this->on_multi, $v);
      return $this;
    }
  }

  protected function serialize($v) {
    if (strpos($v, 0) !== false) {
      return "__64__" . base64_encode($v);
    }
    return $v;
  }

  protected function deserialize($v) {
    if (is_string($v) && substr($v, 0, 6) === "__64__") {
      return base64_decode(substr($v, 6));
    }
    if (is_int($v)) {
      $v = strval($v);
    }
    return $v;
  }

  protected function assert_ok($ret_val) {
    if ($ret_val != "OK") {
      throw new Exception("Aerospike error, result not OK ".$ret_val);
    }
  }

  public function pipeline() {
    return $this->multi();
  }

  public function multi() {
    $this->on_multi = array();
    return $this;
  }

  public function exec() {
    $res = $this->on_multi;
    $this->on_multi = false;
    return $res;
  }

  public function get($key) {
    $status = $this->db->get($this->format_key($key), $ret_val, array(self::BIN_NAME), $this->read_options);
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return $this->out(false);
    }
    if ($status === Aerospike::OK) {
      return $this->out($this->deserialize($ret_val["bins"][self::BIN_NAME]));
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function ttl($key) {
    $status = $this->db->exists($this->format_key($key), $ret_val, $this->read_options);
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return $this->out(-2);
    }
    if ($status === Aerospike::OK) {
      return $this->out(intval($ret_val["ttl"]));
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function setTimeout($key, $ttl) {
    $status = $this->db->touch($this->format_key($key), $ttl, $this->write_options);
    if ($status === Aerospike::OK) {
      return $this->out(true);
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return $this->out(false);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function set($key, $value) {
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), 0, $this->write_options);
    $this->check_result($status);
    return $this->out(true);
  }

  public function del($key) {
    $status = $this->db->remove($this->format_key($key), $this->write_options);
    if ($status === Aerospike::OK) {
      return $this->out(1);
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return $this->out(0);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function delete($key) {
    return $this->del($key);
  }

  public function setex($key, $ttl, $value) {
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), $ttl, $this->write_options);
    $this->check_result($status);
    return $this->out(true);
  }

  public function incr($key) {
    return $this->_hIncrBy($key, self::BIN_NAME, 1);
  }

  public function decr($key) {
    return $this->_hIncrBy($key, self::BIN_NAME, -1);
  }

  public function decrby($key, $value) {
    return $this->_hIncrBy($key, self::BIN_NAME, -$value);
  }

  public function incrby($key, $value) {
      return $this->_hIncrBy($key, self::BIN_NAME, $value);
  }

  public function rpush($key, $value) {
    $status = $this->db->apply($this->format_key($key), "redis", "RPUSH", array(self::BIN_NAME, $this->serialize($value)), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? false : $ret_val);
  }

  public function lpush($key, $value) {
    $status = $this->db->apply($this->format_key($key), "redis", "LPUSH", array(self::BIN_NAME, $this->serialize($value)), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? false : $ret_val);
  }

  public function rpop($key) {
    $status = $this->db->apply($this->format_key($key), "redis", "RPOP", array(self::BIN_NAME, 1), $ret_val);
    $this->check_result($status);
    return $this->out(count($ret_val) == 0 ? false : $this->deserialize($ret_val[0]));
  }

  public function lpop($key) {
    $status = $this->db->apply($this->format_key($key), "redis", "LPOP", array(self::BIN_NAME, 1), $ret_val);
    $this->check_result($status);
    return $this->out(count($ret_val) == 0 ? false : $this->deserialize($ret_val[0]));
  }

  public function lsize($key) {
    $status = $this->db->apply($this->format_key($key), "redis", "LSIZE", array(self::BIN_NAME), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? 0 : $ret_val);
  }

  public function ltrim($key, $start, $end) {
    $status = $this->db->apply($this->format_key($key), "redis", "LTRIM", array(self::BIN_NAME, $start, $end), $ret_val);
    $this->check_result($status);
    $this->assert_ok($ret_val);
    return $this->out(true);
  }

  public function lRange($key, $start, $end) {
    $status = $this->db->apply($this->format_key($key), "redis", "LRANGE", array(self::BIN_NAME, $start, $end), $ret_val);
    $this->check_result($status);
    return $this->out(array_map(array($this, 'deserialize'), $ret_val));
  }

  public function hSet($key, $field, $value) {
    $status = $this->db->apply($this->format_key($key), "redis", "HSET", array($field, $this->serialize($value)), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? 0 : $ret_val);
  }

  public function hGet($key, $field) {
    $status = $this->db->get($this->format_key($key), $ret_val, array($field), $this->read_options);
    if ($status === Aerospike::OK) {
      return $this->out(isset($ret_val["bins"][$field]) ? $this->deserialize($ret_val["bins"][$field]) : false);
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return $this->out(false);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function hDel($key, $field) {
    $status = $this->db->apply($this->format_key($key), "redis", "HDEL", array($field), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? 0 : $ret_val);
  }

  public function hmSet($key, $values) {
    foreach(array_keys($values) as $k) {
      $values[$k] = $this->serialize($values[$k]);
    }
    $status = $this->db->apply($this->format_key($key), "redis", "HMSET", array($values), $ret_val);
    $this->check_result($status);
    $this->assert_ok($ret_val);
    return $this->out(true);
  }

  public function hmGet($key, $keys) {
    $status = $this->db->get($this->format_key($key), $ret_val, $keys, $this->read_options);
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      $r = array();
      foreach($keys as $key) {
        $r[$key] = false;
      }
      return $this->out($r);
    }
    if ($status === Aerospike::OK) {
      $r = array();
      foreach($keys as $key) {
        $r[$key] = ($ret_val["bins"][$key] === NULL) ? false : $this->deserialize($ret_val["bins"][$key]);
      }
      return $this->out($r);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function hGetAll($key) {
    $status = $this->db->apply($this->format_key($key), "redis", "HGETALL", array(), $ret_val);
    $this->check_result($status);
    $r = array();
    for($i = 0; $i < count($ret_val); $i += 2) {
      $r[$ret_val[$i]] = $this->deserialize($ret_val[$i + 1]);
    }
    return $this->out($r);
  }

  protected function _hIncrBy($key, $field, $value, $ttl = null) {
    $operations = array(
      array("op" => Aerospike::OPERATOR_INCR, "bin" => $field, "val" => $value),
      array("op" => Aerospike::OPERATOR_READ, "bin" => $field),
    );
    if ($ttl !== null) {
      array_push($operations, array("op" => Aerospike::OPERATOR_TOUCH, "ttl" => intval($ttl)));
    }
    $status = $this->db->operate($this->format_key($key), $operations, $ret_val, $this->operate_options);
     if ($status === Aerospike::OK) {
      return $this->out($ret_val[$field]);
    }
    if ($status === Aerospike::ERR_BIN_INCOMPATIBLE_TYPE) {
      return $this->out(false);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function hIncrBy($key, $field, $value) {
    return $this->_hIncrBy($key, $field, $value);
  }

  public function hIncrByEx($key, $field, $value, $ttl) {
    return $this->_hIncrBy($key, $field, $value, $ttl);
  }

  public function setnx($key, $value) {
    return $this->setnxex($key, 0, $value);
  }

  public function setnxex($key, $ttl, $value) {
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), $ttl, $this->setex_options);
    if ($status === Aerospike::OK) {
      return $this->out(true);
    }
    if ($status === Aerospike::ERR_RECORD_EXISTS) {
      return $this->out(false);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function batch($key, $operations) {
    $x = array();
    if (isset($operations['hIncrBy'])) {
      foreach(array_keys($operations['hIncrBy']) as $k) {
        array_push($x, array("op" => Aerospike::OPERATOR_INCR, "bin" => $k, "val" => $operations['hIncrBy'][$k]));
      }
    }
    if (isset($operations['setTimeout'])) {
      array_push($x, array("op" => Aerospike::OPERATOR_TOUCH, "ttl" => intval($operations['setTimeout'])));
    }
    if (count($x) === 1 && isset($operations['setTimeout'])) {
      $this->setTimeout($key, $operations['setTimeout']);
      return $this->out(true);
    }
    $status = $this->db->operate($this->format_key($key), $x, $ret_val, $this->operate_options);
    if ($status === Aerospike::OK) {
      return $this->out(true);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function flushdb() {
    $options = array(Aerospike::OPT_SCAN_PRIORITY => Aerospike::SCAN_PRIORITY_HIGH);
    $status = $this->db->scan($this->ns, $this->set, function ($record) {
      $this->db->remove($record["key"]);
    }, array(), $options);
    return $this->out(true);
  }

  public function connect($a1 = 1, $a2 = 1, $a3 = 1, $a4 = 1, $a5 = 1) {
    return true;
  }

  public function pconnect($a1 = 1, $a2 = 1, $a3 = 1, $a4 = 1) {
    return true;
  }

  public function close() {
  }

}

class AerospikeRedisOneBin extends AerospikeRedis {

  public function hSet($key, $field, $value) {
    $status = $this->db->apply($this->format_key($key), "redis", "HSET_ONE_BIN", array(self::BIN_NAME, $field, $this->serialize($value)), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? 0 : $ret_val);
  }

  private function _hGetAll($key) {
    $status = $this->db->get($this->format_key($key), $ret_val, array(self::BIN_NAME), $this->read_options);
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return array();
    }
    if ($status === Aerospike::OK) {
      $r = array();
      foreach(array_keys($ret_val['bins']['r']) as $k) {
        if ($ret_val['bins']['r'][$k] !== NULL) {
          $r[$k] = $this->deserialize($ret_val['bins']['r'][$k]);
        }
      }
      return $r;
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function hGet($key, $field) {
    $all = $this->_hGetAll($key);
    return $this->out(isset($all[$field]) ? $all[$field] : false);
  }

  public function hDel($key, $field) {
    $status = $this->db->apply($this->format_key($key), "redis", "HDEL_ONE_BIN", array(self::BIN_NAME, $field), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? 0 : $ret_val);
  }

  public function hmSet($key, $values) {
    foreach(array_keys($values) as $k) {
      $values[$k] = $this->serialize($values[$k]);
    }
    $status = $this->db->apply($this->format_key($key), "redis", "HMSET_ONE_BIN", array(self::BIN_NAME, $values), $ret_val);
    $this->check_result($status);
    $this->assert_ok($ret_val);
    return $this->out(true);
  }

  public function hmGet($key, $keys) {
    $all = $this->_hGetAll($key);
    $r = array();
    foreach($keys as $k) {
      $r[$k] = isset($all[$k]) ? $all[$k] : false;
    }
    return $this->out($r);
  }

  public function hGetAll($key) {
    return $this->out($this->_hGetAll($key));
  }

  public function hIncrBy($key, $field, $value) {
    $status = $this->db->apply($this->format_key($key), "redis", "HINCRBY_ONE_BIN", array(self::BIN_NAME, $field, $value), $ret_val);
    $this->check_result($status);
    return $this->out($ret_val);
  }

  public function batch($key, $operations) {
    $x = array();
    if (isset($operations['hIncrBy'])) {
      foreach(array_keys($operations['hIncrBy']) as $k) {
        array_push($x, array("op" => "incr", "field" => $k, "increment" => $operations['hIncrBy'][$k]));
      }
    }
    if (isset($operations['setTimeout'])) {
      array_push($x, array("op" => "touch", "ttl" => $operations['setTimeout']));
    }
    if (count($x) === 1 && isset($operations['setTimeout'])) {
      $this->setTimeout($key, $operations['setTimeout']);
      return $this->out(true);
    }
    $status = $this->db->apply($this->format_key($key), "redis", "BATCH_ONE_BIN", array(self::BIN_NAME, $x), $ret_val);
    $this->check_result($status);
    return $this->out(true);
  }

}