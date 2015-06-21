<?php

class AerospikeRedis {

  const BIN_NAME = "r";

  public function __construct($db, $ns, $set) {
    $this->db = $db;
    $this->ns = $ns;
    $this->set = $set;
    $this->on_multi = false;
  }

  private function format_key($key) {
    return $this->db->initKey($this->ns, $this->set, $key);
  }

  private function check_result($status) {
    if ($status != Aerospike::OK) {
      throw new Exception("Aerospike error : ".$this->db->error());
    }
  }

  private function out($v) {
    if ($this->on_multi === false) {
      return $v;
    }
    else {
      array_push($this->on_multi, $v);
      return $this;
    }
  }

  private function serialize($v) {
    if (strpos($v, 0) !== false) {
      return "__64__" . base64_encode($v);
    }
    return $v;
  }

  private function deserialize($v) {
    if (substr($v, 0, 6) === "__64__") {
      return base64_decode(substr($v, 6));
    }
    if (is_int($v)) {
      $v = strval($v);
    }
    return $v;
  }

  private function assert_ok($ret_val) {
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
    $status = $this->db->apply($this->format_key($key), "redis", "GET", array(self::BIN_NAME), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? false : $this->deserialize($ret_val));
  }

  public function ttl($key) {
    $status = $this->db->apply($this->format_key($key), "redis", "TTL", array(self::BIN_NAME), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? false : $ret_val);
  }

  public function setTimeout($key, $ttl) {
    $status = $this->db->touch($this->format_key($key), $ttl);
    $this->check_result($status);
    return $this->out(true);
  }

  public function set($key, $value) {
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)));
    $this->check_result($status);
    return $this->out(true);
  }

  public function del($key) {
    $status = $this->db->remove($this->format_key($key));
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
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), $ttl);
    $this->check_result($status);
    return $this->out(true);
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
    $status = $this->db->get($this->format_key($key), $ret_val, array($field));
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
    $status = $this->db->apply($this->format_key($key), "redis", "HMGET", array($keys), $ret_val);
    $this->check_result($status);
    $r = array();
    for($i = 0; $i < count($keys); $i ++) {
      $r[$keys[$i]] = ($ret_val[$i] === NULL) ? false : $this->deserialize($ret_val[$i]);
    }
    return $r;
  }

  public function hGetAll($key) {
    $status = $this->db->apply($this->format_key($key), "redis", "HGETALL", array(), $ret_val);
    $this->check_result($status);
    $r = array();
    for($i = 0; $i < count($ret_val); $i += 2) {
      $r[$ret_val[$i]] = $this->deserialize($ret_val[$i + 1]);
    }
    return $r;
  }

   public function hIncrBy($key, $field, $value) {
    $status = $this->db->apply($this->format_key($key), "redis", "HINCRBY", array($field, $value), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? 0 : $ret_val);
  }

  public function setnx($key, $value) {
    return $this->setnxex($key, 0, $value);
  }

  public function setnxex($key, $ttl, $value) {
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), $ttl, array(Aerospike::OPT_POLICY_EXISTS => Aerospike::POLICY_EXISTS_CREATE));
    if ($status === Aerospike::OK) {
      return $this->out(true);
    }
    if ($status === Aerospike::ERR_RECORD_EXISTS) {
      return $this->out(false);
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
