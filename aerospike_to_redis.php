<?php

class AerospikeRedis {

  const BIN_NAME = "r";

  public function __construct($db, $ns, $set, $options = array()) {
    $this->db = $db;
    $this->ns = $ns;
    $this->set = $set;
    $this->on_multi = false;
    if (!isset($options['read_options'])) {
      $options['read_options'] = array(Aerospike::OPT_POLICY_CONSISTENCY => Aerospike::POLICY_CONSISTENCY_ONE, Aerospike::OPT_POLICY_REPLICA => Aerospike::POLICY_REPLICA_ANY);
    }
    if (!isset($options['write_options'])) {
      $options['write_options'] = array(Aerospike::OPT_POLICY_COMMIT_LEVEL => Aerospike::POLICY_COMMIT_LEVEL_MASTER);
    }
    $this->read_options = $options['read_options'];
    $this->write_options = $options['write_options'];
    $this->setnx_options = array();
    $this->operate_options = array();
    foreach(array_keys($this->write_options) as $k) {
      $this->setnx_options[$k] = $this->write_options[$k];
      $this->operate_options[$k] = $this->write_options[$k];
    }
    foreach(array_keys($this->read_options) as $k) {
      $this->operate_options[$k] = $this->read_options[$k];
    }
    $this->setnx_options[Aerospike::OPT_POLICY_EXISTS] = Aerospike::POLICY_EXISTS_CREATE;
  }

  protected function format_key($key) {
    return $this->db->initKey($this->ns, $this->set, $key);
  }

  protected function check_result($status) {
    if ($status != Aerospike::OK) {
      throw new Exception("Aerospike error : ".$this->db->error());
    }
  }

  public function out($v) {
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
    return $this->out($this->_setTimeout($key, $ttl));
  }
  protected function _setTimeout($key, $ttl) {
    $status = $this->db->touch($this->format_key($key), $ttl, $this->write_options);
    if ($status === Aerospike::OK) {
      return true;
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return false;
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function set($key, $value) {
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), 0, $this->write_options);
    $this->check_result($status);
    return $this->out(true);
  }

  public function del($key) {
    return $this->out($this->_del($key));
  }

  protected function _del($key) {
    $status = $this->db->remove($this->format_key($key), $this->write_options);
    if ($status === Aerospike::OK) {
      return 1;
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return 0;
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

  public function rpushEx($key, $value, $ttl = -1) {
    $status = $this->db->apply($this->format_key($key), "redis", "RPUSH", array(self::BIN_NAME, $this->serialize($value), $ttl), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? false : $ret_val);
  }

  public function rpush($key, $value) {
    return $this->rpushEx($key, $value);
  }

  public function lpushEx($key, $value, $ttl = -1) {
    $status = $this->db->apply($this->format_key($key), "redis", "LPUSH", array(self::BIN_NAME, $this->serialize($value), $ttl), $ret_val);
    $this->check_result($status);
    return $this->out(is_array($ret_val) ? false : $ret_val);
  }

  public function lpush($key, $value) {
    return $this->lpushEx($key, $value);
  }

  public function rpopEx($key, $ttl = -1) {
    $status = $this->db->apply($this->format_key($key), "redis", "RPOP", array(self::BIN_NAME, 1, $ttl), $ret_val);
    $this->check_result($status);
    return $this->out(count($ret_val) == 0 ? false : $this->deserialize($ret_val[0]));
  }

  public function rpop($key) {
    return $this->rpopEx($key);
  }

  public function lpopEx($key, $ttl = -1) {
    $status = $this->db->apply($this->format_key($key), "redis", "LPOP", array(self::BIN_NAME, 1, $ttl), $ret_val);
    $this->check_result($status);
    return $this->out(count($ret_val) == 0 ? false : $this->deserialize($ret_val[0]));
  }

  public function lpop($key) {
    return $this->lpopEx($key);
  }

  public function lsize($key) {
    $status = $this->db->get($this->format_key($key), $ret_val, array(self::BIN_NAME . '_size'), $this->read_options);
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return $this->out(0);
    }
    if ($status === Aerospike::OK) {
      $l = isset($ret_val["bins"][self::BIN_NAME . '_size']) ? $ret_val["bins"][self::BIN_NAME . '_size'] : 0;
      return $this->out($l);
    }
    throw new Exception("Aerospike error : ".$this->db->error());
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
    $operations = array();
    if ($value !== 0) {
      array_push($operations, array("op" => Aerospike::OPERATOR_INCR, "bin" => $field, "val" => $value));
    }
    array_push($operations, array("op" => Aerospike::OPERATOR_READ, "bin" => $field));
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
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      $status = $this->db->put($this->format_key($key), array($field => $value), $ttl, $this->setnx_options);
      if ($status === Aerospike::OK) {
        return $this->out($value);
      }
      if ($status === Aerospike::ERR_RECORD_EXISTS) {
        return $this->_hIncrBy($key, $field, $value, $ttl);
      }
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
    $status = $this->db->put($this->format_key($key), array(self::BIN_NAME => $this->serialize($value)), $ttl, $this->setnx_options);
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
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      $ttl = isset($operations['setTimeout']) ? $operations['setTimeout'] : null;
      $fields = array();
      if (isset($operations['hIncrBy'])) {
        foreach(array_keys($operations['hIncrBy']) as $k) {
          $fields[$k] = $operations['hIncrBy'][$k];
        }
      }
      $status = $this->db->put($this->format_key($key), $fields, $ttl, $this->setnx_options);
      if ($status === Aerospike::OK) {
        return $this->out(true);
      }
      if ($status === Aerospike::ERR_RECORD_EXISTS) {
        return $this->batch($key, $operations);
      }
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

class AerospikeRedisExpandedMap extends AerospikeRedis {

  const MAIN_KEY_BIN_NAME = "m";
  const SECOND_KEY_BIN_NAME = "s";
  const VALUE_BIN_NAME = "v";
  const ROOT_BIN_NAME = "z";
  const MAIN_SUFFIX = "____MAIN____";

  public function __construct($db, $ns, $set, $options = array()) {
    parent::__construct($db, $ns, $set, $options);
    if (!isset($options['default_ttl'])) {
      $options['default_ttl'] = 3600 * 24 * 31;
    }
    $this->default_ttl = $options['default_ttl'];
  }

  public function now() {
    return strval(intval(microtime(true)));
  }

  private function format_composite_key($key, $field) {
    return "composite_" . $key . "_" . $field;
  }

  private function composite_ttl($ttl) {
    return $ttl === null ? $this->default_ttl : $ttl;
  }

  private function composite_exists($key) {
    $status = $this->db->get($this->format_key($this->format_composite_key($key, self::MAIN_SUFFIX)), $ret_val, array(self::ROOT_BIN_NAME), $this->read_options);
    if ($status === Aerospike::OK) {
      return $ret_val['bins'][self::ROOT_BIN_NAME];
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      return false;
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  private function composite_exists_or_create($key, $ttl, &$created) {
    $created = false;
    $suffixed_key = $this->composite_exists($key);
    if ($suffixed_key !== false) {
      if ($ttl !== null) {
        parent::_setTimeout($this->format_composite_key($key, self::MAIN_SUFFIX), $ttl);
      }
      return $suffixed_key;
    }
    $suffixed_key = $key. '_' . rand();
    $status = $this->db->put($this->format_key($this->format_composite_key($key, self::MAIN_SUFFIX)), array(self::ROOT_BIN_NAME => $suffixed_key, 'created_at' => $this->now()), $ttl, $this->setnx_options);
    if ($status === Aerospike::OK) {
      $created = true;
      return $suffixed_key;
    }
    if ($status === Aerospike::ERR_RECORD_EXISTS) {
      # wait replication
      sleep(0.1);
      $suffixed_key = $this->composite_exists($key);
      if ($suffixed_key === false) {
        throw new Exception("Unable to create key", $key);
      }
      return $suffixed_key;
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function hSet($key, $field, $value) {
    $suffixed_key = $this->composite_exists_or_create($key, null, $created);
    $k = $this->format_key($this->format_composite_key($suffixed_key, $field));
    $exists = $this->db->exists($k, $metadata, $this->read_options);
    $status = $this->db->put($k, array(self::MAIN_KEY_BIN_NAME => $suffixed_key, self::SECOND_KEY_BIN_NAME => $field, self::VALUE_BIN_NAME => $this->serialize($value), 'created_at' => $this->now()), $this->composite_ttl(null), $this->write_options);
    $this->check_result($status);

    return $this->out($created || $exists === Aerospike::ERR_RECORD_NOT_FOUND ? 1 : 0);
  }

  public function hGet($key, $field) {
    $suffixed_key = $this->composite_exists($key);
    if ($suffixed_key !== false) {
      $status = $this->db->get($this->format_key($this->format_composite_key($suffixed_key, $field)), $ret_val, array(self::VALUE_BIN_NAME), $this->read_options);
      if ($status === Aerospike::OK) {
        return $this->out($ret_val['bins'][self::VALUE_BIN_NAME] === NULL ? false : $this->deserialize($ret_val['bins'][self::VALUE_BIN_NAME]));
      }
      if ($status !== Aerospike::ERR_RECORD_NOT_FOUND) {
        throw new Exception("Aerospike error : ".$this->db->error());
      }
    }
    return $this->out(false);
  }

  public function hDel($key, $field) {
    $suffixed_key = $this->composite_exists($key);
    if ($suffixed_key !== false) {
      return parent::del($this->format_composite_key($suffixed_key, $field));
    }
    return $this->out(0);
  }

  public function del($key) {
    if (parent::_del($this->format_composite_key($key, self::MAIN_SUFFIX)) === 1) {
      return $this->out(1);
    }
    return parent::del($key);
  }

  public function setTimeout($key, $ttl) {
    if (parent::_setTimeout($this->format_composite_key($key, self::MAIN_SUFFIX), $ttl) === true) {
      return $this->out(true);
    }
    return parent::setTimeout($key, $ttl);
  }

  public function ttl($key) {
    $status = $this->db->exists($this->format_key($this->format_composite_key($key, self::MAIN_SUFFIX)), $ret_val, $this->read_options);
    if ($status === Aerospike::OK) {
      return $this->out(intval($ret_val["ttl"]));
    }
    return $this->out(parent::ttl($key));
  }

  public function hmSet($key, $values) {
    $suffixed_key = $this->composite_exists_or_create($key, null, $created);
    foreach(array_keys($values) as $k) {
      $status = $this->db->put($this->format_key($this->format_composite_key($suffixed_key, $k)), array(self::MAIN_KEY_BIN_NAME => $suffixed_key, self::SECOND_KEY_BIN_NAME => $k, self::VALUE_BIN_NAME => $this->serialize($this->serialize($values[$k])), 'created_at' => $this->now()), $this->composite_ttl(null), $this->write_options);
      $this->check_result($status);
    }
    return $this->out(true);
  }

  public function hmGet($key, $keys) {
    $r = array();
    $suffixed_key = $this->composite_exists($key);
    if ($suffixed_key !== false) {
      foreach($keys as $k) {
        $status = $this->db->get($this->format_key($this->format_composite_key($suffixed_key, $k)), $ret_val, array(self::VALUE_BIN_NAME), $this->read_options);
        if ($status === Aerospike::OK) {
          $r[$k] = $ret_val['bins'][self::VALUE_BIN_NAME] === NULL ? false : $this->deserialize($ret_val['bins'][self::VALUE_BIN_NAME]);
        }
        elseif ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
          $r[$k] = false;
        }
        else {
          throw new Exception("Aerospike error : ".$this->db->error());
        }
      }
      return $this->out($r);
    }
    foreach($keys as $k) {
      $r[$k] = false;
    }
    return $this->out($r);
  }

  public function hGetAll($key) {
    $r = array();
    $suffixed_key = $this->composite_exists($key);
    if ($suffixed_key !== false) {
      $where = Aerospike::predicateEquals(self::MAIN_KEY_BIN_NAME, $suffixed_key);
      $status = $this->db->query($this->ns, $this->set, $where, function ($record) use (&$r) {
        $r[$record['bins'][self::SECOND_KEY_BIN_NAME]] = $this->deserialize($record['bins'][self::VALUE_BIN_NAME]);
      });
      $this->check_result($status);
    }
    return $this->out($r);
  }

  public function hIncrBy($key, $field, $value) {
    return $this->hIncrByEx($key, $field, $value, null);
  }

  private function compositeIncr($suffixed_key, $field, $value) {
    $operations = array(
      array("op" => Aerospike::OPERATOR_WRITE, "bin" => self::MAIN_KEY_BIN_NAME, "val" => $suffixed_key),
      array("op" => Aerospike::OPERATOR_WRITE, "bin" => self::SECOND_KEY_BIN_NAME, "val" => $field),
      array("op" => Aerospike::OPERATOR_INCR, "bin" => self::VALUE_BIN_NAME, "val" => $value),
      array("op" => Aerospike::OPERATOR_READ, "bin" => self::VALUE_BIN_NAME),
      array("op" => Aerospike::OPERATOR_TOUCH, "ttl" => $this->composite_ttl(null)),
    );
    $k = $this->format_composite_key($suffixed_key, $field);
    $status = $this->db->operate($this->format_key($k), $operations, $ret_val, $this->operate_options);
     if ($status === Aerospike::OK) {
      return $ret_val[self::VALUE_BIN_NAME];
    }
    if ($status === Aerospike::ERR_BIN_INCOMPATIBLE_TYPE) {
      return false;
    }
    if ($status === Aerospike::ERR_RECORD_NOT_FOUND) {
      $status = $this->db->put($this->format_key($k), array(self::MAIN_KEY_BIN_NAME => $suffixed_key, self::SECOND_KEY_BIN_NAME => $field, self::VALUE_BIN_NAME => $value, 'created_at' => $this->now()), $this->composite_ttl(null), $this->setnx_options);
      if ($status === Aerospike::OK) {
        return $this->out($value);
      }
      if ($status === Aerospike::ERR_RECORD_EXISTS) {
        return $this->compositeIncr($suffixed_key, $field, $value);
      }
    }
    throw new Exception("Aerospike error : ".$this->db->error());
  }

  public function hIncrByEx($key, $field, $value, $ttl) {
    $suffixed_key = $this->composite_exists_or_create($key, $ttl, $created);
    return $this->compositeIncr($suffixed_key, $field, $value);
  }

  public function batch($key, $operations) {
    if (isset($operations['hIncrBy'])) {
      $ttl = isset($operations['setTimeout']) ? $operations['setTimeout'] : $this->default_ttl;
      $suffixed_key = $this->composite_exists_or_create($key, $ttl, $created);
      foreach(array_keys($operations['hIncrBy']) as $k) {
        if ($this->compositeIncr($suffixed_key, $k, $operations['hIncrBy'][$k]) === false) {
          throw new Exception("Aerospike error : ".$this->db->error());
        }
      }
      return $this->out(true);
    }
    if (isset($operations['setTimeout'])) {
      parent::_setTimeout($this->format_composite_key($key, self::MAIN_SUFFIX), $operations['setTimeout']);
    }
    return $this->out(true);
  }

}
