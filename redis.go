package main

import (
  "fmt"
  "net"
  "flag"
  "strconv"
  "reflect"

  as "github.com/aerospike/aerospike-client-go"
)

const BIN_NAME = "r"
const module_name = "redis"

type write_func func([]byte) (error)

type handler struct {
  args_count int
  f func(write_func, context, [][]byte) (error)
}

type context struct {
  client *as.Client
  ns string
  set string
  read_policy *as.BasePolicy
  write_policy *as.WritePolicy
}

func fillReadPolicy(read_policy * as.BasePolicy) {
  read_policy.ConsistencyLevel = as.CONSISTENCY_ONE
  read_policy.ReplicaPolicy = as.MASTER_PROLES
}

func fillWritePolicy(write_policy * as.WritePolicy) {
  write_policy.CommitLevel = as.COMMIT_MASTER
}

func fillWritePolicyEx(ctx context, ttl int, create_only bool) * as.WritePolicy {
  policy := as.NewWritePolicy(0, 0)
  if ttl != -1 {
    policy = as.NewWritePolicy(0, uint32(ttl))
  }
  fillWritePolicy(policy)
  if create_only {
    policy.RecordExistsAction = as.CREATE_ONLY
  }
  return policy
}

func buildKey(ctx context, key []byte) (*as.Key, error) {
  return as.NewKey(ctx.ns, ctx.set, string(key))
}

func panicOnError(err error) {
  if err != nil {
    panic(err)
  }
}

func WriteErr(wf write_func, s string) error {
  fmt.Printf("Client error : %s\n", s)
  return wf([]byte("-ERR " + s + "\n"))
}

func WriteByteArray(wf write_func, buf []byte) error {
  err := wf([]byte("$" + strconv.Itoa(len(buf)) + "\r\n"))
  if err != nil {
    return err
  }
  err = wf(buf)
  if err != nil {
    return err
  }
  return wf([]byte("\r\n"))
}

func WriteArray(wf write_func, array []interface{}) error {
  err := WriteLine(wf, "*" + strconv.Itoa(len(array)))
  if err != nil {
    return err
  }
  for _, e := range array {
    err := WriteByteArray(wf, e.([]byte))
    if err != nil {
      return err
    }
  }
  return nil
}

func WriteLine(wf write_func, s string) error {
  return wf([]byte(s + "\r\n"))
}

func WriteValue(wf write_func, x interface{}) error {
  if reflect.TypeOf(x).Kind() == reflect.Int {
    return WriteByteArray(wf, []byte(strconv.Itoa(x.(int))))
  } else {
    return WriteByteArray(wf, x.([]byte))
  }
}

func WriteBin(wf write_func, rec * as.Record, bin_name string, nil_value string) error {
  if rec == nil {
    return WriteLine(wf, nil_value)
  } else {
    x := rec.Bins[bin_name]
    if x == nil {
      return WriteLine(wf, nil_value)
    } else {
      return WriteValue(wf, x)
    }
  }
}

func WriteBinInt(wf write_func, rec * as.Record, bin_name string) error {
  nil_value := ":0"
  if rec == nil {
    return WriteLine(wf, nil_value)
  } else {
    x := rec.Bins[bin_name]
    if x == nil {
      return WriteLine(wf, nil_value)
    } else {
      return WriteLine(wf, ":" + strconv.Itoa(x.(int)))
    }
  }
}

func main() {
  listen := flag.String("listen", "localhost:6379", "Listen string")
  aero_host := flag.String("aero_host", "localhost", "Aerospike server host")
  aero_port := flag.Int("aero_port", 3000, "Aerospike server port")
  ns := flag.String("ns", "test", "Aerospike namespace")
  set := flag.String("set", "redis", "Aerospike set")
  flag.Parse()
  l, err := net.Listen("tcp", *listen)
  panicOnError(err)

  fmt.Printf("Listening on %s\n", *listen)

  client, err := as.NewClient(*aero_host, *aero_port)
  panicOnError(err)

  fmt.Printf("Connected to aero on %s:%d\n", *aero_host, *aero_port)

  read_policy := as.NewPolicy()
  fillReadPolicy(read_policy)

  write_policy := as.NewWritePolicy(0, 0)
  fillWritePolicy(write_policy)

  ctx := context{client, *ns, *set, read_policy, write_policy}

  handlers := make(map[string]handler)
  handlers["DEL"] = handler{1, cmd_DEL}
  handlers["GET"] = handler{1, cmd_GET}
  handlers["SET"] = handler{2, cmd_SET}
  handlers["SETEX"] = handler{3, cmd_SETEX}
  handlers["SETNXEX"] = handler{3, cmd_SETNXEX}
  handlers["SETNX"] = handler{2, cmd_SETNX}
  handlers["LLEN"] = handler{1, cmd_LLEN}
  handlers["RPUSH"] = handler{2, cmd_RPUSH}
  handlers["LPUSH"] = handler{2, cmd_LPUSH}
  handlers["RPUSHEX"] = handler{3, cmd_RPUSHEX}
  handlers["LPUSHEX"] = handler{3, cmd_LPUSHEX}
  handlers["RPOP"] = handler{1, cmd_RPOP}
  handlers["LPOP"] = handler{1, cmd_LPOP}
  handlers["LRANGE"] = handler{3, cmd_LRANGE}
  handlers["LTRIM"] = handler{3, cmd_LTRIM}
  handlers["INCR"] = handler{1, cmd_INCR}
  handlers["INCRBY"] = handler{2, cmd_INCRBY}
  handlers["HINCRBY"] = handler{3, cmd_HINCRBY}
  handlers["HINCRBYEX"] = handler{4, cmd_HINCRBYEX}
  handlers["DECR"] = handler{1, cmd_DECR}
  handlers["DECRBY"] = handler{2, cmd_DECRBY}
  handlers["HGET"] = handler{2, cmd_HGET}
  handlers["HSET"] = handler{3, cmd_HSET}
  handlers["HDEL"] = handler{2, cmd_HDEL}
  handlers["HMGET"] = handler{3, cmd_HMGET}
  handlers["HMSET"] = handler{3, cmd_HMSET}
  handlers["HGETALL"] = handler{1, cmd_HGETALL}
  handlers["EXPIRE"] = handler{2, cmd_EXPIRE}
  handlers["TTL"] = handler{1, cmd_TTL}

  defer l.Close()
  for {
    conn, err := l.Accept()
    if err != nil {
      fmt.Println("Error accepting: ", err.Error())
    } else {
      go HandleRequest(conn, handlers, ctx)
    }
  }
}

func ReadLine(buf []byte, index int, l int) ([]byte, int) {
  for i := index; i < l - 1; i ++ {
    if buf[i] == '\r' && buf[i + 1] == '\n' {
      return buf[index:i], i + 2
    }
  }
  return nil, -1
}

func HandleRequest(conn net.Conn, handlers map[string]handler, ctx context) {
  var multi_buffer [][]byte
  multi_counter := 0
  multi_mode := false
  wf := func(buffer []byte) (error) {
    _, err := conn.Write(buffer)
    return err
  }
  sub_wf := func(buffer []byte) (error) {
    if multi_mode {
      multi_buffer = append(multi_buffer, buffer)
      return nil
    } else {
      return wf(buffer)
    }
  }
  buf := make([]byte, 1024)
  for {
    l, err := conn.Read(buf)
    if err != nil {
      fmt.Println("Error reading:", err.Error())
      conn.Close()
      return;
    }
    // fmt.Printf("Received command %v\n", string(buf[:l]))
    if l == 6 && string(buf[:l]) == "QUIT\r\n" {
      conn.Close()
      break
    }
    line, next := ReadLine(buf, 0, l)
    count := -1
    args := make([][]byte, 0)
    if len(line) > 0 && line[0] == '*' {
      number, err := strconv.Atoi(string(line[1:]))
      if err == nil {
        count = number
        args = make([][]byte, number)
        for i := 0; i < number; i ++ {
          line, next = ReadLine(buf, next, l)
          if line[0] == '$' {
            number, err := strconv.Atoi(string(line[1:]))
            if err == nil {
              if next + number > l {
                number += 2
                local_buf := make([]byte, number)
                copy(local_buf, buf[next:])
                current := l - next
                for ; current < number; {
                  l, err := conn.Read(local_buf[current:])
                  if err != nil {
                    break
                  }
                  current += l
                }
                if current == number {
                  args[i] = local_buf[:number - 2]
                  next = next + number + 2
                  count -= 1
                }
              } else {
                args[i] = buf[next:next + number]
                next = next + number + 2
                count -= 1
              }
            }
          } else {
            break
          }
        }
      }
    }
    if count != 0 {
      WriteErr(wf, "unable to parse")
      conn.Close()
      break
    }
    cmd := string(args[0])
    if cmd == "MULTI" {
      multi_counter = 0
      multi_buffer = multi_buffer[:0]
      WriteLine(wf, "+OK")
      multi_mode = true
    } else if cmd == "EXEC" {
      if multi_mode {
        multi_mode = false
        err := WriteLine(wf, "*" + strconv.Itoa(multi_counter))
        if err != nil {
          fmt.Printf("Client error : %s\n", err)
          break
        }
        on_err := false
        for _, b := range multi_buffer {
          err := wf(b)
          if err != nil {
            on_err = true
            fmt.Printf("Client error : %s\n", err)
            break
          }
        }
        if on_err {
          break
        }
      } else {
        WriteErr(wf, "Not in multi")
        break
      }
    } else {
      args = args[1:]
      h, ok := handlers[cmd]
      // fmt.Printf("Received %v\n", args)
      if ok {
        if h.args_count > len(args) {
          WriteErr(wf, fmt.Sprintf("wrong number of params for '%s': %d", cmd, len(args)))
          break
        } else {
          err := h.f(sub_wf, ctx, args)
          if err != nil {
            WriteErr(wf, fmt.Sprintf("Error '%s'", err))
            break
          }
          if multi_mode {
            multi_counter += 1
            err := WriteLine(wf, "+QUEUED")
            if err != nil {
              fmt.Printf("Client error : %s\n", err)
              break
            }
          }
        }
      } else {
        WriteErr(wf, fmt.Sprintf("unknown command '%s'", cmd))
        break
      }
    }
  }
  conn.Close()
}

func cmd_DEL(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  existed, err := ctx.client.Delete(ctx.write_policy, key)
  if err != nil  {
    return err
  }
  if existed {
    return WriteLine(wf, ":1")
  } else {
    return WriteLine(wf, ":0")
  }
}

func get(wf write_func, ctx context, k []byte, bin_name string) (error) {
  key, err := buildKey(ctx, k)
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, bin_name)
  if err != nil  {
    return err
  }
  return WriteBin(wf, rec, bin_name, "$-1")
}

func cmd_GET(wf write_func, ctx context, args [][]byte) (error) {
  return get(wf, ctx, args[0], BIN_NAME)
}

func cmd_HGET(wf write_func, ctx context, args [][]byte) (error) {
  return get(wf, ctx, args[0], string(args[1]))
}

func setex(wf write_func, ctx context, k []byte, bin_name string, content []byte, ttl int, create_only bool) (error) {
  key, err := buildKey(ctx, k)
  if err != nil {
    return err
  }
  rec := as.BinMap {
    bin_name: content,
  }
  err = ctx.client.Put(fillWritePolicyEx(ctx, ttl, create_only), key, rec)
  if err != nil  {
    if create_only && err.Error() == "Key already exists" {
      return WriteLine(wf, ":0")
    } else {
      return err
    }
  } else {
    if create_only {
      return WriteLine(wf, ":1")
    } else {
      return WriteLine(wf, "+OK")
    }
  }
}

func cmd_SET(wf write_func, ctx context, args [][]byte) (error) {
  return setex(wf, ctx, args[0], BIN_NAME, args[1], -1, false)
}

func cmd_SETEX(wf write_func, ctx context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }

  return setex(wf, ctx, args[0], BIN_NAME, args[2], ttl, false)
}

func cmd_SETNX(wf write_func, ctx context, args [][]byte) (error) {
  return setex(wf, ctx, args[0], BIN_NAME, args[1], -1, true)
}

func cmd_SETNXEX(wf write_func, ctx context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }

  return setex(wf, ctx, args[0], BIN_NAME, args[2], ttl, true)
}

func cmd_HSET(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HSET", as.NewValue(string(args[1])), as.NewValue(args[2]))
  if err != nil  {
    return err;
  }
  return WriteLine(wf, ":" + strconv.Itoa(rec.(int)))
}

func cmd_HDEL(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HDEL", as.NewValue(string(args[1])))
  if err != nil  {
    return err;
  }
  return WriteLine(wf, ":" + strconv.Itoa(rec.(int)))
}

func array_push(wf write_func, ctx context, args [][]byte, f string, ttl int) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, f, as.NewValue(BIN_NAME), as.NewValue(args[1]), as.NewValue(ttl))
  if err != nil  {
    return err;
  }
  return WriteLine(wf, ":" + strconv.Itoa(rec.(int)))
}

func cmd_RPUSH(wf write_func, ctx context, args [][]byte) (error) {
  return array_push(wf, ctx, args, "RPUSH", -1)
}

func cmd_RPUSHEX(wf write_func, ctx context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }

  return array_push(wf, ctx, args, "RPUSH", ttl)
}

func cmd_LPUSH(wf write_func, ctx context, args [][]byte) (error) {
  return array_push(wf, ctx, args, "LPUSH", -1)
}

func cmd_LPUSHEX(wf write_func, ctx context, args [][]byte) (error) {
  ttl, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }

  return array_push(wf, ctx, args, "LPUSH", ttl)
}

func array_pop(wf write_func, ctx context, args [][]byte, f string) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, f, as.NewValue(BIN_NAME), as.NewValue(1), as.NewValue(-1))
  if err != nil  {
    return err;
  }
  if rec == nil {
    return WriteLine(wf, "$-1")
  } else {
    return WriteByteArray(wf, rec.([]interface{})[0].([]byte))
  }
}

func cmd_RPOP(wf write_func, ctx context, args [][]byte) (error) {
  return array_pop(wf, ctx, args, "RPOP")
}

func cmd_LPOP(wf write_func, ctx context, args [][]byte) (error) {
  return array_pop(wf, ctx, args, "LPOP")
}

func cmd_LLEN(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, BIN_NAME + "_size")
  if err != nil  {
    return err
  }
  return WriteBinInt(wf, rec, BIN_NAME + "_size")
}

func cmd_LRANGE(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  start, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  stop, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "LRANGE", as.NewValue(BIN_NAME), as.NewValue(start), as.NewValue(stop))
  if err != nil  {
    return err;
  }
  if rec == nil {
    return WriteLine(wf, "$-1")
  } else {
    return WriteArray(wf, rec.([]interface{}))
  }
}

func cmd_LTRIM(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  start, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err
  }
  stop, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "LTRIM", as.NewValue(BIN_NAME), as.NewValue(start), as.NewValue(stop))
  if err != nil  {
    return err;
  }
  if rec == nil {
    return WriteLine(wf, "$-1")
  } else {
    return WriteLine(wf, "+OK")
  }
}

func hIncrByEx(wf write_func, ctx context, k []byte, field string, incr int, ttl int) (error) {
  key, err := buildKey(ctx, k)
  if err != nil {
    return err
  }
  bin := as.NewBin(field, incr)
  rec, err := ctx.client.Operate(fillWritePolicyEx(ctx, ttl, false), key, as.AddOp(bin), as.GetOpForBin(field))
  if err != nil  {
    if err.Error() == "Bin type error" {
      return WriteLine(wf, "$-1")
    } else {
      return err
    }
  }
  return WriteBinInt(wf, rec, field)
}

func cmd_INCR(wf write_func, ctx context, args [][]byte) (error) {
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, 1, -1)
}

func cmd_DECR(wf write_func, ctx context, args [][]byte) (error) {
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, -1, -1)
}

func cmd_INCRBY(wf write_func, ctx context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err;
  }
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, incr, -1)
}

func cmd_HINCRBY(wf write_func, ctx context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err;
  }
  return hIncrByEx(wf, ctx, args[0], string(args[1]), incr, -1)
}

func cmd_HINCRBYEX(wf write_func, ctx context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[2]))
  if err != nil {
    return err;
  }
  ttl, err := strconv.Atoi(string(args[3]))
  if err != nil {
    return err;
  }
  return hIncrByEx(wf, ctx, args[0], string(args[1]), incr, ttl)
}

func cmd_DECRBY(wf write_func, ctx context, args [][]byte) (error) {
  decr, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err;
  }
  return hIncrByEx(wf, ctx, args[0], BIN_NAME, -decr, -1)
}

func cmd_HMGET(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  a := make([]string, len(args) - 1)
  for i, e := range args[1:] {
    a[i] = string(e)
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, a...)
  if err != nil {
    return err;
  }
  err = WriteLine(wf, "*" + strconv.Itoa(len(a)))
  if err != nil {
    return err;
  }
  for _, e := range a {
    err = WriteBin(wf, rec, e, "$-1")
    if err != nil {
      return err;
    }
  }
  return nil
}

func cmd_HMSET(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  m := make(map[string][]byte)
  for i := 1; i < len(args); i += 2 {
    m[string(args[i])] = args[i + 1]
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HMSET", as.NewValue(m))
  if err != nil {
    return err;
  }
  return WriteLine(wf, "+" + rec.(string))
}


func cmd_HGETALL(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "HGETALL")
  if err != nil {
    return err;
  }
  a := rec.([]interface{})
  err = WriteLine(wf, "*" + strconv.Itoa(len(a)))
  if err != nil {
    return err;
  }
  for i := 0; i < len(a); i += 2 {
    err = WriteByteArray(wf, []byte(a[i].(string)))
    if err != nil {
      return err;
    }
    err = WriteValue(wf, a[i + 1])
    if err != nil {
      return err;
    }
  }
  return nil
}

func cmd_EXPIRE(wf write_func, ctx context, args [][]byte) (error) {
    key, err := buildKey(ctx, args[0])
    if err != nil {
      return err
    }

    ttl, err := strconv.Atoi(string(args[1]))
    if err != nil {
      return err
    }

    err = ctx.client.Touch(fillWritePolicyEx(ctx, ttl, false), key)
    if err != nil {
      if err.Error() == "Key not found" {
        return WriteLine(wf, ":0")
      } else {
        return err
      }
    } else {
      return WriteLine(wf, ":1")
    }

}

func cmd_TTL(wf write_func, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }

  rec, err := ctx.client.GetHeader(ctx.read_policy, key)
  if err != nil {
    return err
  } else {
    if rec == nil {
      return WriteLine(wf, ":-2")
    } else {
      return WriteLine(wf, ":" + strconv.FormatUint(uint64(rec.Expiration), 10))
    }
  }
}
