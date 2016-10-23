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

type handler struct {
  args_count int
  f func(net.Conn, context, [][]byte) (error)
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

func buildKey(ctx context, key []byte) (*as.Key, error) {
  return as.NewKey(ctx.ns, ctx.set, key)
}

func panicOnError(err error) {
  if err != nil {
    panic(err)
  }
}

func WriteErr(conn net.Conn, s string) bool {
  fmt.Printf("Client error : %s\n", s)
  conn.Write([]byte("-ERR " + s + "\n"))
  return false
}

func WriteByteArray(conn net.Conn, buf []byte) error {
  conn.Write([]byte("$" + strconv.Itoa(len(buf)) + "\r\n"))
  conn.Write(buf)
  conn.Write([]byte("\r\n"))
  return nil
}

func WriteArray(conn net.Conn, array []interface{}) error {
  err := WriteLine(conn, "*" + strconv.Itoa(len(array)))
  if err != nil {
    return err
  }
  for _, e := range array {
    err := WriteByteArray(conn, e.([]byte))
    if err != nil {
      return err
    }
  }
  return nil
}

func WriteLine(conn net.Conn, s string) error {
  conn.Write([]byte(s + "\r\n"))
  return nil
}

func WriteBin(conn net.Conn, rec * as.Record, bin_name string, nil_value string) error {
  if rec == nil {
    return WriteLine(conn, nil_value)
  } else {
    x := rec.Bins[bin_name]
    if x == nil {
      return WriteLine(conn, nil_value)
    } else {
      if reflect.TypeOf(x).Kind() == reflect.Int {
        return WriteByteArray(conn, []byte(strconv.Itoa(x.(int))))
      } else {
        return WriteByteArray(conn, x.([]byte))
      }
    }
  }
}

func WriteBinInt(conn net.Conn, rec * as.Record, bin_name string) error {
  nil_value := ":0"
  if rec == nil {
    return WriteLine(conn, nil_value)
  } else {
    x := rec.Bins[bin_name]
    if x == nil {
      return WriteLine(conn, nil_value)
    } else {
      return WriteLine(conn, ":" + strconv.Itoa(x.(int)))
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
  handlers["LLEN"] = handler{1, cmd_LLEN}
  handlers["RPUSH"] = handler{2, cmd_RPUSH}
  handlers["LPUSH"] = handler{2, cmd_LPUSH}
  handlers["RPOP"] = handler{1, cmd_RPOP}
  handlers["LPOP"] = handler{1, cmd_LPOP}
  handlers["LRANGE"] = handler{3, cmd_LRANGE}
  handlers["LTRIM"] = handler{3, cmd_LTRIM}
  handlers["INCR"] = handler{1, cmd_INCR}
  handlers["INCRBY"] = handler{2, cmd_INCRBY}
  handlers["DECR"] = handler{1, cmd_DECR}
  handlers["DECRBY"] = handler{2, cmd_DECRBY}

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
      WriteErr(conn, "unable to parse")
      conn.Close()
      break
    }
    cmd := string(args[0])
    args = args[1:]
    h, ok := handlers[cmd]
    if ok {
      if h.args_count != len(args) {
        WriteErr(conn, fmt.Sprintf("wrong number of params for '%s': %d", cmd, len(args)))
        conn.Close()
        break
      } else {
        err := h.f(conn, ctx, args)
        if err != nil {
          WriteErr(conn, fmt.Sprintf("Error '%s'", err))
          conn.Close()
          break
        }
      }
    } else {
      WriteErr(conn, fmt.Sprintf("unknown command '%s'", cmd))
      conn.Close()
      break
    }
  }
}

func cmd_DEL(conn net.Conn, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  existed, err := ctx.client.Delete(ctx.write_policy, key)
  if err != nil  {
    return err
  }
  if existed {
    return WriteLine(conn, ":1")
  } else {
    return WriteLine(conn, ":0")
  }
}

func cmd_GET(conn net.Conn, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, BIN_NAME)
  if err != nil  {
    return err
  }
  return WriteBin(conn, rec, BIN_NAME, "$-1")
}

func setex(conn net.Conn, ctx context, k []byte, content []byte, ttl int) (error) {
  key, err := buildKey(ctx, k)
  if err != nil {
    return err
  }
  rec := as.BinMap {
    BIN_NAME: content,
  }
  policy := ctx.write_policy
  if ttl != -1 {
    policy = as.NewWritePolicy(0, uint32(ttl))
    fillWritePolicy(policy)
  }
  err = ctx.client.Put(policy, key, rec)
  if err != nil  {
    return err
  } else {
    return WriteLine(conn, "+OK")
  }
}

func cmd_SET(conn net.Conn, ctx context, args [][]byte) (error) {
  return setex(conn, ctx, args[0], args[1], -1)
}

func array_push(conn net.Conn, ctx context, args [][]byte, f string) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, f, as.NewValue(BIN_NAME), as.NewValue(args[1]), as.NewValue("-1"));
  if err != nil  {
    return err;
  }
  return WriteLine(conn, ":" + strconv.Itoa(rec.(int)))
}

func cmd_RPUSH(conn net.Conn, ctx context, args [][]byte) (error) {
  return array_push(conn, ctx, args, "RPUSH")
}

func cmd_LPUSH(conn net.Conn, ctx context, args [][]byte) (error) {
  return array_push(conn, ctx, args, "LPUSH")
}

func array_pop(conn net.Conn, ctx context, args [][]byte, f string) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, f, as.NewValue(BIN_NAME), as.NewValue(1), as.NewValue(-1));
  if err != nil  {
    return err;
  }
  if rec == nil {
    return WriteLine(conn, "$-1")
  } else {
    return WriteByteArray(conn, rec.([]interface{})[0].([]byte))
  }
}

func cmd_RPOP(conn net.Conn, ctx context, args [][]byte) (error) {
  return array_pop(conn, ctx, args, "RPOP")
}

func cmd_LPOP(conn net.Conn, ctx context, args [][]byte) (error) {
  return array_pop(conn, ctx, args, "LPOP")
}

func cmd_LLEN(conn net.Conn, ctx context, args [][]byte) (error) {
  key, err := buildKey(ctx, args[0])
  if err != nil {
    return err
  }
  rec, err := ctx.client.Get(ctx.read_policy, key, BIN_NAME + "_size")
  if err != nil  {
    return err
  }
  return WriteBinInt(conn, rec, BIN_NAME + "_size")
}

func cmd_LRANGE(conn net.Conn, ctx context, args [][]byte) (error) {
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
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "LRANGE", as.NewValue(BIN_NAME), as.NewValue(start), as.NewValue(stop));
  if err != nil  {
    return err;
  }
  if rec == nil {
    return WriteLine(conn, "$-1")
  } else {
    return WriteArray(conn, rec.([]interface{}))
  }
}

func cmd_LTRIM(conn net.Conn, ctx context, args [][]byte) (error) {
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
  rec, err := ctx.client.Execute(ctx.write_policy, key, module_name, "LTRIM", as.NewValue(BIN_NAME), as.NewValue(start), as.NewValue(stop));
  if err != nil  {
    return err;
  }
  if rec == nil {
    return WriteLine(conn, "$-1")
  } else {
    return WriteLine(conn, "+OK")
  }
}

func hIncrByEx(conn net.Conn, ctx context, k []byte, field string, incr int, ttl int) (error) {
  key, err := buildKey(ctx, k)
  if err != nil {
    return err
  }
  bin := as.NewBin(field, incr)
  rec, err := ctx.client.Operate(ctx.write_policy, key, as.AddOp(bin), as.GetOpForBin(field));
  if err != nil  {
    return err;
  }
  return WriteBinInt(conn, rec, field)
}

func cmd_INCR(conn net.Conn, ctx context, args [][]byte) (error) {
  return hIncrByEx(conn, ctx, args[0], BIN_NAME, 1, -1)
}

func cmd_DECR(conn net.Conn, ctx context, args [][]byte) (error) {
  return hIncrByEx(conn, ctx, args[0], BIN_NAME, -1, -1)
}

func cmd_INCRBY(conn net.Conn, ctx context, args [][]byte) (error) {
  incr, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err;
  }
  return hIncrByEx(conn, ctx, args[0], BIN_NAME, incr, -1)
}

func cmd_DECRBY(conn net.Conn, ctx context, args [][]byte) (error) {
  decr, err := strconv.Atoi(string(args[1]))
  if err != nil {
    return err;
  }
  return hIncrByEx(conn, ctx, args[0], BIN_NAME, -decr, -1)
}