package main

import (
  "io"
  "io/ioutil"
  "net/http"
  "reflect"
  "fmt"
  "strconv"

  as "github.com/aerospike/aerospike-client-go"
)

const bin_name = "r"

func panicOnError(err error) {
  if err != nil {
    panic(err)
  }
}

func buildKey(r *http.Request) (*as.Key, error) {
  query := r.URL.Query()
  return as.NewKey(query.Get("namespace"), query.Get("set"), query.Get("key"))
}

func fillReadPolicy(read_policy * as.BasePolicy) {
  read_policy.ConsistencyLevel = as.CONSISTENCY_ONE
  read_policy.ReplicaPolicy = as.MASTER_PROLES
}

func fillWritePolicy(write_policy * as.WritePolicy) {
  write_policy.CommitLevel = as.COMMIT_MASTER
}

func main() {
  read_policy := as.NewPolicy()
  fillReadPolicy(read_policy)

  write_policy := as.NewWritePolicy(0, 0)
  fillWritePolicy(write_policy)

  client, err := as.NewClient("192.168.56.80", 3000)
  panicOnError(err)

  http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }
    rec := as.BinMap {
      bin_name: string(body),
    }

    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    policy := write_policy
    ttl := r.URL.Query().Get("ttl")
    if ttl != "" {
      i, err := strconv.Atoi(ttl)
      if err != nil {
        http.Error(w, err.Error(), 500)
        return
      }
      policy = as.NewWritePolicy(0, uint32(i))
      fillWritePolicy(write_policy)
    }

    err = client.Put(policy, key, rec)
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }
    w.WriteHeader(204)
  });

  http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    rec, err := client.Get(read_policy, key, bin_name)
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    if rec == nil {
      w.WriteHeader(204)
    } else {
      data := rec.Bins[bin_name]
      if reflect.TypeOf(data).Kind() == reflect.Int {
        w.Header().Set("X-Aero-Type", "int")
        io.WriteString(w, fmt.Sprintf("%d", data.(int)))
      } else {
        io.WriteString(w, data.(string))
      }
    }
  });

  http.HandleFunc("/exists", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    rec, err := client.GetHeader(read_policy, key)
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    if rec != nil {
      io.WriteString(w, fmt.Sprintf("%d", rec.Expiration))
    } else {
      w.WriteHeader(404)
    }
  });

  http.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    existed, err := client.Delete(write_policy, key)
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    if existed {
      w.WriteHeader(204)
    } else {
      w.WriteHeader(404)
    }
  });

  http.HandleFunc("/udf_1", func(w http.ResponseWriter, r *http.Request) {
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    query := r.URL.Query()
    package_name := query.Get("package")
    function_name := query.Get("function")
    param := string(body)

    rec, err := client.Execute(write_policy, key, package_name, function_name, as.NewValue(param));
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    if rec == nil {
      w.WriteHeader(204)
    } else {
      if reflect.TypeOf(rec).Kind() == reflect.Int {
        w.Header().Set("X-Aero-Type", "int")
        io.WriteString(w, fmt.Sprintf("%d", rec.(int)))
      } else {
        io.WriteString(w, rec.(string))
      }
    }
  });

  fmt.Printf("Ready.\n");


  http.ListenAndServe(":8000", nil)
}