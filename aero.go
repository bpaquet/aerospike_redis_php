package main

import (
  "io"
  "io/ioutil"
  "net/http"
  "reflect"
  "fmt"
  "strconv"
  "strings"

  as "github.com/aerospike/aerospike-client-go"
)

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

func export(w http.ResponseWriter, data interface{}, code_if_null int) {
  if data == nil {
    w.WriteHeader(code_if_null)
  } else {
    t := reflect.TypeOf(data).Kind()
    if t == reflect.Int {
      w.Header().Set("X-Aero-Type", "int")
      io.WriteString(w, fmt.Sprintf("%d", data.(int)))
    } else if t == reflect.Slice {
      w.Header().Set("X-Aero-Type", "array")
      array := data.([]interface{})
      for _, element := range array {
        if reflect.TypeOf(element).Kind() == reflect.Int {
          element = fmt.Sprintf("%d", element)
        }
        element := element.(string)
        size :=  fmt.Sprintf("%08x", len(element))
        io.WriteString(w, size + element)
      }
    } else {
      io.WriteString(w, data.(string))
    }
  }
}

func extractParam(name string, r *http.Request) (interface{}, error) {
  p := r.URL.Query().Get(name)
  if p == "__body__" {
    body, err := ioutil.ReadAll(r.Body)
    if err != nil {
      return "", err
    } else {
      return string(body), err
    }
  } else {
    if strings.HasPrefix(p, "__int__") {
      return strconv.Atoi(p[7:len(p)])
    } else {
      return p, nil
    }
  }
}

func main() {
  read_policy := as.NewPolicy()
  fillReadPolicy(read_policy)

  write_policy := as.NewWritePolicy(0, 0)
  fillWritePolicy(write_policy)

  client, err := as.NewClient("192.168.56.80", 3000)
  panicOnError(err)

  http.HandleFunc("/put", func(w http.ResponseWriter, r *http.Request) {
    bin_name := r.URL.Query().Get("bin")
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
      fillWritePolicy(policy)
    }

    err = client.Put(policy, key, rec)
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }
    w.WriteHeader(204)
  });

  http.HandleFunc("/touch", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    ttl := r.URL.Query().Get("ttl")
    i, err := strconv.Atoi(ttl)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    policy := as.NewWritePolicy(0, uint32(i))
    fillWritePolicy(policy)

    err = client.Touch(policy, key)
    if err != nil {
      if err.Error() == "Key not found" {
        w.WriteHeader(404)
      } else {
        http.Error(w, err.Error(), 500)
        return
      }
    } else {
      w.WriteHeader(204)
    }
  });

  http.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    bin_name := r.URL.Query().Get("bin")

    rec, err := client.Get(read_policy, key, bin_name)
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    if rec == nil {
      w.WriteHeader(404)
    } else {
      export(w, rec.Bins[bin_name], 404)
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

  http.HandleFunc("/udf_0", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    query := r.URL.Query()
    package_name := query.Get("package")
    function_name := query.Get("function")

    rec, err := client.Execute(write_policy, key, package_name, function_name);
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    export(w, rec, 204)
  });


  http.HandleFunc("/udf_1", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    query := r.URL.Query()
    package_name := query.Get("package")
    function_name := query.Get("function")
    param, err := extractParam("p1", r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    rec, err := client.Execute(write_policy, key, package_name, function_name, as.NewValue(param));
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    export(w, rec, 204)
  });

  http.HandleFunc("/udf_2", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    query := r.URL.Query()
    package_name := query.Get("package")
    function_name := query.Get("function")

    p1, err := extractParam("p1", r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    p2, err := extractParam("p2", r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    rec, err := client.Execute(write_policy, key, package_name, function_name, as.NewValue(p1), as.NewValue(p2));
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    export(w, rec, 204)
  });

  http.HandleFunc("/udf_3", func(w http.ResponseWriter, r *http.Request) {
    key, err := buildKey(r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    query := r.URL.Query()
    package_name := query.Get("package")
    function_name := query.Get("function")

    p1, err := extractParam("p1", r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    p2, err := extractParam("p2", r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    p3, err := extractParam("p3", r)
    if err != nil {
      http.Error(w, err.Error(), 500)
      return
    }

    rec, err := client.Execute(write_policy, key, package_name, function_name, as.NewValue(p1), as.NewValue(p2), as.NewValue(p3));
    if err != nil  {
      http.Error(w, err.Error(), 500)
      return
    }

    export(w, rec, 204)
  });

  fmt.Printf("Ready.\n");


  http.ListenAndServe(":8000", nil)
}