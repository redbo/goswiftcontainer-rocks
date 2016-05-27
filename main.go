package main

import (
	"fmt"
	"log/syslog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
	"encoding/json"

	"github.com/DanielMorsing/rocksdb"
	"github.com/vmihailenco/msgpack"
)

type ContainerServer struct {
	driveRoot      string
	hashPathPrefix string
	hashPathSuffix string
	checkMounts    bool
	logger         *syslog.Writer
}

type ContainerDB struct {
	*rocksdb.DB
	ro *rocksdb.ReadOptions
	wo *rocksdb.WriteOptions
}

func (db ContainerDB) Release() {
	if db.ro != nil {
		db.ro.Close()
	}
	if db.wo != nil {
		db.wo.Close()
	}
	db.Close()
}

func (db ContainerDB) GetMap(key []byte) (map[string]interface{}, error) {
	val, err := db.Get(db.ro, key)
	if err != nil || len(val) == 0 {
		return nil, err
	}
	var info map[string]interface{}
	err = msgpack.Unmarshal(val, &info)
	return info, nil
}

func (server ContainerServer) getDB(vars map[string]string, create bool) (ContainerDB, error) {
	opts := rocksdb.NewOptions()
	//	opts.SetCache(rocksdb.NewLRUCache(3 << 30))
	opts.SetCreateIfMissing(create)
	// return new(ContainerDB, rocksdb.Open("/tmp/containerdb", opts))
	db, err := rocksdb.Open("/tmp/containerdb", opts)
	if err != nil {
		return ContainerDB{nil, nil, nil}, err
	}
	ro := rocksdb.NewReadOptions()
	wo := rocksdb.NewWriteOptions()
	return ContainerDB{db, ro, wo}, nil
}

func (server ContainerServer) ContainerGetHandler(writer http.ResponseWriter, request *http.Request, vars map[string]string) {
	start := []byte("o")
	db, err := server.getDB(vars, false)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	it := db.NewIterator(db.ro)
	defer it.Close()
	it.Seek(start)
	count := 0
	writer.Write([]byte("[\n"))
	for it = it; it.Valid() && count < 10000 && it.Key()[0] == 'o'; it.Next() {
		var out map[string]interface{}
		err = msgpack.Unmarshal(it.Value(), &out)
		if err != nil {
			return
		}
		data, err := json.Marshal(out)
		if err != nil {
			return
		}
		writer.Write(data)
		writer.Write([]byte("\n"))
		count += 1
	}
	writer.Write([]byte("]\n"))
}

func (server ContainerServer) ContainerHeadHandler(writer http.ResponseWriter, request *http.Request, vars map[string]string) {
	db, err := server.getDB(vars, true)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	// TODO: fill in headers
	http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
}

func (server ContainerServer) ContainerPutHandler(writer http.ResponseWriter, request *http.Request, vars map[string]string) {
	db, err := server.getDB(vars, true)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	defer db.Release()
	// TODO: create status record
	http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
}

func (server ContainerServer) ContainerDeleteHandler(writer http.ResponseWriter, request *http.Request, vars map[string]string) {
	opts := rocksdb.NewOptions()
	err := rocksdb.DestroyDatabase("/tmp/containerdb", opts)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
	} else {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
	}
}

func (server ContainerServer) ObjPutHandler(writer http.ResponseWriter, request *http.Request, vars map[string]string) {
	db, err := server.getDB(vars, false)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	x_timestamp, err := strconv.ParseFloat(request.Header.Get("X-Timestamp"), 64)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	info, err := db.GetMap([]byte("o" + vars["obj"]))
	if err != nil {
		info, err = db.GetMap([]byte("t" + vars["obj"]))
	}
	if info != nil {
		ts, err := strconv.ParseFloat(info["created_at"].(string), 64)
		if err == nil && ts > x_timestamp {
			http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
			return
		}
	}
	record := map[string]interface{}{
		"created_at": request.Header.Get("X-Timestamp"),
		"content_type": request.Header.Get("X-Content-Type"),
		"etag": request.Header.Get("X-Etag"),
		"size": request.Header.Get("X-Size"),
		"name":        vars["obj"],
	}
	recordval, err := msgpack.Marshal(record)
	if err != nil {
		fmt.Println("WTF")
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	wb := rocksdb.NewWriteBatch()
	wb.Delete([]byte("t" + vars["obj"]))
	wb.Put([]byte("o"+vars["obj"]), recordval)
	db.Write(db.wo, wb)
	http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
}

func (server ContainerServer) ObjDeleteHandler(writer http.ResponseWriter, request *http.Request, vars map[string]string) {
	db, err := server.getDB(vars, false)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	defer db.Release()
	x_timestamp, err := strconv.ParseFloat(request.Header.Get("X-Timestamp"), 64)
	if err != nil {
		http.Error(writer, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	info, err := db.GetMap([]byte("o" + vars["obj"]))
	if err != nil {
		info, err = db.GetMap([]byte("t" + vars["obj"]))
	}
	if info != nil {
		ts, err := strconv.ParseFloat(info["created_at"].(string), 64)
		if err == nil && ts > x_timestamp {
			http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
			return
		}
	}
	record := map[string]interface{}{
		"created_at": request.Header.Get("X-Timestamp"),
	}
	recordval, err := msgpack.Marshal(record)
	if err != nil {
		fmt.Println("WTF")
		http.Error(writer, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	wb := rocksdb.NewWriteBatch()
	wb.Delete([]byte("o" + vars["obj"]))
	wb.Put([]byte("t"+vars["obj"]), recordval)
	db.Write(db.wo, wb)
	http.Error(writer, http.StatusText(http.StatusOK), http.StatusOK)
}

func GetDefault(h http.Header, key string, dfl string) string {
	val := h.Get(key)
	if val == "" {
		return dfl
	}
	return val
}

type SaveStatusWriter struct {
	http.ResponseWriter
	Status int
}

func (w *SaveStatusWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.Status = status
}

func (server ContainerServer) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/healthcheck" {
		writer.Header().Set("Content-Length", "2")
		writer.WriteHeader(http.StatusOK)
		writer.Write([]byte("OK"))
		return
	}
	parts := strings.SplitN(request.URL.Path, "/", 6)
	vars := make(map[string]string)
	if len(parts) > 1 {
		vars["device"] = parts[1]
		if len(parts) > 2 {
			vars["partition"] = parts[2]
			if len(parts) > 3 {
				vars["account"] = parts[3]
				vars["suffixes"] = parts[3]
				if len(parts) > 4 {
					vars["container"] = parts[4]
					if len(parts) > 5 {
						vars["obj"] = parts[5]
					}
				}
			}
		}
	}
	start := time.Now()
	newWriter := &SaveStatusWriter{writer, 200}
	if len(parts) == 5 {
		switch request.Method {
		case "GET":
			server.ContainerGetHandler(newWriter, request, vars)
		case "HEAD":
			server.ContainerHeadHandler(newWriter, request, vars)
		case "PUT":
			server.ContainerPutHandler(newWriter, request, vars)
		case "DELETE":
			server.ContainerDeleteHandler(newWriter, request, vars)
		}
	} else if len(parts) == 6 {
		switch request.Method {
		case "PUT":
			server.ObjPutHandler(newWriter, request, vars)
		case "DELETE":
			server.ObjDeleteHandler(newWriter, request, vars)
		}
	}

	server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
		request.RemoteAddr,
		time.Now().Format("02/Jan/2006:15:04:05 -0700"),
		request.Method,
		request.URL.Path,
		newWriter.Status,
		GetDefault(writer.Header(), "Content-Length", "-"),
		GetDefault(request.Header, "Referer", "-"),
		GetDefault(request.Header, "X-Trans-Id", "-"),
		GetDefault(request.Header, "User-Agent", "-"),
		time.Since(start).Seconds(),
		"-")) // TODO: "additional info"
}

func RunServer(conf string) {
	server := ContainerServer{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		checkMounts: true,
	}

	if swiftconf, err := LoadIniFile("/etc/swift/swift.conf"); err == nil {
		server.hashPathPrefix = swiftconf.GetDefault("swift-hash", "swift_hash_path_prefix", "")
		server.hashPathSuffix = swiftconf.GetDefault("swift-hash", "swift_hash_path_suffix", "")
	}

	serverconf, err := LoadIniFile(conf)
	if err != nil {
		panic(fmt.Sprintf("Unable to load %s", conf))
	}
	server.driveRoot = serverconf.GetDefault("DEFAULT", "devices", "/srv/node")
	server.checkMounts = LooksTrue(serverconf.GetDefault("DEFAULT", "mount_check", "true"))
	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort, err := strconv.ParseInt(serverconf.GetDefault("DEFAULT", "bind_port", "8080"), 10, 64)
	if err != nil {
		panic("Invalid bind port format")
	}

	sock, err := net.Listen("tcp", fmt.Sprintf("%s:%d", bindIP, bindPort))
	if err != nil {
		panic(fmt.Sprintf("Unable to bind %s:%d", bindIP, bindPort))
	}
	server.logger = SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "object-server")
	DropPrivileges(serverconf.GetDefault("DEFAULT", "user", "swift"))
	srv := &http.Server{Handler: server}
	srv.Serve(sock)
}

func main() {
	if os.Args[1] == "saio" {
		go RunServer("/etc/swift/container-server/1.conf")
		go RunServer("/etc/swift/container-server/2.conf")
		go RunServer("/etc/swift/container-server/3.conf")
		go RunServer("/etc/swift/container-server/4.conf")
		for {
			time.Sleep(10000)
		}
	}
	RunServer(os.Args[1])
}
