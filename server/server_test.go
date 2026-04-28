package server

import (
	"bufio"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	valkyrAOF "github.com/kartik/valkyr/aof"
	"github.com/kartik/valkyr/config"
	"github.com/kartik/valkyr/resp"
)

func startTestServer(t *testing.T) (*Server, string) {
	tmpDir, err := os.MkdirTemp("", "valkyr-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	cfg := config.DefaultConfig()
	cfg.Port = 16379
	cfg.Bind = "127.0.0.1"
	cfg.AOFPath = filepath.Join(tmpDir, "test.aof")

	srv := NewServer(cfg)

	aofFile, err := valkyrAOF.New(cfg.AOFPath)
	if err != nil {
		t.Fatalf("failed to create AOF file: %v", err)
	}
	srv.SetAOFWriter(aofFile)

	go func() {
		if err := srv.Start(); err != nil {
			// server closed/stopped is fine
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	return srv, tmpDir
}

func sendCommand(conn net.Conn, args ...string) (resp.Value, error) {
	w := resp.NewWriter(bufio.NewWriter(conn))
	var valArgs []resp.Value
	for _, arg := range args {
		valArgs = append(valArgs, resp.BulkStringValue(arg))
	}
	if err := w.WriteValue(resp.ArrayValue(valArgs)); err != nil {
		return resp.Value{}, err
	}
	if err := w.Flush(); err != nil {
		return resp.Value{}, err
	}

	r := resp.NewReader(bufio.NewReader(conn))
	return r.ReadValue()
}

func TestServerIntegration(t *testing.T) {
	srv, tmpDir := startTestServer(t)
	defer func() {
		srv.Shutdown()
		os.RemoveAll(tmpDir)
	}()

	conn, err := net.Dial("tcp", "127.0.0.1:16379")
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// 1. Test PING
	res, err := sendCommand(conn, "PING")
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if res.Str != "PONG" {
		t.Errorf("expected PONG, got %s", res.Str)
	}

	// 2. Test SET & GET
	res, err = sendCommand(conn, "SET", "mykey", "myval")
	if err != nil || res.Str != "OK" {
		t.Errorf("SET failed: %v, res: %+v", err, res)
	}

	res, err = sendCommand(conn, "GET", "mykey")
	if err != nil || res.Str != "myval" {
		t.Errorf("GET failed: %v, res: %+v", err, res)
	}

	// 3. Test Sorted Sets (ZADD, ZRANGE)
	res, err = sendCommand(conn, "ZADD", "myzset", "10", "memA", "20", "memB")
	if err != nil || res.Num != 2 {
		t.Errorf("ZADD failed: %v, res: %+v", err, res)
	}

	res, err = sendCommand(conn, "ZRANGE", "myzset", "0", "-1", "WITHSCORES")
	if err != nil || len(res.Array) != 4 {
		t.Errorf("ZRANGE failed: %v, res: %+v", err, res)
	}
	if res.Array[0].Str != "memA" || res.Array[1].Str != "10" {
		t.Errorf("expected memA (10), got %s (%s)", res.Array[0].Str, res.Array[1].Str)
	}

	// 4. Test Transactions
	res, err = sendCommand(conn, "MULTI")
	if err != nil || res.Str != "OK" {
		t.Errorf("MULTI failed: %v", err)
	}
	res, err = sendCommand(conn, "SET", "txkey", "txval")
	if err != nil || res.Str != "QUEUED" {
		t.Errorf("queueing failed: %v", err)
	}
	res, err = sendCommand(conn, "EXEC")
	if err != nil || len(res.Array) != 1 || res.Array[0].Str != "OK" {
		t.Errorf("EXEC failed: %v, res: %+v", err, res)
	}
}

func TestServerPubSub(t *testing.T) {
	srv, tmpDir := startTestServer(t)
	defer func() {
		srv.Shutdown()
		os.RemoveAll(tmpDir)
	}()

	// Subscriber client
	subConn, err := net.Dial("tcp", "127.0.0.1:16379")
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer subConn.Close()

	// Publisher client
	pubConn, err := net.Dial("tcp", "127.0.0.1:16379")
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer pubConn.Close()

	// 1. Subscribe to channel
	subVal, err := sendCommand(subConn, "SUBSCRIBE", "testchan")
	if err != nil {
		t.Fatalf("SUBSCRIBE failed: %v", err)
	}
	if len(subVal.Array) != 3 || subVal.Array[0].Str != "subscribe" || subVal.Array[1].Str != "testchan" {
		t.Errorf("unexpected subscribe response: %+v", subVal)
	}

	// 2. Publish message
	pubVal, err := sendCommand(pubConn, "PUBLISH", "testchan", "hello world")
	if err != nil {
		t.Fatalf("PUBLISH failed: %v", err)
	}
	if pubVal.Num != 1 {
		t.Errorf("expected 1 receiver, got %d", pubVal.Num)
	}

	// 3. Verify message received by subscriber
	reader := resp.NewReader(bufio.NewReader(subConn))
	msgVal, err := reader.ReadValue()
	if err != nil {
		t.Fatalf("failed to read message: %v", err)
	}
	if len(msgVal.Array) != 3 || msgVal.Array[0].Str != "message" || msgVal.Array[1].Str != "testchan" || msgVal.Array[2].Str != "hello world" {
		t.Errorf("unexpected received message: %+v", msgVal)
	}
}

func TestServerAOFCompaction(t *testing.T) {
	srv, tmpDir := startTestServer(t)
	defer func() {
		srv.Shutdown()
		os.RemoveAll(tmpDir)
	}()

	conn, err := net.Dial("tcp", "127.0.0.1:16379")
	if err != nil {
		t.Fatalf("failed to dial server: %v", err)
	}
	defer conn.Close()

	// 1. Set some keys
	res, err := sendCommand(conn, "SET", "keyA", "valA")
	if err != nil || res.Str != "OK" {
		t.Errorf("SET failed: %v", err)
	}

	res, err = sendCommand(conn, "SET", "keyB", "valB")
	if err != nil || res.Str != "OK" {
		t.Errorf("SET failed: %v", err)
	}

	// 2. Trigger BGREWRITEAOF
	res, err = sendCommand(conn, "BGREWRITEAOF")
	if err != nil || res.Str != "Background append only file rewriting started" {
		t.Errorf("BGREWRITEAOF failed: %v, res: %+v", err, res)
	}

	// Wait for background rewrite to complete
	time.Sleep(200 * time.Millisecond)

	// Check if AOF file exists and has content
	aofPath := filepath.Join(tmpDir, "test.aof")
	fi, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("AOF file does not exist: %v", err)
	}
	if fi.Size() == 0 {
		t.Errorf("AOF file is empty")
	}

	// 3. Restart server and verify replay
	srv.Shutdown()
	time.Sleep(100 * time.Millisecond)

	// Spin up a new server reading from the same AOF file
	cfg := config.DefaultConfig()
	cfg.Port = 16380
	cfg.Bind = "127.0.0.1"
	cfg.AOFPath = aofPath

	newSrv := NewServer(cfg)
	newAOF, err := valkyrAOF.New(aofPath)
	if err != nil {
		t.Fatalf("failed to reopen AOF: %v", err)
	}

	err = newAOF.Replay(newSrv.DispatchCommand)
	if err != nil {
		t.Fatalf("AOF replay failed: %v", err)
	}
	newSrv.SetAOFWriter(newAOF)

	go func() {
		_ = newSrv.Start()
	}()
	defer newSrv.Shutdown()

	time.Sleep(100 * time.Millisecond)

	newConn, err := net.Dial("tcp", "127.0.0.1:16380")
	if err != nil {
		t.Fatalf("failed to dial new server: %v", err)
	}
	defer newConn.Close()

	res, err = sendCommand(newConn, "GET", "keyA")
	if err != nil || res.Str != "valA" {
		t.Errorf("GET keyA failed on restarted server: %v, res: %+v", err, res)
	}
}
