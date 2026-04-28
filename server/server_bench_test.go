package server

import (
	"strconv"
	"testing"

	"github.com/kartik/valkyr/config"
	"github.com/kartik/valkyr/resp"
)

func BenchmarkStoreSET(b *testing.B) {
	cfg := config.DefaultConfig()
	srv := NewServer(cfg)
	st := srv.Store()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key-" + strconv.Itoa(i)
		st.Strings.Set(key, "value")
	}
}

func BenchmarkStoreGET(b *testing.B) {
	cfg := config.DefaultConfig()
	srv := NewServer(cfg)
	st := srv.Store()

	for i := 0; i < 1000; i++ {
		st.Strings.Set("key-"+strconv.Itoa(i), "value")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st.Strings.Get("key-" + strconv.Itoa(i%1000))
	}
}

func BenchmarkDispatchPING(b *testing.B) {
	cfg := config.DefaultConfig()
	srv := NewServer(cfg)

	pingCmd := []resp.Value{resp.BulkStringValue("PING")}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.DispatchCommand(pingCmd)
	}
}
