package server

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/kartik/valkyr/resp"
	"github.com/kartik/valkyr/store"
)

// HandlerFunc is the signature for command handler functions.
// It receives the command arguments (excluding the command name) and returns a RESP Value.
type HandlerFunc func(args []resp.Value) resp.Value

// Router maps command names to their handler functions and dispatches incoming commands.
type Router struct {
	handlers map[string]HandlerFunc
	server   *Server
}

// writeCommands is the set of commands that mutate state and should be logged to AOF.
var writeCommands = map[string]bool{
	"SET": true, "DEL": true, "MSET": true, "APPEND": true,
	"INCR": true, "DECR": true, "INCRBY": true,
	"HSET": true, "HDEL": true, "HMSET": true,
	"LPUSH": true, "RPUSH": true, "LPOP": true, "RPOP": true, "LSET": true,
	"SADD": true, "SREM": true,
	"EXPIRE": true, "EXPIREAT": true, "PERSIST": true,
	"FLUSHDB": true, "RENAME": true,
}

// NewRouter creates a new Router and registers all command handlers.
func NewRouter(s *Server) *Router {
	r := &Router{
		handlers: make(map[string]HandlerFunc),
		server:   s,
	}
	r.registerAll()
	return r
}

// Dispatch looks up and executes the handler for the given command.
// Returns a RESP error if the command is unknown.
func (r *Router) Dispatch(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrorValue("ERR empty command")
	}

	cmd := strings.ToUpper(args[0].Str)
	handler, ok := r.handlers[cmd]
	if !ok {
		return resp.ErrorValue(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}

	result := handler(args[1:])

	// Log write commands to AOF
	if writeCommands[cmd] && result.Typ != resp.Error {
		r.server.LogToAOF(args)
	}

	return result
}

// registerAll registers all supported command handlers.
func (r *Router) registerAll() {
	st := r.server.store

	// --- Connection ---
	r.handlers["PING"] = r.cmdPing
	r.handlers["ECHO"] = r.cmdEcho
	r.handlers["COMMAND"] = r.cmdCommand

	// --- String ---
	r.handlers["SET"] = r.makeStringCmd(st, cmdSET)
	r.handlers["GET"] = r.makeStringCmd(st, cmdGET)
	r.handlers["MSET"] = r.makeStringCmd(st, cmdMSET)
	r.handlers["MGET"] = r.makeStringCmd(st, cmdMGET)
	r.handlers["INCR"] = r.makeStringCmd(st, cmdINCR)
	r.handlers["DECR"] = r.makeStringCmd(st, cmdDECR)
	r.handlers["INCRBY"] = r.makeStringCmd(st, cmdINCRBY)
	r.handlers["APPEND"] = r.makeStringCmd(st, cmdAPPEND)
	r.handlers["STRLEN"] = r.makeStringCmd(st, cmdSTRLEN)

	// --- Hash ---
	r.handlers["HSET"] = r.makeHashCmd(st, cmdHSET)
	r.handlers["HGET"] = r.makeHashCmd(st, cmdHGET)
	r.handlers["HGETALL"] = r.makeHashCmd(st, cmdHGETALL)
	r.handlers["HDEL"] = r.makeHashCmd(st, cmdHDEL)
	r.handlers["HLEN"] = r.makeHashCmd(st, cmdHLEN)
	r.handlers["HKEYS"] = r.makeHashCmd(st, cmdHKEYS)
	r.handlers["HEXISTS"] = r.makeHashCmd(st, cmdHEXISTS)
	r.handlers["HMSET"] = r.makeHashCmd(st, cmdHMSET)
	r.handlers["HMGET"] = r.makeHashCmd(st, cmdHMGET)

	// --- List ---
	r.handlers["LPUSH"] = r.makeListCmd(st, cmdLPUSH)
	r.handlers["RPUSH"] = r.makeListCmd(st, cmdRPUSH)
	r.handlers["LPOP"] = r.makeListCmd(st, cmdLPOP)
	r.handlers["RPOP"] = r.makeListCmd(st, cmdRPOP)
	r.handlers["LLEN"] = r.makeListCmd(st, cmdLLEN)
	r.handlers["LRANGE"] = r.makeListCmd(st, cmdLRANGE)
	r.handlers["LINDEX"] = r.makeListCmd(st, cmdLINDEX)
	r.handlers["LSET"] = r.makeListCmd(st, cmdLSET)

	// --- Set ---
	r.handlers["SADD"] = r.makeSetCmd(st, cmdSADD)
	r.handlers["SREM"] = r.makeSetCmd(st, cmdSREM)
	r.handlers["SMEMBERS"] = r.makeSetCmd(st, cmdSMEMBERS)
	r.handlers["SISMEMBER"] = r.makeSetCmd(st, cmdSISMEMBER)
	r.handlers["SCARD"] = r.makeSetCmd(st, cmdSCARD)
	r.handlers["SINTER"] = r.makeSetCmd(st, cmdSINTER)
	r.handlers["SUNION"] = r.makeSetCmd(st, cmdSUNION)
	r.handlers["SDIFF"] = r.makeSetCmd(st, cmdSDIFF)

	// --- Key/TTL ---
	r.handlers["DEL"] = r.cmdDel
	r.handlers["EXISTS"] = r.cmdExists
	r.handlers["EXPIRE"] = r.cmdExpire
	r.handlers["EXPIREAT"] = r.cmdExpireAt
	r.handlers["TTL"] = r.cmdTTL
	r.handlers["PERSIST"] = r.cmdPersist
	r.handlers["TYPE"] = r.cmdType
	r.handlers["RENAME"] = r.cmdRename
	r.handlers["KEYS"] = r.cmdKeys
	r.handlers["RANDOMKEY"] = r.cmdRandomKey
	r.handlers["DBSIZE"] = r.cmdDBSize
	r.handlers["FLUSHDB"] = r.cmdFlushDB
	r.handlers["INFO"] = r.cmdInfo
}

// ───────────────────────── Connection Commands ─────────────────────────

// cmdPing handles the PING command.
func (r *Router) cmdPing(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.SimpleStringValue("PONG")
	}
	return resp.BulkStringValue(args[0].Str)
}

// cmdEcho handles the ECHO command.
func (r *Router) cmdEcho(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}
	return resp.BulkStringValue(args[0].Str)
}

// cmdCommand handles the COMMAND command (stub that returns OK for redis-cli compatibility).
func (r *Router) cmdCommand(args []resp.Value) resp.Value {
	return resp.SimpleStringValue("OK")
}

// ───────────────────────── Helpers for type-checked commands ─────────────────────────

type stringCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type hashCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type listCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type setCmdFunc func(st *store.Store, args []resp.Value) resp.Value

// makeStringCmd wraps a string command with type checking.
func (r *Router) makeStringCmd(st *store.Store, fn stringCmdFunc) HandlerFunc {
	return func(args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

// makeHashCmd wraps a hash command.
func (r *Router) makeHashCmd(st *store.Store, fn hashCmdFunc) HandlerFunc {
	return func(args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

// makeListCmd wraps a list command.
func (r *Router) makeListCmd(st *store.Store, fn listCmdFunc) HandlerFunc {
	return func(args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

// makeSetCmd wraps a set command.
func (r *Router) makeSetCmd(st *store.Store, fn setCmdFunc) HandlerFunc {
	return func(args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

// ───────────────────────── String Commands ─────────────────────────

// cmdSET handles SET key value [EX seconds] [NX|XX]
func cmdSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'set' command")
	}
	key := args[0].Str
	value := args[1].Str

	// Check WRONGTYPE
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var ex int64
	nx := false
	xx := false

	// Parse optional flags
	for i := 2; i < len(args); i++ {
		flag := strings.ToUpper(args[i].Str)
		switch flag {
		case "EX":
			if i+1 >= len(args) {
				return resp.ErrorValue("ERR syntax error")
			}
			i++
			seconds, err := strconv.ParseInt(args[i].Str, 10, 64)
			if err != nil || seconds <= 0 {
				return resp.ErrorValue("ERR invalid expire time in 'set' command")
			}
			ex = seconds
		case "PX":
			if i+1 >= len(args) {
				return resp.ErrorValue("ERR syntax error")
			}
			i++
			ms, err := strconv.ParseInt(args[i].Str, 10, 64)
			if err != nil || ms <= 0 {
				return resp.ErrorValue("ERR invalid expire time in 'set' command")
			}
			ex = ms / 1000
			if ex == 0 {
				ex = 1
			}
		case "NX":
			nx = true
		case "XX":
			xx = true
		default:
			return resp.ErrorValue("ERR syntax error")
		}
	}

	if nx {
		if !st.Strings.SetNX(key, value) {
			return resp.NullValue()
		}
	} else if xx {
		if !st.Strings.SetXX(key, value) {
			return resp.NullValue()
		}
	} else {
		st.Strings.Set(key, value)
	}

	if ex > 0 {
		st.TTL.SetExpire(key, ex)
	}

	return resp.SimpleStringValue("OK")
}

// cmdGET handles GET key
func cmdGET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'get' command")
	}
	key := args[0].Str

	// Check WRONGTYPE
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	val, ok := st.Strings.Get(key)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(val)
}

// cmdMSET handles MSET key value [key value ...]
func cmdMSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'mset' command")
	}
	pairs := make(map[string]string, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Str] = args[i+1].Str
	}
	st.Strings.MSet(pairs)
	return resp.SimpleStringValue("OK")
}

// cmdMGET handles MGET key [key ...]
func cmdMGET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'mget' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	values := st.Strings.MGet(keys)
	result := make([]resp.Value, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = resp.NullValue()
		} else {
			result[i] = resp.BulkStringValue(v.(string))
		}
	}
	return resp.ArrayValue(result)
}

// cmdINCR handles INCR key
func cmdINCR(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'incr' command")
	}
	if t := st.KeyType(args[0].Str); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := st.Strings.IncrBy(args[0].Str, 1)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// cmdDECR handles DECR key
func cmdDECR(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'decr' command")
	}
	if t := st.KeyType(args[0].Str); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := st.Strings.IncrBy(args[0].Str, -1)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// cmdINCRBY handles INCRBY key increment
func cmdINCRBY(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'incrby' command")
	}
	if t := st.KeyType(args[0].Str); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	n, err := st.Strings.IncrBy(args[0].Str, delta)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

// cmdAPPEND handles APPEND key value
func cmdAPPEND(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'append' command")
	}
	if t := st.KeyType(args[0].Str); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	l := st.Strings.Append(args[0].Str, args[1].Str)
	return resp.IntegerValue(int64(l))
}

// cmdSTRLEN handles STRLEN key
func cmdSTRLEN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'strlen' command")
	}
	if t := st.KeyType(args[0].Str); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.Strings.StrLen(args[0].Str)))
}

// ───────────────────────── Hash Commands ─────────────────────────

// cmdHSET handles HSET key field value [field value ...]
func cmdHSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args[1:])%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hset' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	pairs := make(map[string]string, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		pairs[args[i].Str] = args[i+1].Str
	}
	n := st.Hashes.HSet(key, pairs)
	return resp.IntegerValue(int64(n))
}

// cmdHGET handles HGET key field
func cmdHGET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hget' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val, ok := st.Hashes.HGet(key, args[1].Str)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(val)
}

// cmdHGETALL handles HGETALL key
func cmdHGETALL(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hgetall' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	h := st.Hashes.HGetAll(key)
	if h == nil {
		return resp.ArrayValue([]resp.Value{})
	}
	result := make([]resp.Value, 0, len(h)*2)
	for k, v := range h {
		result = append(result, resp.BulkStringValue(k), resp.BulkStringValue(v))
	}
	return resp.ArrayValue(result)
}

// cmdHDEL handles HDEL key field [field ...]
func cmdHDEL(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hdel' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	fields := make([]string, len(args)-1)
	for i, a := range args[1:] {
		fields[i] = a.Str
	}
	n := st.Hashes.HDel(key, fields)
	return resp.IntegerValue(int64(n))
}

// cmdHLEN handles HLEN key
func cmdHLEN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hlen' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.Hashes.HLen(key)))
}

// cmdHKEYS handles HKEYS key
func cmdHKEYS(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hkeys' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	keys := st.Hashes.HKeys(key)
	result := make([]resp.Value, len(keys))
	for i, k := range keys {
		result[i] = resp.BulkStringValue(k)
	}
	return resp.ArrayValue(result)
}

// cmdHEXISTS handles HEXISTS key field
func cmdHEXISTS(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hexists' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if st.Hashes.HExists(key, args[1].Str) {
		return resp.IntegerValue(1)
	}
	return resp.IntegerValue(0)
}

// cmdHMSET handles HMSET key field value [field value ...] (deprecated alias for HSET)
func cmdHMSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args[1:])%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hmset' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	pairs := make(map[string]string, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		pairs[args[i].Str] = args[i+1].Str
	}
	st.Hashes.HSet(key, pairs)
	return resp.SimpleStringValue("OK")
}

// cmdHMGET handles HMGET key field [field ...]
func cmdHMGET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hmget' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	fields := make([]string, len(args)-1)
	for i, a := range args[1:] {
		fields[i] = a.Str
	}
	values := st.Hashes.HMGet(key, fields)
	result := make([]resp.Value, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = resp.NullValue()
		} else {
			result[i] = resp.BulkStringValue(v.(string))
		}
	}
	return resp.ArrayValue(result)
}

// ───────────────────────── List Commands ─────────────────────────

// cmdLPUSH handles LPUSH key element [element ...]
func cmdLPUSH(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lpush' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	values := make([]string, len(args)-1)
	for i, a := range args[1:] {
		values[i] = a.Str
	}
	n := st.Lists.LPush(key, values)
	return resp.IntegerValue(int64(n))
}

// cmdRPUSH handles RPUSH key element [element ...]
func cmdRPUSH(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rpush' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	values := make([]string, len(args)-1)
	for i, a := range args[1:] {
		values[i] = a.Str
	}
	n := st.Lists.RPush(key, values)
	return resp.IntegerValue(int64(n))
}

// cmdLPOP handles LPOP key
func cmdLPOP(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lpop' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val, ok := st.Lists.LPop(key)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(val)
}

// cmdRPOP handles RPOP key
func cmdRPOP(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rpop' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val, ok := st.Lists.RPop(key)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(val)
}

// cmdLLEN handles LLEN key
func cmdLLEN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'llen' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.Lists.LLen(key)))
}

// cmdLRANGE handles LRANGE key start stop
func cmdLRANGE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lrange' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	start, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	elems := st.Lists.LRange(key, start, stop)
	result := make([]resp.Value, len(elems))
	for i, e := range elems {
		result[i] = resp.BulkStringValue(e)
	}
	return resp.ArrayValue(result)
}

// cmdLINDEX handles LINDEX key index
func cmdLINDEX(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lindex' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	idx, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	val, err := st.Lists.LIndex(key, idx)
	if err != nil {
		return resp.NullValue()
	}
	return resp.BulkStringValue(val)
}

// cmdLSET handles LSET key index element
func cmdLSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lset' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	idx, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	if err := st.Lists.LSet(key, idx, args[2].Str); err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.SimpleStringValue("OK")
}

// ───────────────────────── Set Commands ─────────────────────────

// cmdSADD handles SADD key member [member ...]
func cmdSADD(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'sadd' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := make([]string, len(args)-1)
	for i, a := range args[1:] {
		members[i] = a.Str
	}
	n := st.Sets.SAdd(key, members)
	return resp.IntegerValue(int64(n))
}

// cmdSREM handles SREM key member [member ...]
func cmdSREM(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'srem' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := make([]string, len(args)-1)
	for i, a := range args[1:] {
		members[i] = a.Str
	}
	n := st.Sets.SRem(key, members)
	return resp.IntegerValue(int64(n))
}

// cmdSMEMBERS handles SMEMBERS key
func cmdSMEMBERS(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'smembers' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := st.Sets.SMembers(key)
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.BulkStringValue(m)
	}
	return resp.ArrayValue(result)
}

// cmdSISMEMBER handles SISMEMBER key member
func cmdSISMEMBER(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'sismember' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if st.Sets.SIsMember(key, args[1].Str) {
		return resp.IntegerValue(1)
	}
	return resp.IntegerValue(0)
}

// cmdSCARD handles SCARD key
func cmdSCARD(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'scard' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.Sets.SCard(key)))
}

// cmdSINTER handles SINTER key [key ...]
func cmdSINTER(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'sinter' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	members := st.Sets.SInter(keys)
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.BulkStringValue(m)
	}
	return resp.ArrayValue(result)
}

// cmdSUNION handles SUNION key [key ...]
func cmdSUNION(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'sunion' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	members := st.Sets.SUnion(keys)
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.BulkStringValue(m)
	}
	return resp.ArrayValue(result)
}

// cmdSDIFF handles SDIFF key [key ...]
func cmdSDIFF(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'sdiff' command")
	}
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = a.Str
	}
	members := st.Sets.SDiff(keys)
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.BulkStringValue(m)
	}
	return resp.ArrayValue(result)
}

// ───────────────────────── Key / Utility Commands ─────────────────────────

// cmdDel handles DEL key [key ...]
func (r *Router) cmdDel(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'del' command")
	}
	deleted := 0
	for _, a := range args {
		if r.server.store.DeleteKey(a.Str) {
			deleted++
		}
	}
	return resp.IntegerValue(int64(deleted))
}

// cmdExists handles EXISTS key [key ...]
func (r *Router) cmdExists(args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'exists' command")
	}
	count := 0
	for _, a := range args {
		if r.server.store.KeyExists(a.Str) {
			count++
		}
	}
	return resp.IntegerValue(int64(count))
}

// cmdExpire handles EXPIRE key seconds
func (r *Router) cmdExpire(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'expire' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(0)
	}
	seconds, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	r.server.store.TTL.SetExpire(key, seconds)
	return resp.IntegerValue(1)
}

// cmdExpireAt handles EXPIREAT key timestamp
func (r *Router) cmdExpireAt(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'expireat' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(0)
	}
	ts, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	r.server.store.TTL.SetExpireAt(key, ts)
	return resp.IntegerValue(1)
}

// cmdTTL handles TTL key
func (r *Router) cmdTTL(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'ttl' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(-2)
	}
	return resp.IntegerValue(r.server.store.TTL.GetTTL(key))
}

// cmdPersist handles PERSIST key
func (r *Router) cmdPersist(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'persist' command")
	}
	if r.server.store.TTL.Remove(args[0].Str) {
		return resp.IntegerValue(1)
	}
	return resp.IntegerValue(0)
}

// cmdType handles TYPE key
func (r *Router) cmdType(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'type' command")
	}
	return resp.SimpleStringValue(r.server.store.KeyType(args[0].Str))
}

// cmdRename handles RENAME key newkey
func (r *Router) cmdRename(args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rename' command")
	}
	if !r.server.store.RenameKey(args[0].Str, args[1].Str) {
		return resp.ErrorValue("ERR no such key")
	}
	return resp.SimpleStringValue("OK")
}

// cmdKeys handles KEYS pattern (currently supports only *)
func (r *Router) cmdKeys(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'keys' command")
	}
	pattern := args[0].Str
	allKeys := r.server.store.AllKeys()

	// Filter by glob pattern
	var matched []string
	for _, key := range allKeys {
		if matchGlob(pattern, key) {
			matched = append(matched, key)
		}
	}

	result := make([]resp.Value, len(matched))
	for i, k := range matched {
		result[i] = resp.BulkStringValue(k)
	}
	return resp.ArrayValue(result)
}

// cmdRandomKey handles RANDOMKEY
func (r *Router) cmdRandomKey(args []resp.Value) resp.Value {
	key := r.server.RandomKey()
	if key == "" {
		return resp.NullValue()
	}
	return resp.BulkStringValue(key)
}

// cmdDBSize handles DBSIZE
func (r *Router) cmdDBSize(args []resp.Value) resp.Value {
	return resp.IntegerValue(int64(r.server.store.DBSize()))
}

// cmdFlushDB handles FLUSHDB
func (r *Router) cmdFlushDB(args []resp.Value) resp.Value {
	r.server.store.FlushDB()
	return resp.SimpleStringValue("OK")
}

// cmdInfo handles INFO [section]
func (r *Router) cmdInfo(args []resp.Value) resp.Value {
	return resp.BulkStringValue(r.server.Info())
}

// matchGlob implements a simple glob pattern matcher supporting * and ?.
func matchGlob(pattern, str string) bool {
	return matchGlobHelper(pattern, str, 0, 0)
}

func matchGlobHelper(pattern, str string, pi, si int) bool {
	for pi < len(pattern) && si < len(str) {
		switch pattern[pi] {
		case '*':
			// Try matching * with 0 or more characters
			for si <= len(str) {
				if matchGlobHelper(pattern, str, pi+1, si) {
					return true
				}
				si++
			}
			return false
		case '?':
			pi++
			si++
		case '[':
			// Find closing bracket
			end := strings.IndexByte(pattern[pi:], ']')
			if end == -1 {
				return false
			}
			chars := pattern[pi+1 : pi+end]
			negate := false
			if len(chars) > 0 && chars[0] == '^' {
				negate = true
				chars = chars[1:]
			}
			found := strings.ContainsRune(chars, rune(str[si]))
			if negate {
				found = !found
			}
			if !found {
				return false
			}
			pi += end + 1
			si++
		default:
			if pattern[pi] != str[si] {
				return false
			}
			pi++
			si++
		}
	}

	// Consume trailing *
	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern) && si == len(str)
}

// Suppress unused import warning
var _ = filepath.Base
