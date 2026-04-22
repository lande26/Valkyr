package server

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kartik/valkyr/resp"
	"github.com/kartik/valkyr/store"
)

// HandlerFunc is the signature for command handler functions.
type HandlerFunc func(p *Peer, args []resp.Value) resp.Value

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

	// New write commands
	"GETSET": true, "SETEX": true, "PSETEX": true, "SETRANGE": true, "DECRBY": true, "MSETNX": true,
	"HINCRBY": true, "HINCRBYFLOAT": true,
	"LREM": true, "LTRIM": true, "LINSERT": true, "RPOPLPUSH": true,
	"SPOP": true, "SMOVE": true,
	"ZADD": true, "ZREM": true,
	"PEXPIRE": true, "PEXPIREAT": true, "UNLINK": true, "TOUCH": true,
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

// HasHandler returns true if a command has a registered handler.
func (r *Router) HasHandler(cmd string) bool {
	_, ok := r.handlers[strings.ToUpper(cmd)]
	return ok
}

// Dispatch looks up and executes the handler for the given command.
// Returns a RESP error if the command is unknown.
func (r *Router) Dispatch(p *Peer, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrorValue("ERR empty command")
	}

	cmd := strings.ToUpper(args[0].Str)
	handler, ok := r.handlers[cmd]
	if !ok {
		return resp.ErrorValue(fmt.Sprintf("ERR unknown command '%s'", cmd))
	}

	// Memory eviction check before running write commands
	if writeCommands[cmd] {
		if errVal := r.server.CheckAndEvictMemory(); errVal.Typ == resp.Error {
			return errVal
		}
	}

	result := handler(p, args[1:])

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
	// New String
	r.handlers["GETSET"] = r.makeStringCmd(st, cmdGETSET)
	r.handlers["SETEX"] = r.makeStringCmd(st, cmdSETEX)
	r.handlers["PSETEX"] = r.makeStringCmd(st, cmdPSETEX)
	r.handlers["SETRANGE"] = r.makeStringCmd(st, cmdSETRANGE)
	r.handlers["GETRANGE"] = r.makeStringCmd(st, cmdGETRANGE)
	r.handlers["DECRBY"] = r.makeStringCmd(st, cmdDECRBY)
	r.handlers["MSETNX"] = r.makeStringCmd(st, cmdMSETNX)

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
	// New Hash
	r.handlers["HINCRBY"] = r.makeHashCmd(st, cmdHINCRBY)
	r.handlers["HINCRBYFLOAT"] = r.makeHashCmd(st, cmdHINCRBYFLOAT)
	r.handlers["HVALS"] = r.makeHashCmd(st, cmdHVALS)
	r.handlers["HSCAN"] = r.makeHashCmd(st, cmdHSCAN)
	r.handlers["HSTRLEN"] = r.makeHashCmd(st, cmdHSTRLEN)

	// --- List ---
	r.handlers["LPUSH"] = r.makeListCmd(st, cmdLPUSH)
	r.handlers["RPUSH"] = r.makeListCmd(st, cmdRPUSH)
	r.handlers["LPOP"] = r.makeListCmd(st, cmdLPOP)
	r.handlers["RPOP"] = r.makeListCmd(st, cmdRPOP)
	r.handlers["LLEN"] = r.makeListCmd(st, cmdLLEN)
	r.handlers["LRANGE"] = r.makeListCmd(st, cmdLRANGE)
	r.handlers["LINDEX"] = r.makeListCmd(st, cmdLINDEX)
	r.handlers["LSET"] = r.makeListCmd(st, cmdLSET)
	// New List
	r.handlers["LREM"] = r.makeListCmd(st, cmdLREM)
	r.handlers["LTRIM"] = r.makeListCmd(st, cmdLTRIM)
	r.handlers["LINSERT"] = r.makeListCmd(st, cmdLINSERT)
	r.handlers["RPOPLPUSH"] = r.makeListCmd(st, cmdRPOPLPUSH)

	// --- Set ---
	r.handlers["SADD"] = r.makeSetCmd(st, cmdSADD)
	r.handlers["SREM"] = r.makeSetCmd(st, cmdSREM)
	r.handlers["SMEMBERS"] = r.makeSetCmd(st, cmdSMEMBERS)
	r.handlers["SISMEMBER"] = r.makeSetCmd(st, cmdSISMEMBER)
	r.handlers["SCARD"] = r.makeSetCmd(st, cmdSCARD)
	r.handlers["SINTER"] = r.makeSetCmd(st, cmdSINTER)
	r.handlers["SUNION"] = r.makeSetCmd(st, cmdSUNION)
	r.handlers["SDIFF"] = r.makeSetCmd(st, cmdSDIFF)
	// New Set
	r.handlers["SPOP"] = r.makeSetCmd(st, cmdSPOP)
	r.handlers["SRANDMEMBER"] = r.makeSetCmd(st, cmdSRANDMEMBER)
	r.handlers["SMOVE"] = r.makeSetCmd(st, cmdSMOVE)
	r.handlers["SSCAN"] = r.makeSetCmd(st, cmdSSCAN)

	// --- ZSet (New) ---
	r.handlers["ZADD"] = r.makeZSetCmd(st, cmdZADD)
	r.handlers["ZRANGE"] = r.makeZSetCmd(st, cmdZRANGE)
	r.handlers["ZREM"] = r.makeZSetCmd(st, cmdZREM)
	r.handlers["ZCARD"] = r.makeZSetCmd(st, cmdZCARD)
	r.handlers["ZSCORE"] = r.makeZSetCmd(st, cmdZSCORE)
	r.handlers["ZRANK"] = r.makeZSetCmd(st, cmdZRANK)
	r.handlers["ZREVRANGE"] = r.makeZSetCmd(st, cmdZREVRANGE)
	r.handlers["ZREVRANK"] = r.makeZSetCmd(st, cmdZREVRANK)
	r.handlers["ZCOUNT"] = r.makeZSetCmd(st, cmdZCOUNT)

	// --- Pub/Sub (New) ---
	r.handlers["SUBSCRIBE"] = r.cmdSubscribe
	r.handlers["UNSUBSCRIBE"] = r.cmdUnsubscribe
	r.handlers["PSUBSCRIBE"] = r.cmdPSubscribe
	r.handlers["PUNSUBSCRIBE"] = r.cmdPUnsubscribe
	r.handlers["PUBLISH"] = r.cmdPublish

	// --- Transactions (New) ---
	r.handlers["MULTI"] = r.cmdMulti
	r.handlers["EXEC"] = r.cmdExec
	r.handlers["DISCARD"] = r.cmdDiscard

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
	r.handlers["BGSAVE"] = r.cmdBGSave
	r.handlers["BGREWRITEAOF"] = r.cmdBGRewriteAOF

	// New Key/TTL
	r.handlers["SCAN"] = r.cmdScan
	r.handlers["PTTL"] = r.cmdPTTL
	r.handlers["PEXPIRE"] = r.cmdPExpire
	r.handlers["PEXPIREAT"] = r.cmdPExpireAt
	r.handlers["UNLINK"] = r.cmdUnlink
	r.handlers["TOUCH"] = r.cmdTouch
}

// ───────────────────────── Connection Commands ─────────────────────────

func (r *Router) cmdPing(p *Peer, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.SimpleStringValue("PONG")
	}
	return resp.BulkStringValue(args[0].Str)
}

func (r *Router) cmdEcho(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'echo' command")
	}
	return resp.BulkStringValue(args[0].Str)
}

func (r *Router) cmdCommand(p *Peer, args []resp.Value) resp.Value {
	return resp.SimpleStringValue("OK")
}

// ───────────────────────── Helpers for type-checked commands ─────────────────────────

type stringCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type hashCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type listCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type setCmdFunc func(st *store.Store, args []resp.Value) resp.Value
type zsetCmdFunc func(st *store.Store, args []resp.Value) resp.Value

func (r *Router) makeStringCmd(st *store.Store, fn stringCmdFunc) HandlerFunc {
	return func(p *Peer, args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

func (r *Router) makeHashCmd(st *store.Store, fn hashCmdFunc) HandlerFunc {
	return func(p *Peer, args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

func (r *Router) makeListCmd(st *store.Store, fn listCmdFunc) HandlerFunc {
	return func(p *Peer, args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

func (r *Router) makeSetCmd(st *store.Store, fn setCmdFunc) HandlerFunc {
	return func(p *Peer, args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

func (r *Router) makeZSetCmd(st *store.Store, fn zsetCmdFunc) HandlerFunc {
	return func(p *Peer, args []resp.Value) resp.Value {
		return fn(st, args)
	}
}

// ───────────────────────── String Commands ─────────────────────────

func cmdSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'set' command")
	}
	key := args[0].Str
	value := args[1].Str

	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	var ex int64
	nx := false
	xx := false

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

func cmdGET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'get' command")
	}
	key := args[0].Str

	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	val, ok := st.Strings.Get(key)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(val)
}

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

func cmdSTRLEN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'strlen' command")
	}
	if t := st.KeyType(args[0].Str); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.Strings.StrLen(args[0].Str)))
}

func cmdGETSET(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'getset' command")
	}
	key := args[0].Str
	value := args[1].Str
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	old, ok := st.Strings.Get(key)
	st.Strings.Set(key, value)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(old)
}

func cmdSETEX(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'setex' command")
	}
	key := args[0].Str
	seconds, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil || seconds <= 0 {
		return resp.ErrorValue("ERR invalid expire time in 'setex' command")
	}
	value := args[2].Str
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	st.Strings.Set(key, value)
	st.TTL.SetExpire(key, seconds)
	return resp.SimpleStringValue("OK")
}

func cmdPSETEX(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'psetex' command")
	}
	key := args[0].Str
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil || ms <= 0 {
		return resp.ErrorValue("ERR invalid expire time in 'psetex' command")
	}
	value := args[2].Str
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	st.Strings.Set(key, value)
	st.TTL.SetDeadline(key, time.Now().UnixMilli()+ms)
	return resp.SimpleStringValue("OK")
}

func cmdSETRANGE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'setrange' command")
	}
	key := args[0].Str
	offset, err := strconv.Atoi(args[1].Str)
	if err != nil || offset < 0 {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	value := args[2].Str
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	old, ok := st.Strings.Get(key)
	if !ok {
		old = ""
	}
	var newStr string
	if offset > len(old) {
		newStr = old + strings.Repeat("\x00", offset-len(old)) + value
	} else {
		newStr = old[:offset] + value
		if offset+len(value) < len(old) {
			newStr += old[offset+len(value):]
		}
	}
	st.Strings.Set(key, newStr)
	return resp.IntegerValue(int64(len(newStr)))
}

func cmdGETRANGE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'getrange' command")
	}
	key := args[0].Str
	start, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	end, err := strconv.Atoi(args[2].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	val, ok := st.Strings.Get(key)
	if !ok {
		return resp.BulkStringValue("")
	}
	length := len(val)
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end < 0 {
		end = 0
	}
	if start >= length {
		return resp.BulkStringValue("")
	}
	if end >= length {
		end = length - 1
	}
	if start > end {
		return resp.BulkStringValue("")
	}
	return resp.BulkStringValue(val[start : end+1])
}

func cmdDECRBY(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'decrby' command")
	}
	key := args[0].Str
	delta, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	if t := st.KeyType(key); t != "none" && t != "string" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := st.Strings.IncrBy(key, -delta)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

func cmdMSETNX(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'msetnx' command")
	}
	for i := 0; i < len(args); i += 2 {
		if st.KeyExists(args[i].Str) {
			return resp.IntegerValue(0)
		}
	}
	pairs := make(map[string]string, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Str] = args[i+1].Str
	}
	st.Strings.MSet(pairs)
	return resp.IntegerValue(1)
}

// ───────────────────────── Hash Commands ─────────────────────────

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

func cmdHMSET(st *store.Store, args []resp.Value) resp.Value {
	return cmdHSET(st, args)
}

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

func cmdHINCRBY(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hincrby' command")
	}
	key := args[0].Str
	field := args[1].Str
	inc, err := strconv.ParseInt(args[2].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := st.Hashes.HIncrBy(key, field, inc)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(n)
}

func cmdHINCRBYFLOAT(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hincrbyfloat' command")
	}
	key := args[0].Str
	field := args[1].Str
	inc, err := strconv.ParseFloat(args[2].Str, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not a valid float")
	}
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := st.Hashes.HIncrByFloat(key, field, inc)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.BulkStringValue(fmt.Sprintf("%g", n))
}

func cmdHVALS(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hvals' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	vals := st.Hashes.HVals(key)
	result := make([]resp.Value, len(vals))
	for i, v := range vals {
		result[i] = resp.BulkStringValue(v)
	}
	return resp.ArrayValue(result)
}

func cmdHSCAN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hscan' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	h := st.Hashes.HGetAll(key)
	var matchPattern string
	for i := 2; i < len(args); i++ {
		flag := strings.ToUpper(args[i].Str)
		if flag == "MATCH" && i+1 < len(args) {
			matchPattern = args[i+1].Str
			i++
		} else if flag == "COUNT" && i+1 < len(args) {
			i++
		}
	}
	var matched []resp.Value
	for f, v := range h {
		if matchPattern == "" || matchGlob(matchPattern, f) {
			matched = append(matched, resp.BulkStringValue(f), resp.BulkStringValue(v))
		}
	}
	return resp.ArrayValue([]resp.Value{
		resp.BulkStringValue("0"),
		resp.ArrayValue(matched),
	})
}

func cmdHSTRLEN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'hstrlen' command")
	}
	key := args[0].Str
	field := args[1].Str
	if t := st.KeyType(key); t != "none" && t != "hash" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.Hashes.HStrLen(key, field)))
}

// ───────────────────────── List Commands ─────────────────────────

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

func cmdLREM(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'lrem' command")
	}
	key := args[0].Str
	count, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	value := args[2].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n := st.Lists.LRem(key, count, value)
	return resp.IntegerValue(int64(n))
}

func cmdLTRIM(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'ltrim' command")
	}
	key := args[0].Str
	start, err := strconv.Atoi(args[1].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	stop, err := strconv.Atoi(args[2].Str)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	st.Lists.LTrim(key, start, stop)
	return resp.SimpleStringValue("OK")
}

func cmdLINSERT(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 4 {
		return resp.ErrorValue("ERR wrong number of arguments for 'linsert' command")
	}
	key := args[0].Str
	pos := args[1].Str
	pivot := args[2].Str
	value := args[3].Str
	if t := st.KeyType(key); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	n, err := st.Lists.LInsert(key, pos, pivot, value)
	if err != nil {
		return resp.ErrorValue(err.Error())
	}
	return resp.IntegerValue(int64(n))
}

func cmdRPOPLPUSH(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rpoplpush' command")
	}
	src := args[0].Str
	dest := args[1].Str

	if t := st.KeyType(src); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if t := st.KeyType(dest); t != "none" && t != "list" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	val, ok := st.Lists.RPop(src)
	if !ok {
		return resp.NullValue()
	}

	st.Lists.LPush(dest, []string{val})
	return resp.BulkStringValue(val)
}

// ───────────────────────── Set Commands ─────────────────────────

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

func cmdSPOP(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'spop' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := 1
	if len(args) == 2 {
		var err error
		count, err = strconv.Atoi(args[1].Str)
		if err != nil || count <= 0 {
			return resp.ErrorValue("ERR value is not an integer or out of range")
		}
	}
	popped := st.Sets.SPop(key, count)
	if popped == nil {
		if len(args) == 1 {
			return resp.NullValue()
		}
		return resp.ArrayValue([]resp.Value{})
	}
	if len(args) == 1 {
		return resp.BulkStringValue(popped[0])
	}
	result := make([]resp.Value, len(popped))
	for i, m := range popped {
		result[i] = resp.BulkStringValue(m)
	}
	return resp.ArrayValue(result)
}

func cmdSRANDMEMBER(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 1 || len(args) > 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'srandmember' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := 1
	hasCount := false
	if len(args) == 2 {
		hasCount = true
		var err error
		count, err = strconv.Atoi(args[1].Str)
		if err != nil {
			return resp.ErrorValue("ERR value is not an integer or out of range")
		}
	}
	members := st.Sets.SRandMember(key, count)
	if members == nil {
		if hasCount {
			return resp.ArrayValue([]resp.Value{})
		}
		return resp.NullValue()
	}
	if !hasCount {
		return resp.BulkStringValue(members[0])
	}
	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.BulkStringValue(m)
	}
	return resp.ArrayValue(result)
}

func cmdSMOVE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'smove' command")
	}
	src := args[0].Str
	dest := args[1].Str
	member := args[2].Str

	if t := st.KeyType(src); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if t := st.KeyType(dest); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	if st.Sets.SMove(src, dest, member) {
		return resp.IntegerValue(1)
	}
	return resp.IntegerValue(0)
}

func cmdSSCAN(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'sscan' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "set" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := st.Sets.SMembers(key)
	var matchPattern string
	for i := 2; i < len(args); i++ {
		flag := strings.ToUpper(args[i].Str)
		if flag == "MATCH" && i+1 < len(args) {
			matchPattern = args[i+1].Str
			i++
		} else if flag == "COUNT" && i+1 < len(args) {
			i++
		}
	}
	var matched []resp.Value
	for _, m := range members {
		if matchPattern == "" || matchGlob(matchPattern, m) {
			matched = append(matched, resp.BulkStringValue(m))
		}
	}
	return resp.ArrayValue([]resp.Value{
		resp.BulkStringValue("0"),
		resp.ArrayValue(matched),
	})
}

// ───────────────────────── Sorted Set Commands ─────────────────────────

func cmdZADD(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args[1:])%2 != 0 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zadd' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	added := 0
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i].Str, 64)
		if err != nil {
			return resp.ErrorValue("ERR value is not a valid float")
		}
		member := args[i+1].Str
		if st.ZSets.ZAdd(key, score, member) {
			added++
		}
	}
	return resp.IntegerValue(int64(added))
}

func cmdZRANGE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrange' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
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
	withScores := false
	if len(args) == 4 && strings.ToUpper(args[3].Str) == "WITHSCORES" {
		withScores = true
	}
	elements := st.ZSets.ZRange(key, start, stop)
	var res []resp.Value
	for _, e := range elements {
		res = append(res, resp.BulkStringValue(e.Member))
		if withScores {
			res = append(res, resp.BulkStringValue(fmt.Sprintf("%g", e.Score)))
		}
	}
	return resp.ArrayValue(res)
}

func cmdZREM(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrem' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	mems := make([]string, len(args)-1)
	for i, a := range args[1:] {
		mems[i] = a.Str
	}
	removed := st.ZSets.ZRem(key, mems)
	return resp.IntegerValue(int64(removed))
}

func cmdZCARD(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zcard' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.IntegerValue(int64(st.ZSets.ZCard(key)))
}

func cmdZSCORE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zscore' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	member := args[1].Str
	score, ok := st.ZSets.ZScore(key, member)
	if !ok {
		return resp.NullValue()
	}
	return resp.BulkStringValue(fmt.Sprintf("%g", score))
}

func cmdZRANK(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrank' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	member := args[1].Str
	rank, ok := st.ZSets.ZRank(key, member)
	if !ok {
		return resp.NullValue()
	}
	return resp.IntegerValue(int64(rank))
}

func cmdZREVRANGE(st *store.Store, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrevrange' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
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
	withScores := false
	if len(args) == 4 && strings.ToUpper(args[3].Str) == "WITHSCORES" {
		withScores = true
	}
	elements := st.ZSets.ZRevRange(key, start, stop)
	var res []resp.Value
	for _, e := range elements {
		res = append(res, resp.BulkStringValue(e.Member))
		if withScores {
			res = append(res, resp.BulkStringValue(fmt.Sprintf("%g", e.Score)))
		}
	}
	return resp.ArrayValue(res)
}

func cmdZREVRANK(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zrevrank' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	member := args[1].Str
	rank, ok := st.ZSets.ZRevRank(key, member)
	if !ok {
		return resp.NullValue()
	}
	return resp.IntegerValue(int64(rank))
}

func cmdZCOUNT(st *store.Store, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrorValue("ERR wrong number of arguments for 'zcount' command")
	}
	key := args[0].Str
	if t := st.KeyType(key); t != "none" && t != "zset" {
		return resp.ErrorValue("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	min, err := strconv.ParseFloat(args[1].Str, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not a valid float")
	}
	max, err := strconv.ParseFloat(args[2].Str, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not a valid float")
	}
	return resp.IntegerValue(int64(st.ZSets.ZCount(key, min, max)))
}

// ───────────────────────── Pub/Sub Commands ─────────────────────────

func (r *Router) cmdSubscribe(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR SUBSCRIBE only active in connection context")
	}
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'subscribe' command")
	}
	channels := make([]string, len(args))
	for i, a := range args {
		channels[i] = a.Str
	}
	r.server.Subscribe(p, channels)
	return resp.Value{}
}

func (r *Router) cmdUnsubscribe(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR UNSUBSCRIBE only active in connection context")
	}
	channels := make([]string, len(args))
	for i, a := range args {
		channels[i] = a.Str
	}
	r.server.Unsubscribe(p, channels)
	return resp.Value{}
}

func (r *Router) cmdPSubscribe(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR PSUBSCRIBE only active in connection context")
	}
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'psubscribe' command")
	}
	patterns := make([]string, len(args))
	for i, a := range args {
		patterns[i] = a.Str
	}
	r.server.PSubscribe(p, patterns)
	return resp.Value{}
}

func (r *Router) cmdPUnsubscribe(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR PUNSUBSCRIBE only active in connection context")
	}
	patterns := make([]string, len(args))
	for i, a := range args {
		patterns[i] = a.Str
	}
	r.server.PUnsubscribe(p, patterns)
	return resp.Value{}
}

func (r *Router) cmdPublish(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'publish' command")
	}
	channel := args[0].Str
	message := args[1].Str
	n := r.server.Publish(channel, message)
	return resp.IntegerValue(int64(n))
}

// ───────────────────────── Transactions Commands ─────────────────────────

func (r *Router) cmdMulti(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR MULTI only active in connection context")
	}
	if p.inTx {
		return resp.ErrorValue("ERR MULTI calls can not be nested")
	}
	p.inTx = true
	p.txQueue = nil
	return resp.SimpleStringValue("OK")
}

func (r *Router) cmdDiscard(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR DISCARD only active in connection context")
	}
	if !p.inTx {
		return resp.ErrorValue("ERR DISCARD without MULTI")
	}
	p.inTx = false
	p.txQueue = nil
	return resp.SimpleStringValue("OK")
}

func (r *Router) cmdExec(p *Peer, args []resp.Value) resp.Value {
	if p == nil {
		return resp.ErrorValue("ERR EXEC only active in connection context")
	}
	if !p.inTx {
		return resp.ErrorValue("ERR EXEC without MULTI")
	}

	queue := p.txQueue
	p.inTx = false
	p.txQueue = nil

	if len(queue) == 0 {
		return resp.ArrayValue([]resp.Value{})
	}

	results := make([]resp.Value, len(queue))
	for i, cmdArgs := range queue {
		results[i] = r.Dispatch(p, cmdArgs)
	}

	return resp.ArrayValue(results)
}

// ───────────────────────── Key / Utility Commands ─────────────────────────

func (r *Router) cmdDel(p *Peer, args []resp.Value) resp.Value {
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

func (r *Router) cmdExists(p *Peer, args []resp.Value) resp.Value {
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

func (r *Router) cmdExpire(p *Peer, args []resp.Value) resp.Value {
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

func (r *Router) cmdExpireAt(p *Peer, args []resp.Value) resp.Value {
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

func (r *Router) cmdTTL(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'ttl' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(-2)
	}
	return resp.IntegerValue(r.server.store.TTL.GetTTL(key))
}

func (r *Router) cmdPersist(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'persist' command")
	}
	if r.server.store.TTL.Remove(args[0].Str) {
		return resp.IntegerValue(1)
	}
	return resp.IntegerValue(0)
}

func (r *Router) cmdType(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'type' command")
	}
	return resp.SimpleStringValue(r.server.store.KeyType(args[0].Str))
}

func (r *Router) cmdRename(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'rename' command")
	}
	if !r.server.store.RenameKey(args[0].Str, args[1].Str) {
		return resp.ErrorValue("ERR no such key")
	}
	return resp.SimpleStringValue("OK")
}

func (r *Router) cmdKeys(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'keys' command")
	}
	pattern := args[0].Str
	allKeys := r.server.store.AllKeys()

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

func (r *Router) cmdRandomKey(p *Peer, args []resp.Value) resp.Value {
	key := r.server.RandomKey()
	if key == "" {
		return resp.NullValue()
	}
	return resp.BulkStringValue(key)
}

func (r *Router) cmdDBSize(p *Peer, args []resp.Value) resp.Value {
	return resp.IntegerValue(int64(r.server.store.DBSize()))
}

func (r *Router) cmdFlushDB(p *Peer, args []resp.Value) resp.Value {
	r.server.store.FlushDB()
	return resp.SimpleStringValue("OK")
}

func (r *Router) cmdInfo(p *Peer, args []resp.Value) resp.Value {
	return resp.BulkStringValue(r.server.Info())
}

func (r *Router) cmdBGSave(p *Peer, args []resp.Value) resp.Value {
	if err := r.server.SyncAOF(); err != nil {
		return resp.ErrorValue("ERR " + err.Error())
	}
	return resp.SimpleStringValue("Background saving started")
}

func (r *Router) cmdBGRewriteAOF(p *Peer, args []resp.Value) resp.Value {
	if err := r.server.BGRewriteAOF(); err != nil {
		return resp.ErrorValue("ERR " + err.Error())
	}
	return resp.SimpleStringValue("Background append only file rewriting started")
}

func (r *Router) cmdScan(p *Peer, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'scan' command")
	}
	keys := r.server.store.AllKeys()
	var matchPattern string
	for i := 1; i < len(args); i++ {
		flag := strings.ToUpper(args[i].Str)
		if flag == "MATCH" && i+1 < len(args) {
			matchPattern = args[i+1].Str
			i++
		} else if flag == "COUNT" && i+1 < len(args) {
			i++
		}
	}
	var matched []resp.Value
	for _, k := range keys {
		if matchPattern == "" || matchGlob(matchPattern, k) {
			matched = append(matched, resp.BulkStringValue(k))
		}
	}
	return resp.ArrayValue([]resp.Value{
		resp.BulkStringValue("0"),
		resp.ArrayValue(matched),
	})
}

func (r *Router) cmdPTTL(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'pttl' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(-2)
	}
	deadline, ok := r.server.store.TTL.GetDeadline(key)
	if !ok {
		return resp.IntegerValue(-1)
	}
	remaining := deadline - time.Now().UnixMilli()
	if remaining < 0 {
		return resp.IntegerValue(0)
	}
	return resp.IntegerValue(remaining)
}

func (r *Router) cmdPExpire(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'pexpire' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(0)
	}
	ms, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	r.server.store.TTL.SetDeadline(key, time.Now().UnixMilli()+ms)
	return resp.IntegerValue(1)
}

func (r *Router) cmdPExpireAt(p *Peer, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrorValue("ERR wrong number of arguments for 'pexpireat' command")
	}
	key := args[0].Str
	if !r.server.store.KeyExists(key) {
		return resp.IntegerValue(0)
	}
	ts, err := strconv.ParseInt(args[1].Str, 10, 64)
	if err != nil {
		return resp.ErrorValue("ERR value is not an integer or out of range")
	}
	r.server.store.TTL.SetDeadline(key, ts)
	return resp.IntegerValue(1)
}

func (r *Router) cmdUnlink(p *Peer, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'unlink' command")
	}
	deleted := 0
	for _, a := range args {
		if r.server.store.DeleteKey(a.Str) {
			deleted++
		}
	}
	return resp.IntegerValue(int64(deleted))
}

func (r *Router) cmdTouch(p *Peer, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrorValue("ERR wrong number of arguments for 'touch' command")
	}
	touched := 0
	for _, a := range args {
		if r.server.store.KeyExists(a.Str) {
			r.server.store.Touch(a.Str)
			touched++
		}
	}
	return resp.IntegerValue(int64(touched))
}

// matchGlob implements a simple glob pattern matcher supporting * and ?.
func matchGlob(pattern, str string) bool {
	return matchGlobHelper(pattern, str, 0, 0)
}

func matchGlobHelper(pattern, str string, pi, si int) bool {
	for pi < len(pattern) && si < len(str) {
		switch pattern[pi] {
		case '*':
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

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern) && si == len(str)
}
