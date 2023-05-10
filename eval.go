package CouloyDB

import (
	"fmt"
	"github.com/Kirov7/CouloyDB/public"
	lua "github.com/yuin/gopher-lua"
	"strings"
)

func (db *DB) Eval(script luaScript) (*Cmd, error) {
	if db.L == nil {
		return nil, public.ErrLuaInterpreterDisabled
	}
	if err := db.L.DoString(string(script)); err != nil {
		return nil, err
	}

	resultValue := db.L.Get(-1)
	db.L.Pop(1)

	switch v := resultValue.(type) {
	case lua.LNumber:
		if float64(v) == float64(int64(v)) {
			return &Cmd{Value: int64(v)}, nil
		}
		return &Cmd{Value: fmt.Sprint(float64(v))}, nil
	case lua.LString:
		return &Cmd{Value: string(v)}, nil
	case lua.LBool:
		return &Cmd{Value: bool(v)}, nil
	case *lua.LTable:
		var arr []interface{}
		v.ForEach(func(key, value lua.LValue) {
			arr = append(arr, value)
		})
		return &Cmd{Value: arr}, nil
	default:
		return &Cmd{Value: nil}, nil
	}
}

func (db *DB) initLuaInterpreter() {
	db.L = lua.NewState()
	db.L.SetGlobal("get", db.L.NewFunction(func(L *lua.LState) int {
		key := L.CheckString(1)
		value, _ := db.Get([]byte(key))
		L.Push(lua.LString(value))
		return 1
	}))

	db.L.SetGlobal("put", db.L.NewFunction(func(L *lua.LState) int {
		key := L.CheckString(1)
		value := L.CheckString(2)
		_ = db.Put([]byte(key), []byte(value))
		return 0
	}))

	db.L.SetGlobal("delete", db.L.NewFunction(func(L *lua.LState) int {
		key := L.CheckString(1)
		_ = db.Del([]byte(key))
		return 0
	}))
}

func BuildScript(raw ...string) luaScript {
	return luaScript(strings.Join(raw, ""))
}

type Cmd struct {
	Value interface{}
}

func (r *Cmd) AsInt() (int, error) {
	switch v := r.Value.(type) {
	case int:
		return v, nil
	case int8:
		return int(v), nil
	case int16:
		return int(v), nil
	case int32:
		return int(v), nil
	case int64:
		return int(v), nil
	case uint:
		return int(v), nil
	case uint8:
		return int(v), nil
	case uint16:
		return int(v), nil
	case uint32:
		return int(v), nil
	case uint64:
		return int(v), nil
	default:
		return 0, fmt.Errorf("result is not an int")
	}
}

func (r *Cmd) AsString() (string, error) {
	v, ok := r.Value.(string)
	if !ok {
		return "", fmt.Errorf("result is not a string")
	}
	return v, nil
}

func (r *Cmd) AsBool() (bool, error) {
	v, ok := r.Value.(bool)
	if !ok {
		return false, fmt.Errorf("result is not a bool")
	}
	return v, nil
}

func (r *Cmd) AsArray() ([]interface{}, error) {
	v, ok := r.Value.([]interface{})
	if !ok {
		return nil, fmt.Errorf("result is not an array")
	}
	return v, nil
}

type luaScript string
type LuaScriptBuilder struct {
	scriptParts []string
	raw         string
}

func NewLuaScriptBuilder() *LuaScriptBuilder {
	return &LuaScriptBuilder{}
}

func (b *LuaScriptBuilder) Raw(raw ...string) *LuaScriptBuilder {
	b.raw = strings.Join(raw, "")
	return b
}
func (b *LuaScriptBuilder) RawCode(code string) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, code)
	return b
}
func (b *LuaScriptBuilder) If(condition string, fn func(builder *LuaScriptBuilder)) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("if %s then", condition))
	fn(b)
	b.scriptParts = append(b.scriptParts, "end")
	return b
}
func (b *LuaScriptBuilder) ElseIf(condition string, fn func(builder *LuaScriptBuilder)) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("elseif %s then", condition))
	fn(b)
	return b
}
func (b *LuaScriptBuilder) Else(fn func(builder *LuaScriptBuilder)) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, "else")
	fn(b)
	b.scriptParts = append(b.scriptParts, "end")
	return b
}
func (b *LuaScriptBuilder) For(init, condition, step string, fn func(builder *LuaScriptBuilder)) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("for %s, %s, %s do", init, condition, step))
	fn(b)
	b.scriptParts = append(b.scriptParts, "end")
	return b
}
func (b *LuaScriptBuilder) DeclareArray(name string, values []string) *LuaScriptBuilder {
	quotedValues := make([]string, len(values))
	for i, value := range values {
		quotedValues[i] = fmt.Sprintf("%s", value)
	}
	arrayValues := strings.Join(quotedValues, ", ")
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("local %s = {%s}", name, arrayValues))
	return b
}

func (b *LuaScriptBuilder) GetArrayLength(name string) string {
	return fmt.Sprintf("#%s", name)
}

func (b *LuaScriptBuilder) GetValueFromArray(name, index string) string {
	return fmt.Sprintf("%s[%s]", name, index)
}

func (b *LuaScriptBuilder) SetValueInArray(name, index, value string) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("%s[%s] = '%s'", name, index, value))
	return b
}

func (b *LuaScriptBuilder) Put(key, value string) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("put('%s', '%s')", key, value))
	return b
}

func (b *LuaScriptBuilder) Set(key, value string) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("set('%s', '%s')", key, value))
	return b
}
func (b *LuaScriptBuilder) Del(key string) *LuaScriptBuilder {
	b.scriptParts = append(b.scriptParts, fmt.Sprintf("del('%s')", key))
	return b
}

func (b *LuaScriptBuilder) Build() luaScript {
	if b.raw != "" {
		return luaScript(b.raw)
	}
	return luaScript(strings.Join(b.scriptParts, "\n"))
}

//func test() {
//	builder := NewLuaScriptBuilder()
//	builder.
//		If("x > 0", func(b *LuaScriptBuilder) {
//			b.RawCode("print('x is positive')")
//		}).
//		ElseIf("x < 0", func(b *LuaScriptBuilder) {
//			b.RawCode("print('x is negative')")
//		}).
//		Else(func(b *LuaScriptBuilder) {
//			b.RawCode("print('x is zero')")
//		}).
//		For("i = 1", "i <= 10", "1", func(b *LuaScriptBuilder) {
//			b.RawCode("print(i)")
//		})
//	fmt.Println(builder.Build())
//}
