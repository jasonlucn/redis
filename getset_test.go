package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/pkg/errors"
)

type testStruct struct {
	Foo string `json:"foo"`
	Bar int    `json:"bar"`
	Baz bool   `json:"baz"`
}

func (suite *redisTestSuite) TestGetSet() {
	t := suite.T()
	ast := assert.New(t)
	client, err := newClient(suite.bizID)
	if err != nil {
		t.Fatalf("New client err:%v", err)
	}


	var (
		ctx = context.TODO()
		val interface{}
		key                      = "foo"
		defaultValue interface{} = "bar"
	)

	client.Del(ctx, key)
	if val, err = client.GetSetWithScan(ctx, key, defaultValue); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(defaultValue, val)
	}

	// get exists value
	if val, err = client.GetSetWithScan(ctx, key, "bar2"); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(defaultValue, val)
	}

	//int
	key = "foo_int"
	intVal := 100
	client.Set(ctx, key, intVal, time.Duration(0))

	//get int
	if val, err = client.GetSetWithScan(ctx, key, int(0)); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(int(intVal), val)
	}

	//get int64
	if val, err = client.GetSetWithScan(ctx, key, int64(0)); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(int64(intVal), val)
	}

	//get uint64
	if val, err = client.GetSetWithScan(ctx, key, uint64(0)); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(uint64(intVal), val)
	}

	//bool
	key = "foo_bool"
	client.Set(ctx, key, true, time.Duration(0))
	if val, err = client.GetSetWithScan(ctx, key, false); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(true, val)
	}

	//[]byte
	key = "foo_byte"
	byteVal := []byte("hello")
	client.Set(ctx, key, byteVal, time.Duration(0))
	if val, err = client.GetSetWithScan(ctx, key, []byte{}); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(byteVal, val)
	}
	//[]byte => string
	if val, err = client.GetSetWithScan(ctx, key, ""); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(string(byteVal), val)
	}

	//[]string
	key = "foo_strings"
	// 复杂结构不能直接调用Set方法写入
	strsVal := []string{"hello", "world"}
	client.Del(ctx, key)
	// 第一次写入默认值
	if val, err = client.GetSetWithScan(ctx, key, strsVal); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(strsVal, val)
	}

	// 读取之前设置的值
	if val, err = client.GetSetWithScan(ctx, key, []string{}); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(strsVal, val)
	}

	//-------------------------------
	//map
	key = "foo_map"
	client.Del(ctx, key)
	m := map[string]string{
		"foo": "foo_value",
		"bar": "bar_value",
		"baz": "baz_value",
	}
	if val, err = client.GetSetWithScan(ctx, key, m); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(m, val)
	}

	if val, err = client.GetSetWithScan(ctx, key, map[string]string{}); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(m, val)
	}

	//------------------------------------------
	//struct
	key = "foo_struct"
	client.Del(ctx, key)
	s := &testStruct{
		Foo: "foo_value",
		Bar: 1,
		Baz: true,
	}

	if val, err = client.GetSetWithScan(ctx, key, s); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(s, val)
	}

	// get exists value
	if val, err = client.GetSetWithScan(ctx, key, &testStruct{}); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(s, val)
	}

	// struct always return pointer
	if val, err = client.GetSetWithScan(ctx, key, testStruct{}); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(s, val)
	}

	//[]struct
	key = "foo_structs"
	client.Del(ctx, key)
	s2 := &testStruct{
		Foo: "foo_value2",
		Bar: 10,
		Baz: false,
	}
	ssVal := []*testStruct{s, s2}
	if val, err = client.GetSetWithScan(ctx, key, ssVal); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(ssVal, val)
	}

	if val, err = client.GetSetWithScan(ctx, key, []*testStruct{}); err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(ssVal, val)
	}

	//fetcher
	client.Del(ctx, key)
	var testErr = fmt.Errorf("test error")
	val, err = client.GetSetWithScan(ctx, key, s, WithFetcher(func() (interface{}, error) {
		return nil, testErr
	}))
	ast.Equal(testErr, errors.Cause(err))

	val, err = client.GetSetWithScan(ctx, key, s, WithFetcher(func() (interface{}, error) {
		return s, nil
	}))
	if err != nil {
		t.Fatalf("GetSetWithScan err:%v", err)
	} else {
		ast.Equal(s, val)
	}
}