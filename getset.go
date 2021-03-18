package redis

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/pkg/errors"
	stdredis "github.com/go-redis/redis/v8"
)

type options struct {
	ctx        context.Context
	expiration time.Duration
	fetcher    func() (interface{}, error)
}

type Option interface {
	apply(*options)
}

//ctx
type ctxOption struct {
	v context.Context
}

func (ctx ctxOption) apply(opts *options) {
	opts.ctx = context.Context(ctx.v)
}

func WithContext(ctx context.Context) Option {
	return &ctxOption{
		v: ctx,
	}
}

//expiration
type expirationOption time.Duration

func (e expirationOption) apply(opts *options) {
	opts.expiration = time.Duration(e)
}

func WithExpiration(e time.Duration) Option {
	return expirationOption(e)
}

type Fetcher func() (interface{}, error)

//fetcher
type fetcherOption Fetcher

func (f fetcherOption) apply(opts *options) {
	opts.fetcher = Fetcher(f)
}

func WithFetcher(f Fetcher) Option {
	return fetcherOption(f)
}

// 获取key对应的值，并按给定的类型结构化数据
// 如果缓存不存在，则优先从指定的Fetcher中获取（会限流调用），没有Fetcher，会传入的value做为默认值，默认值会Set到redis中
// 指定了Fetcher选项，value值可以给对应内容的0值
//
// value参数除了做为默认值外，同样是传入数据类型信息，返回的interface{}会和value的类型一致
// 唯一例外的是，如果是struct类型，返回的interface{}是*struct类型，即struct始终返回指针类型
//
//	client := redis.Ins("test")
//
//	if v, err := client.GetSetWithScan("string_key", ""/*key不存在时指定的默认值*/); err != nil {
//		fmt.Println(v.(string))
//	}
//
//	if v, err := client.GetSetWithScan("map_key", map[string]string{}); err != nil {
//		m := v.(map[string]string)
//	}
//
//	v, err := client.GetSetWithScan("struct_key", &MyStruct{}, redis.WithFetcher(func() (interface{}, error) {
//		// db 查询之类
//		return &MyStruct{Val:"..."}, nil
//	}))
//
func (c *Client) GetSetWithScan(ctx context.Context, key string, value interface{}, opts ...Option) (interface{}, error) {
	options := options{
		expiration: 300 * time.Second, // 默认缓存时间，通过参数中的WithExpiration覆盖
	}
	for _, opt := range opts {
		opt.apply(&options)
	}

	//ctx := metadata.Ins(options.ctx)

	var err error

	//if !ctx.NoCache() {

	//span := itracing.BeginRecord(ctx, fmt.Sprintf("redis.Get.%v", c.bizID))
	cmd := c.Get(ctx, key)
	//itracing.EndRecord(span)

	if err = cmd.Err(); err != nil {
		if err != stdredis.Nil {
			err = errors.Wrapf(err, "redis[%s] Get %v error", c.bizID, key)
			logger.Errorf("redis[%s] Get[%v] failed: %v", c.bizID, key, err)
			// TODO redis error是否应该直接回源？
		}
	} else {
		if result, err := scan(cmd, value); err == nil {
			return result, nil
		} else {
			// TODO 结构错误，回源获取，业务方是否要感知这个错误？
			logger.Errorf("redis[%s] Get[%v] scan err:%v %v", c.bizID, key, err, cmd.String())
		}
	}
	//}

	var setValue interface{}

	if options.fetcher != nil {
		var shared bool
		value, err, shared = c.fetcherSf.Do(key, func() (interface{}, error) {
			return options.fetcher()
		})
		if err != nil {
			return nil, errors.Wrapf(err, "redis[%s] fetch %s from source error", c.bizID, key)
		}

		if !shared {
			setValue = value
		}
	} else {
		setValue = value
	}

	if setValue != nil {
		//span := itracing.BeginRecord(ctx, fmt.Sprintf("redis.Set.%v", c.bizID))
		c.Set(ctx, key, setValue, options.expiration)
		//itracing.EndRecord(span)
	}

	return value, nil
}

// 替换标准库的Set方法，对非简单类型做序列化
func (c *Client) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *stdredis.StatusCmd {
	if !isSimpleValue(value) {
		if setValue, err := json.Marshal(value); err == nil {
			value = setValue
		}
	}

	return c.Client.Set(ctx, key, value, expiration)
}

//implementing BinaryUnmarshaler
type dataWrapper struct {
	v interface{}
}

func (w *dataWrapper) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, w.v)
}

func scan(cmd *stdredis.StringCmd, value interface{}) (r interface{}, err error) {
	reflectValue := reflect.ValueOf(value)
	results := indirect(reflectValue)
	results = reflect.New(results.Type()).Elem()

	if isSimpleValue(value) {
		err = cmd.Scan(results.Addr().Interface())
		if err == nil {
			r = results.Interface()
		}
		return
	}

	w := &dataWrapper{
		v: results.Addr().Interface(),
	}
	err = cmd.Scan(w)
	if err == nil {
		// struct 结构返回指针
		if results.Kind() == reflect.Struct {
			r = results.Addr().Interface()
		} else {
			r = results.Interface()
		}
	}
	return
}
