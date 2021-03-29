package redis

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/techxmind/config"
	logger2 "github.com/techxmind/logger"
	"golang.org/x/sync/singleflight"
	stdredis "github.com/go-redis/redis/v8"
)

var (
	ConfigPathTpl = "/conf/db/redis/%s"
)

var (
	logger         = logger2.Named("redis")
)

type Client struct {
	*stdredis.Client

	// Fecher函数回源的限流控制
	fetcherSf singleflight.Group

	bizID string
}

var (
	instances    sync.Map
	insCreatorSf singleflight.Group
)

// 根据业务ID获取Redis客户端实例
func Ins(bizID string) (*Client, error) {
	if client, ok := instances.Load(bizID); ok {
		return client.(*Client), nil
	}

	client, err, _ := insCreatorSf.Do(bizID, func() (interface{}, error) {
		client, err := newClient(bizID)
		if err == nil {
			instances.Store(bizID, client)
		}
		return client, err
	})

	if err != nil {
		return nil, err
	}

	return client.(*Client), nil
}

// @param bizID Redis配置中的业务ID，业务ID映射到真实的Redis实例节点及DB
func newClient(bizID string) (*Client, error) {
	insInfo := config.String("redis." + bizID)
	if insInfo == "" {
		return nil, errors.Errorf("Redis config[%s] not found", bizID)
	}

	segments := strings.Split(insInfo, ".")
	insID := segments[0]
	db := 0
	if len(segments) > 1 {
		if v, err := strconv.Atoi(segments[1]); err != nil {
			return nil, errors.Wrapf(err, "Redis config[%s] invalid", bizID)
		} else {
			db = v
		}
	}

	insCfgPath := ConfigPath(insID)
	insCfg, err := config.Load(insCfgPath)
	if err != nil {
		return nil, errors.Wrapf(err, "Redis config[%s] get instance config[%s] err.", bizID, insID)
	}

	//TODO: 更多定制化配置
	options := &stdredis.Options{
		Addr:     insCfg.String("host") + ":" + insCfg.String("port"),
		Password: insCfg.String("auth"),
		PoolSize: int(insCfg.UintDefault("pool.max_connections", 30)),
		DB:       db,
	}
	if options.Addr == ":" {
		return nil, errors.Errorf("Redis config[%s] not exists.", bizID)
	}

	stdclient := stdredis.NewClient(options)

	client := &Client{
		Client: stdclient,
		bizID:  bizID,
	}

	client.AddHook(client)

	return client, nil
}

func ConfigPath(insID string) string {
	return fmt.Sprintf(ConfigPathTpl, insID)
}
