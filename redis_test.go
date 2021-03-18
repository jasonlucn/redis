package redis

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/suite"
	"github.com/techxmind/config"
)

type redisTestSuite struct {
	suite.Suite
	rds *miniredis.Miniredis
	bizID string
	invalidBizID string
}

func (suite *redisTestSuite) SetupSuite() {
	rds, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	suite.rds = rds

	suite.bizID = "biz1"
	suite.invalidBizID = "biz2"

	// mock config
	cfgStorage := config.NewMockAsyncer(true)
	config.RegisterAsyner("mock", &config.AsyncerArgs{
		Ins:          cfgStorage,
		CacheTime:    1 * time.Millisecond,
		RefreshAsync: true,
	})
	config.Set(config.DefaultConfSourceKey, "mock")
	config.Set("redis", map[string]interface{}{
		suite.bizID:        "instance1",
		suite.invalidBizID: "instance2",
	})
	cfgValue := fmt.Sprintf(`{"host":"%s", "port":"%s"}`, rds.Host(), rds.Port())
	cfgStorage.Set(ConfigPath("instance1"), []byte(cfgValue))
}

func (suite *redisTestSuite) TearDownSuite() {
	suite.rds.Close()
}

func (suite *redisTestSuite) TestIns() {
	c, err := Ins(suite.bizID)
	assert.NoError(suite.T(), err)
	assert.NotNil(suite.T(), c)
	c, err = Ins(suite.invalidBizID)
	assert.Error(suite.T(), err)
}

func TestRedis(t *testing.T) {
	suite.Run(t, new(redisTestSuite))
}
