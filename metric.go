package redis

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/prometheus"
	stdprometheus "github.com/prometheus/client_golang/prometheus"
	stdredis "github.com/go-redis/redis/v8"
)

type ctxKey int

var (
	hostname    string
	serviceName string
	histogram   metrics.Histogram
	cmdTypeRegexp          = regexp.MustCompile(`^\s*(\w+)`)
	slowWarningTime        = 1.0
	slowErrorTime          = 3.0

	keyStartTime    ctxKey = 1
)

func init() {
	var err error
	hostname, err = os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	serviceName = filepath.Base(os.Args[0])

	opts := stdprometheus.HistogramOpts{
		Namespace: "",
		Help:      "redis histogram",
		Name:      "redis_time",
		Buckets: []float64{
			.005, .01, .05, .1, .3, .5, .7, 1, 3, 5,
		},
	}
	labels := []string{"hostname", "service", "biz", "type", "result"}
	histogram = prometheus.NewHistogramFrom(opts, labels)
}


func (c *Client) BeforeProcess(ctx context.Context, cmd stdredis.Cmder) (context.Context, error) {
	ctx = context.WithValue(ctx, keyStartTime, time.Now())
	return ctx, nil
}

func (c *Client) AfterProcess(ctx context.Context, cmd stdredis.Cmder) error {
	start := ctx.Value(keyStartTime)
	if start == nil {
		return nil
	}
	elapse := time.Since(start.(time.Time)).Seconds()
	cmdStr := cmd.String()
	ma := cmdTypeRegexp.FindStringSubmatch(cmdStr)
	if len(ma) > 0 {
		logger.Debugf("\033[0;32mCMD:\033[0m%s \033[0;32mELAPSE:\033[0m%.3fs", cmdStr, elapse)
		cmdType := ma[1]
		metric(c.bizID, cmdType, cmd.Err() == nil, elapse)
	}
	return nil
}

func (c *Client) BeforeProcessPipeline(ctx context.Context, cmds []stdredis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (c *Client) AfterProcessPipeline(ctx context.Context, cmds []stdredis.Cmder) error {
	return nil
}

func metric(bizID, cmdType string, succ bool, useTime float64) {
	result := "ok"
	if !succ {
		result = "err"
	}
	histogram.With(
		"hostname", hostname,
		"service", serviceName,
		"biz", bizID,
		"type", cmdType,
		"result", result,
	).Observe(useTime)
}
