package log

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	_CTX_KEY = "ctx_log_key"

	_ENV_NAME_LEVEL  = "LOG_LEVEL"
	_ENV_NAME_TARGET = "LOG_TARGET"

	_ENV_VALUE_TARGET_OS = "os"

	_ENV_VALUE_LEVEL_DEBUG = "debug"
	_ENV_VALUE_LEVEL_INFO  = "info"
	_ENV_VALUE_LEVEL_WARN  = "warn"
	_ENV_VALUE_LEVEL_ERROR = "error"
)

type Logger interface {
	Info(string)
	Debug(string)
	Warn(string)
	Error(string)
	ErrorPkg(error)
	WarnPkg(error)
}

type ctxType string

func SetByCtx(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, ctxType(_CTX_KEY), l)
}

func GetByCtx(ctx context.Context) Logger {
	cltInter := ctx.Value(ctxType(_CTX_KEY))
	if clt, ok := cltInter.(Logger); ok {
		return clt
	}
	return nil
}

func SetByReq(req *http.Request, l Logger) *http.Request {
	return req.WithContext(SetByCtx(req.Context(), l))
}

func GetByReq(req *http.Request) Logger {
	return GetByCtx(req.Context())
}

func SetByGinCtx(c *gin.Context, l Logger) {
	c.Set(_CTX_KEY, l)
}

func GetByGinCtx(c *gin.Context) Logger {
	l, ok := c.Get(_CTX_KEY)
	if !ok {
		return nil
	}
	return l.(Logger)
}

type LoggerDI interface {
	NewLogger(service string, pid string) (Logger, error)
}

type LoggerConf struct {
	FluentLog *fluentLog `yaml:"fluentd,omitempty"`
}

func NewLogerConfWithFluentd(host string, port int) *LoggerConf {
	return &LoggerConf{
		FluentLog: &fluentLog{Host: host, Port: port},
	}
}
