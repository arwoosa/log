package log

import (
	"github.com/fluent/fluent-logger-golang/fluent"
	"github.com/pkg/errors"
)

const _ENV_VALUE_TARGET_FLUENTD = "fluentd"

type fluentLog struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`

	config *fluent.Config
}

func (rl *fluentLog) new() (*fluent.Fluent, error) {
	if rl.config != nil {
		return fluent.New(*rl.config)
	}
	if rl.Host == "" {
		return nil, errors.New("missing fluentd host")
	}
	if rl.Port == 0 {
		return nil, errors.New("missing fluentd port")
	}

	rl.config = &fluent.Config{
		FluentHost: rl.Host,
		FluentPort: rl.Port,
	}

	return fluent.New(*rl.config)
}
