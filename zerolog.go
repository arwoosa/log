package log

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/pkgerrors"
)

var (
	zeroLoglevelMap = map[string]zerolog.Level{
		_ENV_VALUE_LEVEL_DEBUG: zerolog.DebugLevel,
		_ENV_VALUE_LEVEL_INFO:  zerolog.InfoLevel,
		_ENV_VALUE_LEVEL_WARN:  zerolog.WarnLevel,
		_ENV_VALUE_LEVEL_ERROR: zerolog.ErrorLevel,
	}
)

func (lc *LoggerConf) NewLogger(service, pid string) (Logger, error) {
	var multi zerolog.LevelWriter

	// init writer
	targetStr := os.Getenv(_ENV_NAME_TARGET)
	if targetStr == "" {
		targetStr = _ENV_VALUE_TARGET_OS
	}
	targetAry := strings.Split(targetStr, "|")
	var writers []io.Writer
	for _, s := range targetAry {
		switch s {
		case _ENV_VALUE_TARGET_OS:
			writers = append(writers, zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
		case _ENV_VALUE_TARGET_FLUENTD:
			if lc.FluentLog == nil {
				return nil, errors.New("config missing fluentlog")
			}
			writers = append(writers, newZeroFluent(lc.FluentLog))
		}
	}
	if len(writers) <= 0 {
		multi = zerolog.MultiLevelWriter(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339})
	} else {
		multi = zerolog.MultiLevelWriter(writers...)
	}

	// UNIX Time is faster and smaller than most timestamps
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.ErrorStackMarshaler = pkgerrors.MarshalStack

	// set log level
	envLogLevel := os.Getenv(_ENV_NAME_LEVEL)
	level, ok := zeroLoglevelMap[envLogLevel]
	if !ok {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	logger := zerolog.New(multi).With().Stack().Timestamp().
		Str("service", service).Str("pid", pid).Logger()
	return &zeroLogImpl{Logger: logger}, nil
}

type zeroLogImpl struct {
	zerolog.Logger
}

func (impl *zeroLogImpl) Info(msg string) {
	impl.Logger.Info().Msg(msg)
}
func (impl *zeroLogImpl) Debug(msg string) {
	impl.Logger.Debug().Msg(msg)
}
func (impl *zeroLogImpl) Warn(msg string) {
	impl.Logger.Warn().Msg(msg)
}
func (impl *zeroLogImpl) Error(msg string) {
	impl.Logger.Error().Msg(msg)
}

func (impl *zeroLogImpl) WarnPkg(e error) {
	impl.Logger.Warn().Err(e).Msg("")
}
func (impl *zeroLogImpl) ErrorPkg(e error) {
	impl.Logger.Error().Err(e).Msg("")
}

func newZeroFluent(fluentd *fluentLog) io.Writer {
	return &zeroFluent{
		fluentLog: fluentd,
	}
}

type zeroFluent struct {
	*fluentLog
}

func (sf *zeroFluent) Write(p []byte) (int, error) {
	data := map[string]any{}
	err := json.Unmarshal(p, &data)
	if err != nil {
		return 0, err
	}
	logger, err := sf.new()
	if err != nil {
		return 0, err
	}
	defer logger.Close()
	t := time.Unix(int64(data["time"].(float64)), 0)
	delete(data, "time")
	tag := fmt.Sprintf("%s_%s.log", data["service"], data["level"])
	err = logger.PostWithTime(tag, t, data)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
