/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package logger

import (
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/natefinch/lumberjack"
	"github.com/sirupsen/logrus"
)

type Logger interface {
	Debugf(msg string, args ...interface{})
	Infof(msg string, args ...interface{})
	Warnf(msg string, args ...interface{})
	Errorf(msg string, args ...interface{})
	Level(level string)
	OutputPath(path string) (err error)
}

var rLog Logger

func init() {
	r := &defaultLogger{
		logger: logrus.New(),
	}
	level := os.Getenv("GO_SCHEDULERX_WORKER_LOG_LEVEL")
	switch strings.ToLower(level) {
	case "debug":
		r.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		r.logger.SetLevel(logrus.WarnLevel)
	case "error":
		r.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		r.logger.SetLevel(logrus.FatalLevel)
	default:
		r.logger.SetLevel(logrus.InfoLevel)
	}
	rLog = r
}

type defaultLogger struct {
	logger *logrus.Logger
}

func (l *defaultLogger) Debugf(msg string, args ...interface{}) {
	l.logger.Debugf(msg, args...)
}

func (l *defaultLogger) Infof(msg string, args ...interface{}) {
	l.logger.Infof(msg, args...)
}

func (l *defaultLogger) Warnf(msg string, args ...interface{}) {
	l.logger.Warnf(msg, args...)
}

func (l *defaultLogger) Errorf(msg string, args ...interface{}) {
	l.logger.Errorf(msg, args...)
}

func (l *defaultLogger) Level(level string) {
	switch strings.ToLower(level) {
	case "debug":
		l.logger.SetLevel(logrus.DebugLevel)
	case "warn":
		l.logger.SetLevel(logrus.WarnLevel)
	case "error":
		l.logger.SetLevel(logrus.ErrorLevel)
	case "fatal":
		l.logger.SetLevel(logrus.FatalLevel)
	default:
		l.logger.SetLevel(logrus.InfoLevel)
	}
}

type Config struct {
	OutputPath    string
	MaxFileSizeMB int
	MaxBackups    int
	MaxAges       int
	Compress      bool
	LocalTime     bool
}

func (c *Config) Logger() *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   filepath.ToSlash(c.OutputPath),
		MaxSize:    c.MaxFileSizeMB, // MB
		MaxBackups: c.MaxBackups,
		MaxAge:     c.MaxAges,  // days
		Compress:   c.Compress, // disabled by default
		LocalTime:  c.LocalTime,
	}
}

func defaultConfig() Config {
	userHome, _ := os.UserHomeDir()
	return Config{
		OutputPath:    path.Join(userHome, "logs/schedulerx/schedulerx.log"),
		MaxFileSizeMB: 10,
		MaxBackups:    5,
		MaxAges:       3,
		Compress:      false,
		LocalTime:     true,
	}
}

func (l *defaultLogger) Config(conf Config) (err error) {
	l.logger.Out = conf.Logger()
	return
}

func (l *defaultLogger) OutputPath(path string) (err error) {
	config := defaultConfig()
	config.OutputPath = path

	l.logger.Out = config.Logger()
	return
}

// SetLogger use specified logger user customized, in general, we suggest user to replace the default logger with specified
func SetLogger(logger Logger) {
	rLog = logger
}

func SetLogLevel(level string) {
	if level == "" {
		return
	}
	rLog.Level(level)
}

func SetOutputPath(path string) (err error) {
	if "" == path {
		return
	}

	return rLog.OutputPath(path)
}

func Debugf(msg string, args ...interface{}) {
	rLog.Debugf(msg, args...)
}

func Infof(msg string, args ...interface{}) {
	rLog.Infof(msg, args...)
}

func Warnf(msg string, args ...interface{}) {
	rLog.Warnf(msg, args...)
}

func Errorf(msg string, args ...interface{}) {
	rLog.Errorf(msg, args...)
}
