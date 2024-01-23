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

package persistence

import (
	"database/sql"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"

	"github.com/alibaba/schedulerx-worker-go/logger"
)

const memoryModeDataSourceName = ":memory:"

type H2ConnectionPool struct {
	*sql.DB
}

type Options struct {
	dataSourceName string
}

type Option func(o *Options)

func WithDataSourceName(dataSourceName string) Option {
	return func(o *Options) {
		o.dataSourceName = dataSourceName
	}
}

func NewH2ConnectionPool(opts ...Option) (*H2ConnectionPool, error) {
	options := new(Options)
	for _, opt := range opts {
		opt(options)
	}

	dataSourceName := "sqlite3.db"
	if options.dataSourceName != "" {
		dataSourceName = options.dataSourceName
	}

	files, err := filepath.Glob("schedulerx2_*_sqlite3.db")
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return nil, err
		}
	}

	// memory mode no need creating local file
	if dataSourceName != memoryModeDataSourceName {
		logger.Infof("Creating %s sqlite3...", dataSourceName)
		file, err := os.Create(dataSourceName)
		if err != nil {
			return nil, err
		}
		defer file.Close()

		logger.Infof("sqlite3 DB=%s created", dataSourceName)
	}

	sqliteDatabase, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}

	return &H2ConnectionPool{
		sqliteDatabase,
	}, nil
}
