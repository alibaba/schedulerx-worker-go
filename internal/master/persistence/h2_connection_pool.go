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
)

const memoryModeDataSourceName = "file::memory:?cache=shared"

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

	files, err := filepath.Glob("schedulerx2_*_sqlite3.db")
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		if err = os.Remove(file); err != nil {
			return nil, err
		}
	}

	dataSourceName := "sqlite3.db"
	if options.dataSourceName != "" {
		dataSourceName = options.dataSourceName
	}
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(5) // default 2
	return &H2ConnectionPool{DB: db}, nil
}
