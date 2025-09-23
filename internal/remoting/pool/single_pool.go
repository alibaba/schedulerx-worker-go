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

package pool

import (
	"context"
	"net"
	"sync"

	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	poolOnce sync.Once
	connPool ConnPool
)

func InitConnPool(ctx context.Context, dialer func() (net.Conn, error), opts ...Option) {
	poolOnce.Do(func() {
		connPool = newSingleConnPool(ctx, dialer, opts...)
	})
}

// GetConnPool first executes InitConnPool and then calls it, otherwise it returns nil
func GetConnPool() ConnPool {
	return connPool
}

type ConnPool interface {
	Get(ctx context.Context) (net.Conn, error)
	ReconnectTrigger() chan struct{}
}

type singleConnPool struct {
	lock              sync.RWMutex
	conn              net.Conn
	dialer            func() (net.Conn, error)
	reconnectSignalCh chan struct{}
	options           *Options
}

type Options struct {
	postDialer          func(context.Context, net.Conn) error
	addrChangedSignalCh chan struct{}
}

type Option func(*Options)

func WithPostDialer(postDialer func(context.Context, net.Conn) error) Option {
	return func(o *Options) {
		o.postDialer = postDialer
	}
}

func WithAddrChangedSignalCh(addrChangedSignalCh chan struct{}) Option {
	return func(o *Options) {
		o.addrChangedSignalCh = addrChangedSignalCh
	}
}

func newSingleConnPool(ctx context.Context, dialer func() (net.Conn, error), opts ...Option) ConnPool {
	options := new(Options)
	for _, opt := range opts {
		opt(options)
	}

	pool := &singleConnPool{
		dialer:            dialer,
		reconnectSignalCh: make(chan struct{}, 3),
		options:           options,
	}

	// network is broken or heartbeat timeout
	go pool.onReconnectTrigger(ctx)

	// server addr changed
	if options.addrChangedSignalCh != nil {
		go pool.onAddrChanged(ctx)
	}

	return pool
}

func (p *singleConnPool) newConn(ctx context.Context) (net.Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.conn != nil {
		_ = p.conn.Close()
	}

	conn, err := p.dialer()
	if err != nil {
		return nil, err
	}

	// handshake success means connection is truly established
	if postDialer := p.options.postDialer; postDialer != nil {
		if err := postDialer(ctx, conn); err != nil {
			return nil, err
		}
	}
	p.conn = conn
	return conn, nil
}

func (p *singleConnPool) Get(ctx context.Context) (net.Conn, error) {
	p.lock.RLock()
	if p.conn == nil {
		p.lock.RUnlock()
		// create a new connection if there is no existing connection
		return p.newConn(ctx)
	}
	defer p.lock.RUnlock()
	return p.conn, nil
}

func (p *singleConnPool) ReconnectTrigger() chan struct{} {
	return p.reconnectSignalCh
}

func (p *singleConnPool) onReconnectTrigger(ctx context.Context) {
	for range p.reconnectSignalCh {
		if _, err := p.newConn(ctx); err != nil {
			logger.Errorf("Reconnect server failed after connection isn't available, err=%s", err.Error())
		}
	}
}

func (p *singleConnPool) onAddrChanged(ctx context.Context) {
	for range p.options.addrChangedSignalCh {
		if _, err := p.newConn(ctx); err != nil {
			logger.Errorf("Reconnect server failed after addr if changed, err=%s", err.Error())
		}
	}
}
