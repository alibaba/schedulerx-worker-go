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

package connpool

import (
	"context"
	"net"
	"sync"

	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	_ ConnPool = &singleConnPool{}

	connPool ConnPool
	once     sync.Once
	lock     sync.RWMutex
)

func InitConnPool(pool ConnPool) {
	once.Do(func() {
		connPool = pool
	})
}

// GetConnPool first executes InitConnPool and then calls it, otherwise it returns nil
func GetConnPool() ConnPool {
	lock.RLock()
	defer lock.RUnlock()
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

func NewSingleConnPool(ctx context.Context, dialer func() (net.Conn, error), opts ...Option) ConnPool {
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
	p.clean()

	conn, err := p.dialer()
	if err != nil {
		return nil, err
	}

	p.lock.Lock()
	defer p.lock.Unlock()
	p.conn = conn

	// handshake success means connection is truly established
	if postDialer := p.options.postDialer; postDialer != nil {
		if err := postDialer(ctx, conn); err != nil {
			return nil, err
		}
	}

	return conn, nil
}

func (p *singleConnPool) Get(ctx context.Context) (net.Conn, error) {
	if !p.isConnExisted() {
		// create a new connection if there is no existing connection
		return p.newConn(ctx)
	}

	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.conn, nil
}

func (p *singleConnPool) ReconnectTrigger() chan struct{} {
	return p.reconnectSignalCh
}

func (p *singleConnPool) isConnExisted() bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.conn != nil
}

func (p *singleConnPool) onReconnectTrigger(ctx context.Context) {
	for {
		select {
		case <-p.reconnectSignalCh:
			if _, err := p.newConn(ctx); err != nil {
				logger.Errorf("Reconnect server failed after connection isn't available, err=%s", err.Error())
			}
		}
	}
}

func (p *singleConnPool) onAddrChanged(ctx context.Context) {
	for {
		select {
		case <-p.options.addrChangedSignalCh:
			if _, err := p.newConn(ctx); err != nil {
				logger.Errorf("Reconnect server failed after addr if changed, err=%s", err.Error())
			}
		}
	}
}

func (p *singleConnPool) clean() {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
}
