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
	poolMgrOnce sync.Once
	poolMgr     *ConnPoolManager
)

// PoolFactory creates a ConnPool for a given groupId.
type PoolFactory func(ctx context.Context, groupId string) ConnPool

// ConnPoolManager manages per-groupId isolated connection pools.
type ConnPoolManager struct {
	mu           sync.RWMutex
	pools        map[string]ConnPool
	defaultGroup string
	factory      PoolFactory
	ctx          context.Context
	onNewPool    func(groupId string, p ConnPool)
}

type ManagerOption func(*ConnPoolManager)

func WithDefaultGroup(groupId string) ManagerOption {
	return func(m *ConnPoolManager) {
		m.defaultGroup = groupId
	}
}

func WithOnNewPool(fn func(groupId string, p ConnPool)) ManagerOption {
	return func(m *ConnPoolManager) {
		m.onNewPool = fn
	}
}

func InitConnPoolManager(ctx context.Context, factory PoolFactory, opts ...ManagerOption) {
	poolMgrOnce.Do(func() {
		poolMgr = &ConnPoolManager{
			pools:   make(map[string]ConnPool),
			factory: factory,
			ctx:     ctx,
		}
		for _, opt := range opts {
			opt(poolMgr)
		}
	})
}

func GetConnPoolManager() *ConnPoolManager {
	return poolMgr
}

// GetConnPool returns the default (parent group) ConnPool for backward compatibility.
// Actor layer and other legacy code that doesn't need per-group isolation can keep using this.
func GetConnPool() ConnPool {
	if poolMgr == nil {
		return nil
	}
	return poolMgr.GetDefault()
}

type connPoolCtxKey struct{}

// WithConnPool embeds a ConnPool in the context so that downstream code
// (processors, SchedulerxServerPid, etc.) can route to the correct server.
func WithConnPool(ctx context.Context, p ConnPool) context.Context {
	return context.WithValue(ctx, connPoolCtxKey{}, p)
}

// ConnPoolFromContext extracts the ConnPool embedded by WithConnPool.
// Returns nil if the context does not carry a pool.
func ConnPoolFromContext(ctx context.Context) ConnPool {
	if ctx == nil {
		return nil
	}
	if p, ok := ctx.Value(connPoolCtxKey{}).(ConnPool); ok {
		return p
	}
	return nil
}

// ConnPoolCtxForGroup returns a context with the ConnPool for the given groupId.
// Used when proactively sending messages to the server (e.g. status reports)
// where groupId is known but no originating connection context is available.
func ConnPoolCtxForGroup(groupId string) context.Context {
	if mgr := GetConnPoolManager(); mgr != nil && groupId != "" {
		return WithConnPool(context.Background(), mgr.GetOrCreate(groupId))
	}
	return context.Background()
}

func (m *ConnPoolManager) GetDefault() ConnPool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pools[m.defaultGroup]
}

// GetOrCreate returns the ConnPool for the given groupId, lazily creating one via the factory if absent.
func (m *ConnPoolManager) GetOrCreate(groupId string) ConnPool {
	m.mu.RLock()
	p, ok := m.pools[groupId]
	m.mu.RUnlock()
	if ok {
		return p
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if p, ok = m.pools[groupId]; ok {
		return p
	}
	p = m.factory(m.ctx, groupId)
	m.pools[groupId] = p
	if m.onNewPool != nil {
		go m.onNewPool(groupId, p)
	}
	return p
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

// NewSingleConnPool creates a single-connection pool with the given dialer and options.
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

	go pool.onReconnectTrigger(ctx)

	if options.addrChangedSignalCh != nil {
		go pool.onAddrChanged(ctx)
	}

	return pool
}

// dialAndHandshake creates a new TCP connection and runs the post-dialer (handshake).
// Must be called with p.lock held.
func (p *singleConnPool) dialAndHandshake(ctx context.Context) (net.Conn, error) {
	conn, err := p.dialer()
	if err != nil {
		return nil, err
	}
	if postDialer := p.options.postDialer; postDialer != nil {
		if err := postDialer(ctx, conn); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}
	p.conn = conn
	return conn, nil
}

// getOrCreateConn returns the existing connection or creates one if absent.
// Unlike replaceConn, it does NOT close an existing connection.
func (p *singleConnPool) getOrCreateConn(ctx context.Context) (net.Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.conn != nil {
		return p.conn, nil
	}
	return p.dialAndHandshake(ctx)
}

// replaceConn unconditionally closes the existing connection and creates a new one.
// Used by onAddrChanged and onReconnectTrigger.
func (p *singleConnPool) replaceConn(ctx context.Context) (net.Conn, error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.conn != nil {
		_ = p.conn.Close()
		p.conn = nil
	}
	return p.dialAndHandshake(ctx)
}

func (p *singleConnPool) Get(ctx context.Context) (net.Conn, error) {
	p.lock.RLock()
	if p.conn != nil {
		defer p.lock.RUnlock()
		return p.conn, nil
	}
	p.lock.RUnlock()
	return p.getOrCreateConn(ctx)
}

func (p *singleConnPool) ReconnectTrigger() chan struct{} {
	return p.reconnectSignalCh
}

func (p *singleConnPool) onReconnectTrigger(ctx context.Context) {
	for range p.reconnectSignalCh {
		if _, err := p.replaceConn(ctx); err != nil {
			logger.Errorf("Reconnect server failed after connection isn't available, err=%s", err.Error())
		}
	}
}

func (p *singleConnPool) onAddrChanged(ctx context.Context) {
	for range p.options.addrChangedSignalCh {
		if _, err := p.replaceConn(ctx); err != nil {
			logger.Errorf("Reconnect server failed after addr is changed, err=%s", err.Error())
		}
	}
}
