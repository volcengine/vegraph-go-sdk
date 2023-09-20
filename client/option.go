// Copyright 2022 Beijing Volcanoengine Technology Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

type Option func(*Options)

type Options struct {
	DefaultTable string

	authType       AuthType
	userName       string
	passwordSha256 string
	authPort       int

	HostPorts  []string
	domainName string
	port       int

	MaxIdle         int
	MaxIdleGlobal   int
	MaxIdleTimeout  time.Duration
	RpcTimeout      time.Duration
	DecodeUseStruct bool
	// compression 是否开启返回值压缩，用于大数据量下降低带宽。需要集群支持,业务侧无感知。开了可能会导致cpu上升。
	compression bool
}

type AuthType int

const (
	AuthType_DisableAuth AuthType = iota
	AuthType_PasswordSha256
	AuthType_PasswordEncrypt
)

func newDefaultOptions() *Options {
	return &Options{
		authPort:       6287,
		authType:       AuthType_DisableAuth,
		MaxIdle:        DefaultMaxIdle,
		MaxIdleTimeout: DefaultMaxIdleTimeout,
		RpcTimeout:     DefaultRpcTimeout,
		MaxIdleGlobal:  DefaultMaxIdleGlobal,
	}
}

func WithDefaultTable(table string) Option {
	return func(op *Options) {
		op.DefaultTable = table
	}
}

// Specify hosts for the client to connect to
func WithHostPort(hosts ...string) Option {
	return func(op *Options) {
		op.HostPorts = append(op.HostPorts, hosts...)
	}
}

// Authentication
func WithUserPwd(username, password string) Option {
	return func(op *Options) {
		op.userName = username
		op.authType = AuthType_PasswordSha256

		hash := sha256.New()
		hash.Write([]byte(password))
		op.passwordSha256 = hex.EncodeToString(hash.Sum(nil))
	}
}

func WithUserPwdEncrypted(username, password string) Option {
	return func(op *Options) {
		op.userName = username
		op.authType = AuthType_PasswordEncrypt
		op.passwordSha256 = password
	}
}

func WithAuthPort(port int) Option {
	return func(op *Options) {
		op.authPort = port
	}
}

func WithServiceNamePort(domainName string, port int) Option {
	return func(op *Options) {
		op.domainName = domainName
		op.port = port
	}
}

func WithRpcTimeout(d time.Duration) Option {
	return func(op *Options) {
		op.RpcTimeout = d
	}
}
