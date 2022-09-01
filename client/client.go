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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/volcengine/vegraph-go-sdk/kitex_gen/base"

	kitex "github.com/cloudwego/kitex/client"
	"github.com/cloudwego/kitex/pkg/connpool"
	"github.com/cloudwego/kitex/pkg/endpoint"
	"github.com/cloudwego/kitex/transport"
	"github.com/golang/snappy"

	"github.com/volcengine/vegraph-go-sdk/authentication"
	"github.com/volcengine/vegraph-go-sdk/gerrors"
	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph"
	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph/bytegraphservice"
	"github.com/volcengine/vegraph-go-sdk/provider/protocol"
	"github.com/volcengine/vegraph-go-sdk/structure"
)

const (
	BinaryV1MagicNumber            = 0x0101
	BinaryV1CompressionMagicNumber = 0x0102
)

const (
	DefaultMaxIdle        = 10
	DefaultMaxIdleGlobal  = 2147483647
	DefaultMaxIdleTimeout = time.Millisecond * 2500
)

type Client struct {
	table  string
	klient bytegraphservice.Client

	// authentication
	authType AuthType
	auth     authentication.IClient
	mux      sync.RWMutex

	decodeUseStruct bool
	compression     bool
}

type DebugKey struct {
}

func NewDebugMiddleWare() endpoint.Middleware {
	return func(next endpoint.Endpoint) endpoint.Endpoint {
		return func(ctx context.Context, req, resp interface{}) (err error) {
			err = next(ctx, req, resp)
			val := ctx.Value(DebugKey{})
			if debug, ok := val.(bool); ok && debug {
				fmt.Println("-----------------")
				fmt.Printf("request: %+v\n", req)
				fmt.Printf("resp: %+v\n", resp)
				fmt.Printf("err: %+v\n", err)
				fmt.Println("-----------------")
			}
			return
		}
	}
}

// Create a goroutine-safe ByteGraph client.
func NewClient(destService string, ops ...Option) (*Client, error) {
	opts := newDefaultOptions()
	for _, do := range ops {
		do(opts)
	}
	client := &Client{
		table:           opts.DefaultTable,
		authType:        opts.authType,
		decodeUseStruct: opts.DecodeUseStruct,
		compression:     opts.compression,
		mux:             sync.RWMutex{},
	}

	var authHostPorts []string
	if len(opts.domainName) != 0 {
		if len(destService) == 0 {
			destService = "anything"
		}
		es, err := net.LookupHost(opts.domainName)
		if err != nil {
			err = fmt.Errorf("LookupHost(%v): %s", opts.domainName, err.Error())
			return nil, err
		}
		for _, host := range es {
			ip := net.ParseIP(host).String()
			opts.HostPorts = append(opts.HostPorts, net.JoinHostPort(ip, strconv.Itoa(opts.port)))
			if client.authType == AuthType_PasswordSha256 {
				authHostPorts = append(authHostPorts, fmt.Sprintf("%v:%v", host, opts.authPort))
			}
		}

	} else if client.authType == AuthType_PasswordSha256 && len(opts.HostPorts) > 0 {
		if len(destService) == 0 {
			destService = "anything"
		}
		for _, hostPort := range opts.HostPorts {
			host, _, err := net.SplitHostPort(hostPort)
			if err != nil {
				err = fmt.Errorf("SplitHostPort(%v): %s", hostPort, err.Error())
				return nil, err
			}
			authHostPorts = append(authHostPorts, fmt.Sprintf("%v:%v", host, opts.authPort))
		}
	}
	client.auth = authentication.NewClient(authentication.WithUserPwdSha256(opts.userName, opts.passwordSha256),
		authentication.WithHostPorts(authHostPorts))

	kitexOpts := make([]kitex.Option, 0)
	if len(opts.HostPorts) > 0 {
		kitexOpts = append(kitexOpts, kitex.WithHostPorts(opts.HostPorts...))
	}
	kitexOpts = append(kitexOpts, kitex.WithMiddleware(NewDebugMiddleWare()))
	kitexOpts = append(kitexOpts,
		kitex.WithLongConnection(connpool.IdleConfig{
			MaxIdleGlobal:     opts.MaxIdleGlobal,
			MaxIdlePerAddress: opts.MaxIdle,
			MaxIdleTimeout:    opts.MaxIdleTimeout,
		}),
		kitex.WithTransportProtocol(transport.Framed))

	// create kite client
	clt, err := bytegraphservice.NewClient(destService, kitexOpts...)
	if err != nil {
		return nil, gerrors.New(gerrors.ErrorCode_NETWORK_ERROR, err)
	}
	client.klient = clt
	return client, nil
}

func (c *Client) getKlient() bytegraphservice.Client {
	c.mux.RLocker().Lock()
	defer c.mux.RLocker().Unlock()
	return c.klient
}

func (c *Client) setklient(klient bytegraphservice.Client) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.klient = klient
}

func (c *Client) setAuthClient(authClient authentication.IClient) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.auth = authClient
}

func authBase(reqBase *base.Base, newUserExtra map[string]string) *base.Base {
	if reqBase == nil {
		reqBase = &base.Base{}
	}
	if reqBase.Extra == nil {
		reqBase.Extra = make(map[string]string)
	}

	userExtra := make(map[string]string)
	_ = json.Unmarshal([]byte(reqBase.Extra[authentication.UserExtra]), &userExtra)
	for k, v := range newUserExtra {
		userExtra[k] = v
	}

	userExtraJson, _ := json.Marshal(userExtra)
	reqBase.Extra[authentication.UserExtra] = string(userExtraJson)
	return reqBase
}

func (c *Client) submitBatchRequestAuthSha256(ctx context.Context, request *bytegraph.GremlinQueryRequest) (resp *bytegraph.GremlinQueryResponse, err error) {
	var sessID string
	for retryCnt := 2; retryCnt > 0; retryCnt-- {
		sessID, err = c.auth.Session(false)
		if err != nil {
			return nil, err
		}

		request.Base = authBase(request.Base, map[string]string{
			authentication.PersistSessionKey: sessID,
			authentication.PersistUserKey:    c.auth.UserName(),
		})

		resp, err = c.getKlient().GremlinQuery(ctx, request)
		if err == nil {
			return resp, nil
		}
		if !authentication.NotExistErr(err.Error()) {
			return nil, gerrors.New(gerrors.ErrorCode_NETWORK_ERROR, err)
		}

		if _, err = c.auth.Session(true); err == nil || authentication.NotExistErr(err.Error()) {
			continue
		}
	}

	return nil, gerrors.New(gerrors.ErrorCode_NETWORK_ERROR, err)
}

func (c *Client) submitBatchRequestAuthEncrypted(ctx context.Context, request *bytegraph.GremlinQueryRequest) (*bytegraph.GremlinQueryResponse, error) {
	request.Base = authBase(request.Base, map[string]string{
		authentication.PersistPwdKey:  c.auth.Password(),
		authentication.PersistUserKey: c.auth.UserName(),
	})
	return c.getKlient().GremlinQuery(ctx, request)
}

// table is used to specify a temporary table in replace of default table to use in the request.
func (c *Client) Submit(ctx context.Context, query string, table ...string) (structure.Element, error) {
	request := &bytegraph.GremlinQueryRequest{
		Queries:   []string{query},
		UseBinary: true,
	}
	reqTable, err := c.reqTable(table...)
	if err != nil {
		return nil, err
	}
	request.Table = reqTable
	elems, _, errs := c.submitBatchRequest(ctx, request)
	var elem structure.Element
	if len(errs) > 0 {
		err = errs[0]
	} else {
		err = gerrors.New(gerrors.ErrorCode_SYSTEM_ERROR, fmt.Errorf("unexpected error number returned by submitTemplates: %v", errs))
	}
	if len(elems) > 0 {
		elem = elems[0]
	}
	return elem, err
}

// 1. size of []error keeps equal to the number of queries in request;
// 2. the order of []error is keep the same as the order of queries in request;
// 3. ErrorCode_SUCCESS is promised to be converted to nil when returned by []error
func (c *Client) submitBatchRequest(ctx context.Context, request *bytegraph.GremlinQueryRequest) ([]structure.Element, []*structure.Extra, []error) {
	if len(request.Queries) == 0 {
		return []structure.Element{}, []*structure.Extra{}, []error{}
	}
	var batchSize = len(request.Queries)
	if c.compression {
		request.Compression = c.compression
	}
	var err error
	var resp *bytegraph.GremlinQueryResponse
	if c.authType == AuthType_PasswordSha256 {
		resp, err = c.submitBatchRequestAuthSha256(ctx, request)
	} else if c.authType == AuthType_PasswordEncrypt {
		resp, err = c.submitBatchRequestAuthEncrypted(ctx, request)
	} else {
		resp, err = c.getKlient().GremlinQuery(ctx, request)
	}

	if err != nil {
		return nil, nil, gerrors.DuplicateErr(gerrors.New(gerrors.ErrorCode_NETWORK_ERROR, err), batchSize)
	}

	results := make([]structure.Element, batchSize)
	extras := make([]*structure.Extra, batchSize)
	errs := make([]error, batchSize)
	rets := resp.BatchBinaryRet
	if batchSize != len(resp.BatchErrCode) {
		panic(fmt.Sprintf("unexpected batch size of req and resp not equal:%+v %+v %+v %+v", batchSize, resp.BatchErrCode, request, resp))
	}
	for i, berr := range resp.BatchErrCode {
		if berr != bytegraph.ErrorCode_SUCCESS {
			errs[i] = gerrors.New(gerrors.ErrorCode(berr), errors.New(resp.BatchDesc[i]))
			continue
		}
		retBinary := rets[i]
		var res structure.Element
		res, err = DecodeEx(retBinary, c.decodeUseStruct)
		if err != nil {
			errs[i] = err
			continue
		}
		results[i] = res
		if i < len(resp.Costs) {
			extras[i] = &structure.Extra{Cost: resp.Costs[i]}
		}
	}
	return results, extras, errs
}

func (c *Client) reqTable(tables ...string) (string, error) {
	var table string
	switch {
	case len(tables) > 1:
		return table, gerrors.New(gerrors.ErrorCode_INVALID_REQUEST, errors.New("SubmitTemplates takes at most one table argument"))
	case len(tables) == 1 && tables[0] != "":
		table = tables[0]
	case (len(tables) == 0 || tables[0] == "") && c.table != "":
		table = c.table
	default:
		return table, gerrors.New(gerrors.ErrorCode_INVALID_REQUEST, errors.New("no table specified for c request"))
	}
	return table, nil
}

func DecodeEx(bts []byte, useStruct bool) (ret structure.Element, err error) {
	defer func() {
		if r := recover(); r != nil {
			ret, err = nil, gerrors.New(gerrors.ErrorCode_SYSTEM_ERROR, fmt.Errorf("gremlin query result decode failed. bts: %v, err: %v", bts, r))
		}
	}()
	r := protocol.BigEndianReader{}
	r.Reset(bts, false)
	magicNumber, err := r.ReadInt16()
	if err != nil {
		return nil, err
	}
	switch magicNumber {
	// binary protocol version 1
	case BinaryV1MagicNumber:
		return structure.DecodeEx(&r, useStruct)
	case BinaryV1CompressionMagicNumber:
		unComprBytes, err := snappy.Decode(nil, r.Bytes())
		if err != nil {
			return nil, gerrors.New(gerrors.ErrorCode_SYSTEM_ERROR, fmt.Errorf("bts: %v, err: %v", bts, r))
		}
		r.Reset(unComprBytes, false)
		return structure.DecodeEx(&r, useStruct)
	default:
		//nolint:gosimple
		return nil, gerrors.New(gerrors.ErrorCode_SYSTEM_ERROR, errors.New(fmt.Sprintf("unknown protocol header magic number %x", magicNumber)))
	}
}
