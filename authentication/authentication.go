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

package authentication

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"time"

	json "github.com/json-iterator/go"

	"github.com/volcengine/vegraph-go-sdk/gerrors"
)

const (
	ExchangePattern = "/exchange"

	SessionKey  = "session"
	PasswordKey = "pwd"
	UserNameKey = "user"

	UserExtra         = "user_extra"
	PersistSessionKey = "RPC_PERSIST_session"
	PersistPwdKey     = "RPC_PERSIST_pwd"
	PersistUserKey    = "RPC_PERSIST_user"

	defaultAuthHostPort = "localhost:6287"
)

type IClient interface {
	UserName() string
	Password() string
	Session(bool) (string, error)
}

type BaseResp struct {
	StatusMessage string            `json:"StatusMessage"`
	StatusCode    int32             `json:"StatusCode"`
	Extra         map[string]string `json:"Extra"`
}

var (
	ErrInvalidSession = errors.New("invalid session")
	ErrNotExist       = errors.New("the session is not found")
)

func NotExistErr(err string) bool {
	return strings.Contains(err, ErrInvalidSession.Error()) || strings.Contains(err, ErrNotExist.Error())
}

type exchangeRequest struct {
	User      string `json:"user"`
	PwdSha256 string `json:"pwd"`
}
type exchangeResponse struct {
	SessionID string    `json:"session_id"` // JWT token
	Base      *BaseResp `json:"base"`
}

// Option .
type Option func(*Options)

type Options struct {
	hostsPorts []string

	username       string
	passwordSha256 string
}

func WithUserPwdSha256(user, pwdSha256 string) Option {
	return func(op *Options) {
		op.username = user
		op.passwordSha256 = pwdSha256
	}
}

func WithHostPorts(hostsPorts []string) Option {
	return func(op *Options) {
		op.hostsPorts = hostsPorts
	}
}

type Client struct {
	cli        *http.Client
	hostsPorts []string

	sessionID      string
	username       string
	passwordSha256 string
	mu             sync.RWMutex
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
func newDefaultOptions() *Options {
	return &Options{
		hostsPorts:     []string{defaultAuthHostPort},
		username:       "",
		passwordSha256: "",
	}
}
func NewClient(ops ...Option) IClient {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // ignore_security_alert
	}

	opts := newDefaultOptions()
	for _, do := range ops {
		do(opts)
	}
	return &Client{
		cli:            &http.Client{Transport: tr},
		hostsPorts:     opts.hostsPorts,
		username:       opts.username,
		passwordSha256: opts.passwordSha256,
		mu:             sync.RWMutex{},
	}
}

func (client *Client) UserName() string {
	client.mu.RLock()
	defer client.mu.RUnlock()

	return client.username
}

func (client *Client) Password() string {
	client.mu.RLock()
	defer client.mu.RUnlock()

	return client.passwordSha256
}

func (client *Client) sessionInner() string {
	client.mu.RLock()
	defer client.mu.RUnlock()

	return client.sessionID
}

func (client *Client) Session(refresh bool) (string, error) {
	var err error
	var sessionID string
	if !refresh {
		sessionID = client.sessionInner()
	}
	if len(sessionID) == 0 {
		client.mu.Lock()
		defer client.mu.Unlock()

		err = client.refreshSession()
		return client.sessionID, err
	}

	return sessionID, err
}

func (client *Client) refreshSession() error {
	resp, err := client.exchange(&exchangeRequest{
		User:      client.username,
		PwdSha256: client.passwordSha256,
	})
	if err != nil {
		return err
	}
	if resp.Base != nil && resp.Base.StatusCode != 0 {
		return gerrors.New(gerrors.ErrorCode_AUTH_FAILED, fmt.Errorf("exchange failed(errcode: %v): %v", resp.Base.StatusCode, resp.Base.StatusMessage))
	}
	client.sessionID = resp.SessionID
	return nil
}

func (client *Client) patternAddr(pattern string) string {
	idx := rand.Intn(len(client.hostsPorts))
	return fmt.Sprintf("https://%v%v", client.hostsPorts[idx], pattern)
}

func (client *Client) post(pattern string, buffer *bytes.Buffer) ([]byte, error) {
	resp, err := client.cli.Post(client.patternAddr(pattern), "application/json", buffer) // ignore_security_alert
	if err != nil {
		return nil, gerrors.New(gerrors.ErrorCode_AUTH_FAILED, fmt.Errorf("post error: %v", err))
	}
	defer resp.Body.Close()

	var body []byte
	body, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, gerrors.New(gerrors.ErrorCode_AUTH_FAILED, fmt.Errorf("read body error: %v", err))
	}
	return body, nil
}

func (client *Client) exchange(req *exchangeRequest) (*exchangeResponse, error) {
	postBody, err := json.Marshal(req)
	if err != nil {
		return nil, gerrors.New(gerrors.ErrorCode_AUTH_FAILED, fmt.Errorf("exchange marshal failed: %v", err))
	}

	var respBody []byte
	respBody, err = client.post(ExchangePattern, bytes.NewBuffer(postBody))
	if err != nil {
		return nil, err
	}
	resp := &exchangeResponse{}
	err = json.Unmarshal(respBody, &resp)
	if err != nil {
		return nil, gerrors.New(gerrors.ErrorCode_AUTH_FAILED, fmt.Errorf("umarshal exchange response body(%v) failed: %v", string(respBody), err))
	}
	return resp, nil
}
