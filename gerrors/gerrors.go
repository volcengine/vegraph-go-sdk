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

package gerrors

import (
	"fmt"
)

type GremlinError interface {
	error               // 实现error方法
	ErrCode() ErrorCode // 错误码
	ErrCause() error    // 错误描述
}

type gremlinError struct {
	ErrorCode
	cause error
}

func New(errcode ErrorCode, cause ...error) GremlinError {
	var err error
	if len(cause) > 0 {
		err = cause[0]
	}
	return &gremlinError{
		ErrorCode: errcode,
		cause:     err,
	}
}

func (e *gremlinError) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("ErrorCode: %d, ErrorDesc: %s, ErrorCause: %s", e.ErrorCode, e.ErrorCode, e.cause.Error())
	} else {
		return fmt.Sprintf("ErrorCode: %d, ErrorDesc: %s", e.ErrorCode, e.ErrorCode)
	}

}

func (e *gremlinError) ErrCode() ErrorCode {
	return e.ErrorCode
}

func (e *gremlinError) ErrCause() error {
	return e.cause
}

func DuplicateErr(err error, times int) []error {
	errs := make([]error, times)
	for i := 0; i < times; i++ {
		errs[i] = err
	}
	return errs
}
