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
	"errors"
	"strings"

	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph"
)

var (
	ErrOrmTypeMismatch        = errors.New("orm container type error")
	ErrOrmElemUnAddressable   = errors.New("orm container element is not addressable")
	ErrOrmUnsupportedElemType = errors.New("response element not support orm")
)

var (
	ErrNegativeInt   = errors.New("unexpected negative int")
	ErrUnexpectedEOB = errors.New("unexpected end of buffer")
)

type ErrorCode int32

// DefaultRetryErrorCodes 常见需要重试的一些错误
var DefaultRetryErrorCodes = []ErrorCode{
	ErrorCode_RETRY,
	ErrorCode_NETWORK_ERROR,
}

const (
	// inherited from ByteGraph
	ErrorCode_SUCCESS                 = ErrorCode(bytegraph.ErrorCode_SUCCESS)
	ErrorCode_SYSTEM_ERROR            = ErrorCode(bytegraph.ErrorCode_SYSTEM_ERROR)
	ErrorCode_UNKNOWN_ERROR           = ErrorCode(bytegraph.ErrorCode_UNKNOWN_ERROR)
	ErrorCode_POINT_NOT_EXIST         = ErrorCode(bytegraph.ErrorCode_POINT_NOT_EXIST)
	ErrorCode_EDGE_NOT_EXIST          = ErrorCode(bytegraph.ErrorCode_EDGE_NOT_EXIST)
	ErrorCode_RETRY                   = ErrorCode(bytegraph.ErrorCode_RETRY)
	ErrorCode_TABLE_NOT_EXIST         = ErrorCode(bytegraph.ErrorCode_TABLE_NOT_EXIST)
	ErrorCode_INVALID_REQUEST         = ErrorCode(bytegraph.ErrorCode_INVALID_REQUEST)
	ErrorCode_EDGE_ALREADY_EXIST      = ErrorCode(bytegraph.ErrorCode_EDGE_ALREADY_EXIST)
	ErrorCode_NOT_IMPLEMENTED         = ErrorCode(bytegraph.ErrorCode_NOT_IMPLEMENTED)
	ErrorCode_IO_TIMEOUT              = ErrorCode(bytegraph.ErrorCode_IO_TIMEOUT)
	ErrorCode_UDF_NOT_FOUND           = ErrorCode(bytegraph.ErrorCode_UDF_NOT_FOUND)
	ErrorCode_INDEX_OUT_OF_RANGE      = ErrorCode(bytegraph.ErrorCode_INDEX_OUT_OF_RANGE)
	ErrorCode_SERVICE_OVERLOAD        = ErrorCode(bytegraph.ErrorCode_SERVICE_OVERLOAD)
	ErrorCode_EDGE_OVER_QUOTA         = ErrorCode(bytegraph.ErrorCode_EDGE_OVER_QUOTA)
	ErrorCode_PART_OVER_QUOTA         = ErrorCode(bytegraph.ErrorCode_PART_OVER_QUOTA)
	ErrorCode_SLAVE_WRITE_NOT_ALLOWED = ErrorCode(bytegraph.ErrorCode_SLAVE_WRITE_NOT_ALLOWED)
	ErrorCode_COMMIT_FAILED           = ErrorCode(bytegraph.ErrorCode_COMMIT_FAILED)
	ErrorCode_KEY_IN_BLACKLIST        = ErrorCode(bytegraph.ErrorCode_KEY_IN_BLACKLIST)
	ErrorCode_PSM_OVER_QUOTA          = ErrorCode(bytegraph.ErrorCode_PSM_OVER_QUOTA)
	ErrorCode_PROPERTY_VALUE_INVALID  = ErrorCode(bytegraph.ErrorCode_PROPERTY_VALUE_INVALID)
	ErrorCode_PROPERTY_NOT_FOUND      = ErrorCode(bytegraph.ErrorCode_PROPERTY_NOT_FOUND)
	ErrorCode_GREMLIN_INVALID_QUERY   = ErrorCode(bytegraph.ErrorCode_GREMLIN_INVALID_QUERY)
	ErrorCode_ELEM_NOT_EXIST          = ErrorCode(bytegraph.ErrorCode_ELEM_NOT_EXIST)
	ErrorCode_WRITE_STALL             = ErrorCode(bytegraph.ErrorCode_WRITE_STALL)
	ErrorCode_TXN_CONFLICT            = ErrorCode(bytegraph.ErrorCode_TXN_CONFLICT)
	ErrorCode_NOT_SETED               = ErrorCode(bytegraph.ErrorCode_NOT_SETED)
	ErrorCode_AUTH_FAILED             = ErrorCode(bytegraph.ErrorCode_AUTH_FAILED)

	// error code for all rpc framework error
	ErrorCode_NETWORK_ERROR ErrorCode = 254
)

func (p ErrorCode) String() string {
	if p == ErrorCode_NETWORK_ERROR {
		return "NETWORK_ERROR"
	}
	return strings.TrimPrefix(bytegraph.ErrorCode(p).String(), "ErrorCode_")
}
