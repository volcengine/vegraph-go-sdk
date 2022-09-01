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

package protocol

import (
	"github.com/pkg/errors"
	"github.com/volcengine/vegraph-go-sdk/gerrors"
)

type baseWriter []byte

func (w *baseWriter) WriteInt8(i int8) {
	*w = append(*w, uint8(i))
}

func (w *baseWriter) Reset() {
	*w = (*w)[:0]
}

func (w *baseWriter) ResetBytes(bs []byte) {
	*w = bs
}

func (w *baseWriter) Bytes() []byte {
	return *w
}

func (w *baseWriter) Len() int {
	return len(*w)
}

func (w *baseWriter) Write(b []byte) (int, error) {
	*w = append(*w, b...)
	return len(b), nil
}

func (w *baseWriter) WriteRawString(s string) {
	*w = append(*w, s...)
}

func (w *baseWriter) WriteBool(v bool) {
	if v {
		*w = append(*w, 1)
	} else {
		*w = append(*w, 0)
	}
}

type baseReader struct {
	p int
	b []byte

	nocopy bool
}

func (r *baseReader) Reset(b []byte, nocopy bool) {
	r.p = 0
	r.b = b
	r.nocopy = nocopy
}

func (r *baseReader) Next(n int) ([]byte, error) {
	if n < 0 {
		return nil, gerrors.ErrNegativeInt
	}
	b := r.b[r.p:]
	if len(b) < n {
		return nil, errors.Wrapf(gerrors.ErrUnexpectedEOB, "bytes are:%v", r.b)
	}
	r.p += n
	return b[:n:n], nil
}

func (r *baseReader) Bytes() []byte {
	return r.b[r.p:]
}

func (r *baseReader) ReadBool() (bool, error) {
	n, err := r.ReadInt8()
	return n > 0, err
}

func (r *baseReader) ReadInt8() (int8, error) {
	b, err := r.Next(1)
	if err != nil {
		return 0, err
	}
	return int8(b[0]), nil
}

// PeekInt8 不会更改reader的游标
func (r *baseReader) PeekInt8() (int8, error) {
	return int8(r.b[r.p]), nil
}

func (r *baseReader) Cursor() int {
	return r.p
}
