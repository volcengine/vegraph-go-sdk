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
	"encoding/binary"
	"math"
	"sync"

	"github.com/volcengine/vegraph-go-sdk/provider/util"
)

// LittleEndianReader 使用小端序decode
type LittleEndianReader struct {
	baseReader
}

var littleEndianReaderPool = sync.Pool{
	New: func() interface{} {
		return &LittleEndianReader{}
	},
}

func GetLittleEndianReader(bs []byte, nocopy bool) *LittleEndianReader {
	r := littleEndianReaderPool.Get().(*LittleEndianReader)
	r.b = bs
	r.nocopy = nocopy
	return r
}

func PutLittleEndianReader(r *LittleEndianReader) {
	r.p = 0
	r.b = nil
	r.nocopy = false
	littleEndianReaderPool.Put(r)
}

func (r *LittleEndianReader) ReadUint16() (uint16, error) {
	b, err := r.Next(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b), nil
}

func (r *LittleEndianReader) ReadInt16() (int16, error) {
	b, err := r.Next(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.LittleEndian.Uint16(b)), nil
}

func (r *LittleEndianReader) ReadInt32() (int32, error) {
	b, err := r.Next(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.LittleEndian.Uint32(b)), nil
}

func (r *LittleEndianReader) ReadInt64() (int64, error) {
	b, err := r.Next(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(b)), nil
}

func (r *LittleEndianReader) ReadDouble() (float64, error) {
	b, err := r.Next(8)
	if err != nil {
		return 0, err
	}
	n := binary.LittleEndian.Uint64(b)
	return math.Float64frombits(n), err
}

func (r *LittleEndianReader) ReadFloat32() (float32, error) {
	b, err := r.Next(4)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(binary.LittleEndian.Uint32(b)), nil
}

func (r *LittleEndianReader) ReadBytes() ([]byte, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	b, err := r.Next(int(n))
	if err != nil || r.nocopy {
		return b, err
	}
	bs := make([]byte, 0, n)
	return append(bs, b...), nil
}

func (r *LittleEndianReader) ReadString() (string, error) {
	b, err := r.ReadBytes()
	if err != nil {
		return "", err
	}
	if r.nocopy {
		return util.UnsafeString(b), nil
	}
	return string(b), nil
}

func (r *LittleEndianReader) SkipBytes() error {
	n, err := r.ReadInt32()
	if err != nil {
		return err
	}
	_, err = r.Next(int(n))
	return err
}
