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
)

type BigEndianWriter struct {
	baseWriter
}

func (w *BigEndianWriter) WriteInt16(i int16) {
	n := uint16(i)
	w.baseWriter = append(w.baseWriter, byte(n>>8), byte(n))
}

func (w *BigEndianWriter) WriteInt32(i int32) {
	n := uint32(i)
	w.baseWriter = append(w.baseWriter, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

func (w *BigEndianWriter) WriteInt64(i int64) {
	n := uint64(i)
	w.baseWriter = append(w.baseWriter, byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

func (w *BigEndianWriter) WriteFloat(f float32) {
	n := math.Float32bits(f)
	w.baseWriter = append(w.baseWriter, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

func (w *BigEndianWriter) WriteDouble(f float64) {
	n := math.Float64bits(f)
	w.baseWriter = append(w.baseWriter, byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
		byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
}

func (w *BigEndianWriter) WriteString(s string) {
	w.WriteInt32(int32(len(s)))
	w.baseWriter = append(w.baseWriter, s...)
}

func (w *BigEndianWriter) WriteBytes(b []byte) {
	w.WriteInt32(int32(len(b)))
	w.baseWriter = append(w.baseWriter, b...)
}

func (w *BigEndianWriter) WriteUvarint(x uint64) {
	for x >= 0x80 {
		w.baseWriter = append(w.baseWriter, byte(x)|0x80)
		x >>= 7
	}
	w.baseWriter = append(w.baseWriter, byte(x))
}

type BigEndianReader struct {
	baseReader
}

func (r *BigEndianReader) ReadInt16() (int16, error) {
	b, err := r.Next(2)
	if err != nil {
		return 0, err
	}
	return int16(binary.BigEndian.Uint16(b)), nil
}

func (r *BigEndianReader) ReadInt32() (int32, error) {
	b, err := r.Next(4)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b)), nil
}

func (r *BigEndianReader) ReadInt64() (int64, error) {
	b, err := r.Next(8)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func (r *BigEndianReader) ReadDouble() (float64, error) {
	b, err := r.Next(8)
	if err != nil {
		return 0, err
	}
	n := binary.BigEndian.Uint64(b)
	return math.Float64frombits(n), err
}

func (r *BigEndianReader) ReadBytes() ([]byte, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	b, err := r.Next(int(n))
	if err != nil || r.nocopy {
		return b, err
	}
	return append([]byte{}, b...), nil
}

func (r *BigEndianReader) ReadString() (string, error) {
	b, err := r.ReadBytes()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *BigEndianReader) NoCopyReadBytes() ([]byte, error) {
	n, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	b, err := r.Next(int(n))
	if err != nil {
		return nil, err
	}
	return b, nil
}
