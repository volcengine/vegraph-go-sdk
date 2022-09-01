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

package util

import (
	"reflect"
	"unsafe"
)

func UnsafeString(b []byte) (s string) {
	if len(b) == 0 {
		return ""
	}
	pBytes := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	pString := (*reflect.StringHeader)(unsafe.Pointer(&s))
	pString.Data = pBytes.Data
	pString.Len = pBytes.Len
	return
}

var BitMask = [8]byte{1, 2, 4, 8, 16, 32, 64, 128}

// RoundUp8 rounds size to the next multiple of 8.
func RoundUp8(size int) int { return (size + 7) &^ 7 }

// SetBitTo1 sets the bit at index i in buf to 1.
func SetBitTo1(buf []byte, i int) { buf[uint(i)/8] |= BitMask[byte(i)%8] }

// SetBitTo0 sets the bit at index i in buf to 0.
func SetBitTo0(buf []byte, i int) { buf[uint(i)/8] &= ^(1 << (uint(i) % 8)) }
