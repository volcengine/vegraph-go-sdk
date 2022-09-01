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

package columnar

import (
	"fmt"
	"unsafe"

	"github.com/volcengine/vegraph-go-sdk/provider/util"
)

var (
	errFieldConflict = fmt.Errorf("field conflict")
	errInvalidIndex  = fmt.Errorf("invalid index")
)

var valueSize = map[ValueType]int{
	ValueType_BOOL:   1,
	ValueType_INT32:  4,
	ValueType_FLOAT:  4,
	ValueType_INT64:  8,
	ValueType_DOUBLE: 8,
}

const (
	sizeInt64   = int(unsafe.Sizeof(int64(0)))
	sizeInt32   = int(unsafe.Sizeof(int32(0)))
	sizeFloat32 = int(unsafe.Sizeof(float32(0)))
	sizeFloat64 = int(unsafe.Sizeof(float64(0)))
	sizeBool    = int(unsafe.Sizeof(false))
)

const InlinableFieldsArrayLen = 12

type ValueType int32

const (
	ValueType_UNKNOWN ValueType = 0
	ValueType_BOOL    ValueType = 1
	ValueType_INT32   ValueType = 2
	ValueType_INT64   ValueType = 3
	ValueType_FLOAT   ValueType = 4
	ValueType_DOUBLE  ValueType = 5
	ValueType_STRING  ValueType = 6
)

type Meta struct {
	Length int32
	Key    int32
	Value  []byte
}

type Field struct {
	Name  string
	Type  ValueType
	Metas []Meta
}

type InlinableFields struct {
	len   int
	array [InlinableFieldsArrayLen]Field
	slice []Field
}

func (mf *InlinableFields) FieldIdx(name string) (int, bool) {
	if mf.len < InlinableFieldsArrayLen {
		for i := 0; i < mf.len; i++ {
			if mf.array[i].Name == name {
				return i, true
			}
		}
	} else {
		for i := 0; i < InlinableFieldsArrayLen; i++ {
			if mf.array[i].Name == name {
				return i, true
			}
		}
		for i := 0; i < len(mf.slice); i++ {
			if mf.slice[i].Name == name {
				return i + InlinableFieldsArrayLen, true
			}
		}
	}
	return 0, false
}

func (mf *InlinableColumns) Column(idx int) Column {
	if idx < InlinableFieldsArrayLen {
		return mf.array[idx]
	}
	return mf.slice[idx-InlinableFieldsArrayLen]
}

func (mf *InlinableFields) Append(field Field) {
	if mf.len < InlinableFieldsArrayLen {
		mf.array[mf.len] = field
	} else {
		mf.slice = append(mf.slice, field)
	}
	mf.len++
}

func (mf *InlinableFields) Field(idx int) Field {
	if idx >= mf.len {
		panic(errInvalidIndex)
	}
	if idx < InlinableFieldsArrayLen {
		return mf.array[idx]
	}
	return mf.slice[idx-InlinableFieldsArrayLen]
}

func (mf *InlinableFields) FieldIter() FieldIter {
	return FieldIter{fields: mf}
}

type FieldIter struct {
	fields *InlinableFields
	p      int
}

func (iter *FieldIter) HasNext() bool {
	return iter.p < iter.fields.len
}

func (iter *FieldIter) Next() (f Field) {
	if iter.p >= iter.fields.len {
		return
	}
	if iter.p < InlinableFieldsArrayLen {
		f = iter.fields.array[iter.p]
	} else {
		f = iter.fields.slice[iter.p-InlinableFieldsArrayLen]
	}
	iter.p++
	return
}

func (iter *FieldIter) Cursor() int {
	return iter.p
}

type Schema struct {
	fields InlinableFields
	metas  []Meta
}

func (s *Schema) FieldIdx(name string) (int, bool) {
	return s.fields.FieldIdx(name)
}

func (s *Schema) FieldIter() FieldIter {
	return s.fields.FieldIter()
}

type nullBitmap struct {
	length int
	bytes  []byte
}

type Column struct {
	length int
	// nullBitmap len(nullBitmap)%8==0
	nullBitmap nullBitmap
	offsets    []int32
	data       []byte
	// slotSize Column 中单个元素的长度。如果是变长类型如string,slotSize==0
	slotSize int
}

func (c *Column) GetString(row int) string {
	return util.UnsafeString(c.data[c.offsets[row]:c.offsets[row+1]])
}

func (c *Column) GetInt32(row int) int32 {
	return *(*int32)(unsafe.Pointer(&c.data[row*sizeInt32]))
}

func (c *Column) GetInt64(row int) int64 {
	return *(*int64)(unsafe.Pointer(&c.data[row*sizeInt64]))
}

func (c *Column) GetBool(row int) bool {
	return *(*bool)(unsafe.Pointer(&c.data[row]))
}

func (c *Column) GetFloat32(row int) float32 {
	return *(*float32)(unsafe.Pointer(&c.data[row*sizeFloat32]))
}

func (c *Column) GetFloat64(row int) float64 {
	return *(*float64)(unsafe.Pointer(&c.data[row*sizeFloat64]))
}

func (c *Column) IsNull(row int) bool {
	nullByte := c.nullBitmap.bytes[row/8]
	return nullByte&(1<<(uint(row)&7)) == 0
}

type InlinableColumns struct {
	len   int
	array [InlinableFieldsArrayLen]Column
	slice []Column
}

func (ic *InlinableColumns) ColumnPtr(idx int) *Column {
	if idx >= ic.len {
		panic(errInvalidIndex)
	}
	if idx < InlinableFieldsArrayLen {
		return &ic.array[idx]
	}
	return &ic.slice[idx-InlinableFieldsArrayLen]
}

type Table struct {
	Schema  Schema
	Columns InlinableColumns
}

func (t *Table) RowCount() int {
	if t.Columns.len == 0 {
		return 0
	}
	// 目前Table 内所有的Column等长
	return t.Columns.array[0].length
}

func (t *Table) GetColumnPtr(name string) *Column {
	if idx, ok := t.Schema.fields.FieldIdx(name); ok {
		return t.Columns.ColumnPtr(idx)
	}
	return nil
}

func (t *Table) FieldCount() int {
	return t.Schema.fields.len
}

func (t *Table) GetColumnByIdx(idx int) Column {
	return t.Columns.Column(idx)
}

func ResizeInlinableColumns(table *Table, size int) {
	table.Columns.len = size
	if size < InlinableFieldsArrayLen {
		return
	}
	sliceSize := size - InlinableFieldsArrayLen
	if cap(table.Columns.slice) >= sliceSize {
		table.Columns.slice = table.Columns.slice[:sliceSize]
		return
	}
	newSlice := make([]Column, sliceSize)
	copy(newSlice, table.Columns.slice)
	table.Columns.slice = newSlice
}
