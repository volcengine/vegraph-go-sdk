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
	"reflect"
	"sync"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/volcengine/vegraph-go-sdk/provider/protocol"
	"github.com/volcengine/vegraph-go-sdk/provider/util"
)

var (
	errUnknownProtocolVersion = fmt.Errorf("unknown protocol version")
	errUnknownProtocol        = fmt.Errorf("unknown protocol")
	errUnexpectRowCount       = fmt.Errorf("unexpect row count")
	errUnexpectFieldLen       = fmt.Errorf("unexpect field length")
	errUnsupportedParseMeta   = fmt.Errorf("unsupported parse meta")
)

const ProtoColumnarMagicNumberUint16 int16 = 17985

type Protocol int32

const (
	Protocol_Binary     Protocol = 0
	Protocol_ColumnarV1 Protocol = 1
)

// init 列式协议仅适用于小端系统
func init() {
	var i int32 = 0x01020304
	b := (*byte)(unsafe.Pointer(&i))
	if *b != 0x04 {
		panic("columnar protocol cannot work on big-endian system")
	}
}

var tablePool = sync.Pool{New: func() interface{} {
	return &Table{}
}}

func GetTable() *Table {
	return tablePool.Get().(*Table)
}

func Decode(bs []byte) (table *Table, err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "bytes are:%v", bs)
		}
	}()
	r := protocol.GetLittleEndianReader(bs, true)
	defer protocol.PutLittleEndianReader(r)
	table = GetTable()
	if err = decode(r, table); err != nil {
		return nil, err
	}
	return table, nil
}

func decode(r *protocol.LittleEndianReader, table *Table) (err error) {
	//************* parse header ***************
	// magic number
	magicNumber, err := r.ReadUint16()
	if err != nil {
		return err
	}
	if magicNumber != uint16(ProtoColumnarMagicNumberUint16) {
		return errors.WithStack(errUnknownProtocol)
	}
	// proto version
	vsn, err := r.ReadUint16()
	if err != nil {
		return err
	}
	if vsn != uint16(Protocol_ColumnarV1) {
		return errors.WithStack(errUnknownProtocolVersion)
	}
	// parse row count
	tmpRowCount, err := r.ReadInt32()
	if err != nil {
		return err
	}
	rowCount := int(tmpRowCount)
	if rowCount == 0 {
		return nil
	}
	if rowCount < 0 {
		return errors.WithStack(errUnexpectRowCount)
	}
	//************* parse schema ***************
	// field length
	fieldLen, err := r.ReadInt32()
	if err != nil {
		return err
	}
	if fieldLen <= 0 {
		return errors.WithStack(errUnexpectFieldLen)
	}
	// originLen: 原本的table列数
	originLen := table.Schema.fields.len
	ResizeInlinableColumns(table, int(fieldLen)+originLen)

	var newField Field
	for i := int32(0); i < fieldLen; i++ {
		fieldType, err := r.ReadInt32()
		if err != nil {
			return err
		}
		newField.Type = ValueType(fieldType)
		newField.Name, err = r.ReadString()
		if err != nil {
			return err
		}
		if _, ok := table.Schema.FieldIdx(newField.Name); ok {
			return errors.WithStack(errFieldConflict)
		}
		table.Schema.fields.Append(newField)

		fieldMetaLen, err := r.ReadInt32()
		if err != nil {
			return err
		}
		if fieldMetaLen != 0 {
			return errors.WithStack(errUnsupportedParseMeta)
		}
		//TODO(lmj): parse field meta
	}

	schemaMetaLen, err := r.ReadInt32()
	if err != nil {
		return err
	}
	if schemaMetaLen != 0 {
		return errors.WithStack(errUnsupportedParseMeta)
	}
	// TODO(lmj): parse Schema meta

	// parse padding
	if _, err = r.Next(util.RoundUp8(r.Cursor()) - r.Cursor()); err != nil {
		return err
	}

	//************* parse data ***************
	for i := originLen; i < int(fieldLen)+originLen; i++ {
		col := table.Columns.ColumnPtr(i)
		col.length = rowCount
		// parse null bytes. with padding
		col.nullBitmap.bytes, err = r.Next(util.RoundUp8(int(uint32(rowCount+7) / 8)))
		col.nullBitmap.length = rowCount
		if err != nil {
			return err
		}

		if slotSize, isFixedSize := valueSize[table.Schema.fields.Field(i).Type]; isFixedSize {
			// 定长类型
			// parse data
			col.data, err = r.Next(util.RoundUp8(slotSize * rowCount))
			if err != nil {
				return err
			}
			// trim padding
			col.data = col.data[: slotSize*rowCount : slotSize*rowCount]
			col.slotSize = slotSize
		} else {
			// 变长类型
			// parse offsets
			// offsets 是int32长度(4字节），个数为rowCount+1。所以总长度是 (rowCount+1)*4。最后还需要加上padding
			offsetBytes, err := r.Next(util.RoundUp8((rowCount + 1) << 2))
			if err != nil {
				return err
			}
			sh := (*reflect.SliceHeader)(unsafe.Pointer(&col.offsets))
			sh.Data = (*reflect.SliceHeader)(unsafe.Pointer(&offsetBytes)).Data
			// 对 padding 截断
			sh.Len = rowCount + 1
			sh.Cap = rowCount + 1

			// parse data
			col.data, err = r.Next(util.RoundUp8(int(col.offsets[rowCount])))
			if err != nil {
				return err
			}
			col.slotSize = 0
		}
	}
	return nil
}
