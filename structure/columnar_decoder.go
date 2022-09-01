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

package structure

import (
	"fmt"

	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph"
	"github.com/volcengine/vegraph-go-sdk/provider/columnar"
	"github.com/volcengine/vegraph-go-sdk/provider/protocol"
)

func readColumnarEdge(w *protocol.BigEndianReader, dir bytegraph.DirectionType) ([]Element, error) {
	bs, err := w.NoCopyReadBytes()
	if err != nil {
		return nil, err
	}
	tb, err := columnar.Decode(bs)
	if err != nil {
		return nil, err
	}
	edgeCount := tb.RowCount()
	edges := make([]Edge, edgeCount)
	ret := make([]Element, 0, edgeCount)
	vertices := make([]Vertex, edgeCount*2)
	startVIdCol := tb.GetColumnPtr(ClientProtoEdgeStartVIdKey)
	startVTypeCol := tb.GetColumnPtr(ClientProtoEdgeStartVTypeKey)
	edgeTypeCol := tb.GetColumnPtr(ClientProtoEdgeTypeKey)
	endVIdCol := tb.GetColumnPtr(ClientProtoVtxIdKey)
	endVTypeCol := tb.GetColumnPtr(ClientProtoVtxTypeKey)

	for i, k := 0, 0; i < edgeCount; i++ {
		ret = append(ret, &edges[i])
		edges[i].Type = edgeTypeCol.GetString(i)
		k = i * 2
		vertices[k].Id = startVIdCol.GetInt64(i)
		vertices[k].Type = startVTypeCol.GetInt32(i)
		vertices[k+1].Id = endVIdCol.GetInt64(i)
		vertices[k+1].Type = endVTypeCol.GetInt32(i)
		if dir == bytegraph.DirectionType_Reverse {
			edges[i].InV = &vertices[k]
			edges[i].OutV = &vertices[k+1]
			continue
		}
		edges[i].InV = &vertices[k+1]
		edges[i].OutV = &vertices[k]
	}

	fieldIter := tb.Schema.FieldIter()
	for fieldIter.HasNext() {
		fieldIdx := fieldIter.Cursor()
		field := fieldIter.Next()
		switch field.Name {
		case ClientProtoEdgeStartVTypeKey, ClientProtoEdgeStartVIdKey, ClientProtoVtxIdKey, ClientProtoVtxTypeKey, ClientProtoEdgeTypeKey:
			continue
		default:
			properties := make([]Property, edgeCount)
			pCol := tb.GetColumnByIdx(fieldIdx)
			switch field.Type {
			case columnar.ValueType_STRING:
				for i := 0; i < edgeCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetString(i)
					edges[i].Properties = append(edges[i].Properties, &properties[i])
				}
			case columnar.ValueType_BOOL:
				for i := 0; i < edgeCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetBool(i)
					edges[i].Properties = append(edges[i].Properties, &properties[i])
				}
			case columnar.ValueType_FLOAT:
				for i := 0; i < edgeCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetFloat32(i)
					edges[i].Properties = append(edges[i].Properties, &properties[i])
				}
			case columnar.ValueType_DOUBLE:
				for i := 0; i < edgeCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetFloat64(i)
					edges[i].Properties = append(edges[i].Properties, &properties[i])
				}
			case columnar.ValueType_INT32:
				for i := 0; i < edgeCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetInt32(i)
					edges[i].Properties = append(edges[i].Properties, &properties[i])
				}
			case columnar.ValueType_INT64:
				for i := 0; i < edgeCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetInt64(i)
					edges[i].Properties = append(edges[i].Properties, &properties[i])
				}
			default:
				panic(fmt.Errorf("unsupport type %v", field.Type))
			}
		}
	}
	return ret, nil
}

func readColumnarVertex(w *protocol.BigEndianReader) ([]Element, error) {
	bs, err := w.NoCopyReadBytes()
	if err != nil {
		return nil, err
	}
	tb, err := columnar.Decode(bs)
	if err != nil {
		return nil, err
	}
	vCount := tb.RowCount()
	ret := make([]Element, 0, vCount)
	vertices := make([]Vertex, vCount)
	vIdCol := tb.GetColumnPtr(ClientProtoVtxIdKey)
	vTypeCol := tb.GetColumnPtr(ClientProtoVtxTypeKey)

	for i := 0; i < vCount; i++ {
		ret = append(ret, &vertices[i])
		vertices[i].Id = vIdCol.GetInt64(i)
		vertices[i].Type = vTypeCol.GetInt32(i)
	}

	fieldIter := tb.Schema.FieldIter()
	for fieldIter.HasNext() {
		fieldIdx := fieldIter.Cursor()
		field := fieldIter.Next()
		switch field.Name {
		case ClientProtoVtxIdKey, ClientProtoVtxTypeKey:
			continue
		default:
			properties := make([]Property, vCount)
			pCol := tb.GetColumnByIdx(fieldIdx)
			switch field.Type {
			case columnar.ValueType_STRING:
				for i := 0; i < vCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetString(i)
					vertices[i].Properties = append(vertices[i].Properties, &properties[i])
				}
			case columnar.ValueType_BOOL:
				for i := 0; i < vCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetBool(i)
					vertices[i].Properties = append(vertices[i].Properties, &properties[i])
				}
			case columnar.ValueType_FLOAT:
				for i := 0; i < vCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetFloat32(i)
					vertices[i].Properties = append(vertices[i].Properties, &properties[i])
				}
			case columnar.ValueType_DOUBLE:
				for i := 0; i < vCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetFloat64(i)
					vertices[i].Properties = append(vertices[i].Properties, &properties[i])
				}
			case columnar.ValueType_INT32:
				for i := 0; i < vCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetInt32(i)
					vertices[i].Properties = append(vertices[i].Properties, &properties[i])
				}
			case columnar.ValueType_INT64:
				for i := 0; i < vCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					properties[i].Key = field.Name
					properties[i].Value = pCol.GetInt64(i)
					vertices[i].Properties = append(vertices[i].Properties, &properties[i])
				}
			default:
				panic(fmt.Errorf("unsupport type %v", field.Type))
			}
		}
	}
	return ret, nil
}

func readProperties(w *protocol.BigEndianReader) ([]Element, error) {
	bs, err := w.NoCopyReadBytes()
	if err != nil {
		return nil, err
	}
	tb, err := columnar.Decode(bs)
	if err != nil {
		return nil, err
	}
	rowCount := tb.RowCount()
	propCount := rowCount * tb.FieldCount()
	ret := make([]Element, 0, propCount)
	properties := make([]Property, propCount)
	fieldIter := tb.Schema.FieldIter()
	for fieldIter.HasNext() {
		fieldIdx := fieldIter.Cursor()
		field := fieldIter.Next()
		pCol := tb.GetColumnByIdx(fieldIdx)
		switch field.Type {
		case columnar.ValueType_STRING:
			for i := 0; i < rowCount; i++ {
				if pCol.IsNull(i) {
					continue
				}
				pIdx := fieldIdx*rowCount + i
				properties[pIdx].Key = field.Name
				properties[pIdx].Value = pCol.GetString(i)
				ret = append(ret, &properties[pIdx])
			}
		case columnar.ValueType_BOOL:
			for i := 0; i < rowCount; i++ {
				if pCol.IsNull(i) {
					continue
				}
				pIdx := fieldIdx*rowCount + i
				properties[pIdx].Key = field.Name
				properties[pIdx].Value = pCol.GetBool(i)
				ret = append(ret, &properties[pIdx])
			}
		case columnar.ValueType_FLOAT:
			for i := 0; i < rowCount; i++ {
				if pCol.IsNull(i) {
					continue
				}
				pIdx := fieldIdx*rowCount + i
				properties[pIdx].Key = field.Name
				properties[pIdx].Value = pCol.GetFloat32(i)
				ret = append(ret, &properties[pIdx])
			}
		case columnar.ValueType_DOUBLE:
			for i := 0; i < rowCount; i++ {
				if pCol.IsNull(i) {
					continue
				}
				pIdx := fieldIdx*rowCount + i
				properties[pIdx].Key = field.Name
				properties[pIdx].Value = pCol.GetFloat64(i)
				ret = append(ret, &properties[pIdx])
			}
		case columnar.ValueType_INT32:
			for i := 0; i < rowCount; i++ {
				if pCol.IsNull(i) {
					continue
				}
				pIdx := fieldIdx*rowCount + i
				properties[pIdx].Key = field.Name
				properties[pIdx].Value = pCol.GetInt32(i)
				ret = append(ret, &properties[pIdx])
			}
		case columnar.ValueType_INT64:
			for i := 0; i < rowCount; i++ {
				if pCol.IsNull(i) {
					continue
				}
				pIdx := fieldIdx*rowCount + i
				properties[pIdx].Key = field.Name
				properties[pIdx].Value = pCol.GetInt64(i)
				ret = append(ret, &properties[pIdx])
			}
		default:
			panic(fmt.Errorf("unsupport type %v", field.Type))
		}
	}
	return ret, nil
}

func readValues(w *protocol.BigEndianReader) ([]Element, error) {
	bs, err := w.NoCopyReadBytes()
	if err != nil {
		return nil, err
	}
	tb, err := columnar.Decode(bs)
	if err != nil {
		return nil, err
	}
	rowCount := tb.RowCount()
	ret := make([]Element, 0, rowCount*tb.FieldCount())
	fieldIter := tb.Schema.FieldIter()
	for fieldIter.HasNext() {
		fieldIdx := fieldIter.Cursor()
		field := fieldIter.Next()
		switch field.Name {
		case VtxIdKey, VtxTypeKey:
			continue
		default:
			pCol := tb.GetColumnByIdx(fieldIdx)
			switch field.Type {
			case columnar.ValueType_STRING:
				for i := 0; i < rowCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					ret = append(ret, String(pCol.GetString(i)))
				}
			case columnar.ValueType_BOOL:
				for i := 0; i < rowCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					ret = append(ret, Bool(pCol.GetBool(i)))
				}
			case columnar.ValueType_FLOAT:
				for i := 0; i < rowCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					ret = append(ret, Float32(pCol.GetFloat32(i)))
				}
			case columnar.ValueType_DOUBLE:
				for i := 0; i < rowCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					ret = append(ret, Float64(pCol.GetFloat64(i)))
				}
			case columnar.ValueType_INT32:
				for i := 0; i < rowCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					ret = append(ret, Int32(pCol.GetInt32(i)))
				}
			case columnar.ValueType_INT64:
				for i := 0; i < rowCount; i++ {
					if pCol.IsNull(i) {
						continue
					}
					ret = append(ret, Int64(pCol.GetInt64(i)))
				}
			default:
				panic(fmt.Errorf("unsupport type %v", field.Type))
			}
		}
	}
	return ret, nil
}
