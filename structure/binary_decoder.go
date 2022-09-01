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
	"math"

	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph"
	"github.com/volcengine/vegraph-go-sdk/provider/protocol"
)

type VIdTypeType int8

const (
	IdTypeInt64Int32   = VIdTypeType(0) // <int64, int32>
	IdTypeStringString = VIdTypeType(3) // <string, string>
)

func decodeValue(w *protocol.BigEndianReader) (interface{}, error) {
	i8, err := w.ReadInt8()
	if err != nil {
		return nil, err
	}
	ty := CoreDataType(i8)
	switch ty {
	case TrueType:
		return true, nil
	case FalseType:
		return false, nil
	case Int32Type:
		i32, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		return i32, nil
	case Int64Type:
		i64, err := w.ReadInt64()
		if err != nil {
			return nil, err
		}
		return i64, nil
	case FloatType:
		i32, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		return math.Float32frombits(uint32(i32)), nil
	case DoubleType:
		i64, err := w.ReadInt64()
		if err != nil {
			return nil, err
		}
		return math.Float64frombits(uint64(i64)), nil
	case StringType:
		s, err := w.ReadBytes()
		if err != nil {
			return nil, err
		}
		return string(s), nil
	default:
		return nil, fmt.Errorf("value can only be basic type, rather than coreDataType(%d)", ty)
	}
}

func readVertex(w *protocol.BigEndianReader) (*Vertex, error) {
	id, err := w.ReadInt64()
	if err != nil {
		return nil, err
	}
	ptype, err := w.ReadInt32()
	if err != nil {
		return nil, err
	}
	return &Vertex{
		Id:   id,
		Type: ptype,
	}, nil
}

func readVertexWithProperties(w *protocol.BigEndianReader) (*Vertex, error) {
	vertex, err := readVertex(w)
	if err != nil {
		return nil, err
	}
	pptLen, err := w.ReadInt32()
	if err != nil {
		return nil, err
	}
	vertex.Properties = make([]*Property, 0, pptLen)
	for i := 0; i < int(pptLen); i++ {
		ppt, err := Decode(w)
		if err != nil {
			return nil, err
		}
		vertex.Properties = append(vertex.Properties, ppt.(*Property))
	}
	return vertex, nil
}

func readSVertex(w *protocol.BigEndianReader) (*Vertex, error) {
	sid, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	stype, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	return &Vertex{SId: sid, SType: stype, VType: IdTypeStringString}, nil
}

func readSVertexWithProperties(w *protocol.BigEndianReader) (*Vertex, error) {
	vertex, err := readSVertex(w)
	if err != nil {
		return nil, err
	}
	pptLen, err := w.ReadInt16()
	if err != nil {
		return nil, err
	}
	vertex.Properties = make([]*Property, pptLen)
	for i := 0; i < int(pptLen); i++ {
		ppt, err := Decode(w)
		if err != nil {
			return nil, err
		}
		vertex.Properties[i] = ppt.(*Property)
	}
	return vertex, nil
}

func readEdge(w *protocol.BigEndianReader, d DirectionType) (*Edge, error) {
	label, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	p1id, err := w.ReadInt64()
	if err != nil {
		return nil, err
	}
	p1type, err := w.ReadInt32()
	if err != nil {
		return nil, err
	}
	p2id, err := w.ReadInt64()
	if err != nil {
		return nil, err
	}
	p2type, err := w.ReadInt32()
	if err != nil {
		return nil, err
	}
	e := &Edge{
		OutV: &Vertex{Id: p1id, Type: p1type},
		InV:  &Vertex{Id: p2id, Type: p2type},
		Type: label,
	}
	if d == DirectionType_Reverse {
		e.InV, e.OutV = e.OutV, e.InV
	}
	return e, nil
}

func readEdgeWithProperties(w *protocol.BigEndianReader, d DirectionType) (*Edge, error) {
	edge, err := readEdge(w, d)
	if err != nil {
		return nil, err
	}
	pptLen, err := w.ReadInt32()
	if err != nil {
		return nil, err
	}
	edge.Properties = make([]*Property, 0, pptLen)
	for i := 0; i < int(pptLen); i++ {
		ppt, err := Decode(w)
		if err != nil {
			return nil, err
		}
		edge.Properties = append(edge.Properties, ppt.(*Property))
	}
	return edge, nil
}

func readSEdge(w *protocol.BigEndianReader, d DirectionType) (*Edge, error) {
	label, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	p1sid, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	p1stype, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	p2sid, err := w.ReadString()
	if err != nil {
		return nil, err
	}
	p2stype, err := w.ReadString()
	if err != nil {
		return nil, err
	}

	edge := &Edge{
		OutV: &Vertex{SId: p1sid, SType: p1stype, VType: IdTypeStringString},
		InV:  &Vertex{SId: p2sid, SType: p2stype, VType: IdTypeStringString},
		Type: label,
	}
	if d == DirectionType_Reverse {
		edge.InV, edge.OutV = edge.OutV, edge.InV
	}
	return edge, nil
}

func readSEdgeWithProperties(w *protocol.BigEndianReader, d DirectionType) (*Edge, error) {
	edge, err := readSEdge(w, d)
	if err != nil {
		return nil, err
	}
	pptLen, err := w.ReadInt16()
	if err != nil {
		return nil, err
	}
	edge.Properties = make([]*Property, pptLen)
	for i := 0; i < int(pptLen); i++ {
		ppt, err := Decode(w)
		if err != nil {
			return nil, err
		}
		edge.Properties[i] = ppt.(*Property)
	}
	return edge, nil
}

func Decode(w *protocol.BigEndianReader) (Element, error) {
	return DecodeEx(w, false)
}

func DecodeEx(w *protocol.BigEndianReader, useStruct bool) (Element, error) {
	i8, err := w.ReadInt8()
	if err != nil {
		return nil, err
	}
	ty := CoreDataType(i8)
	switch ty {
	case TrueType:
		return Bool(true), nil
	case FalseType:
		return Bool(false), nil
	case Int32Type:
		i32, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		return Int32(i32), nil
	case Int64Type:
		i64, err := w.ReadInt64()
		if err != nil {
			return nil, err
		}
		return Int64(i64), nil
	case FloatType:
		i32, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		return Float32(math.Float32frombits(uint32(i32))), nil
	case DoubleType:
		i64, err := w.ReadInt64()
		if err != nil {
			return nil, err
		}
		return Float64(math.Float64frombits(uint64(i64))), nil
	case StringType:
		s, err := w.ReadString()
		if err != nil {
			return nil, err
		}
		return String(s), nil
	case VertexType:
		return readVertex(w)
	case VertexWithPropertiesType:
		return readVertexWithProperties(w)
	case SVertexType:
		return readSVertex(w)
	case SVertexWithPropertiesType:
		return readSVertexWithProperties(w)
	case PathType:
		// decode labels
		lt, err := w.ReadInt8()
		if err != nil {
			return nil, err
		}
		if CoreDataType(lt) != ListType {
			return nil, fmt.Errorf("unsupported data type of path labels: coreDataType(%d)", lt)
		}
		labelsLength, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		for i := int32(0); i < labelsLength; i++ {
			// Currently we don't support labels.
			_, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
		}
		// decode objects
		objlisttype, err := w.ReadInt8()
		if err != nil {
			return nil, err
		}
		if CoreDataType(objlisttype) != ListType {
			return nil, fmt.Errorf("unsupported data type of path objects: coreDataType(%d)", objlisttype)
		}
		objectListLength, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		var p Path
		if objectListLength != 0 {
			p = make([]Element, 0, objectListLength)
		}
		for i := int32(0); i < objectListLength; i++ {
			object, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
			p = append(p, object)
		}
		if useStruct {
			return &PathStruct{Elems: p}, nil
		}
		return p, nil
	case ForwardEdgeType:
		return readEdge(w, DirectionType_Forward)
	case ReverseEdgeType:
		return readEdge(w, DirectionType_Reverse)
	case DoubleEdgeType:
		return readEdge(w, DirectionType_Double)
	case ForwardEdgeWithPropertiesType:
		return readEdgeWithProperties(w, DirectionType_Forward)
	case ReverseEdgeWithPropertiesType:
		return readEdgeWithProperties(w, DirectionType_Reverse)
	case DoubleEdgeWithPropertiesType:
		return readEdgeWithProperties(w, DirectionType_Double)
	case ForwardSEdgeType:
		return readSEdge(w, DirectionType_Forward)
	case ReverseSEdgeType:
		return readSEdge(w, DirectionType_Reverse)
	case DoubleSEdgeType:
		return readSEdge(w, DirectionType_Double)
	case ForwardSEdgeWithPropertiesType:
		return readSEdgeWithProperties(w, DirectionType_Forward)
	case ReverseSEdgeWithPropertiesType:
		return readSEdgeWithProperties(w, DirectionType_Reverse)
	case DoubleSEdgeWithPropertiesType:
		return readSEdgeWithProperties(w, DirectionType_Double)
	case ListType:
		length, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		var r List
		if length == 0 {
			return r, nil
		}
		nextType, err := w.PeekInt8()
		if err != nil {
			return nil, err
		}
		if CoreDataType(nextType) == ColumnarBinType {
			// 这里实现较为trick，因为DecodeEx返回的都是单个Element，而列式协议解析出来是多个Element，然后添加到list里。
			for i := int32(0); i < length; i++ {
				ret, err := decodeColumnarBinType(w)
				if err != nil {
					return nil, err
				}
				r = append(r, ret...)
			}
			return r, nil
		}
		if length != 0 {
			r = make([]Element, 0, length)
		}
		for i := int32(0); i < length; i++ {
			elem, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
			r = append(r, elem)
		}
		if useStruct {
			return &ListStruct{Elems: r}, nil
		}
		return r, nil
	case MapType:
		length, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		r := Map(make(map[Element]Element, length))
		for i := int32(0); i < length; i++ {
			key, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
			value, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
			r[key] = value
		}
		if useStruct {
			return &MapStruct{Elems: r}, nil
		}
		return r, nil
	case LinkedMapType:
		length, err := w.ReadInt32()
		if err != nil {
			return nil, err
		}
		linkedMap := LinkedMap{Keys: make([]Element, length), Elems: map[Element]Element{}}
		for i := int32(0); i < length; i++ {
			key, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
			value, err := DecodeEx(w, useStruct)
			if err != nil {
				return nil, err
			}
			linkedMap.Keys[i] = key
			linkedMap.Elems[key] = value
		}
		return linkedMap, nil
	case PropertyType:
		key, err := w.ReadString()
		if err != nil {
			return nil, err
		}
		value, err := decodeValue(w)
		if err != nil {
			return nil, err
		}
		return &Property{
			Key:   key,
			Value: value,
		}, nil
	default:
		return nil, fmt.Errorf("unknown coreDataType %d", ty)
	}
}

func decodeColumnarBinType(w *protocol.BigEndianReader) ([]Element, error) {
	_, err := w.ReadInt8()
	if err != nil {
		return nil, err
	}
	unmarshalType, err := w.ReadInt8()
	if err != nil {
		return nil, err
	}
	switch CoreDataType(unmarshalType) {
	case ForwardEdgeType, DoubleEdgeType, ForwardEdgeWithPropertiesType, DoubleEdgeWithPropertiesType:
		return readColumnarEdge(w, bytegraph.DirectionType_Forward)
	case ReverseEdgeType, ReverseEdgeWithPropertiesType:
		return readColumnarEdge(w, bytegraph.DirectionType_Reverse)
	case VertexType, VertexWithPropertiesType:
		return readColumnarVertex(w)
	case PropertyType:
		return readProperties(w)
	case ValueType:
		return readValues(w)
	default:
		panic(fmt.Errorf("[decodeColumnarBinType]: unknow type: %v", unmarshalType))
	}
}
