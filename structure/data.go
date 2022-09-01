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

type CoreDataType byte

const (
	// all data type in encoding protocol
	_          CoreDataType = iota
	TrueType   CoreDataType = 1
	FalseType  CoreDataType = 2
	Int32Type  CoreDataType = 3
	Int64Type  CoreDataType = 4
	FloatType  CoreDataType = 5
	DoubleType CoreDataType = 6
	StringType CoreDataType = 7
	// TODO(huyingqian): determine how many slots should be reserved for ValueType

	VertexType CoreDataType = 8
	PathType   CoreDataType = 9

	// the relative location of these three fields should be synced with DirectionType
	ForwardEdgeType CoreDataType = 10
	ReverseEdgeType CoreDataType = 11
	DoubleEdgeType  CoreDataType = 12

	ListType     CoreDataType = 13
	MapType      CoreDataType = 14
	PropertyType CoreDataType = 15

	VertexWithPropertiesType      CoreDataType = 16
	ForwardEdgeWithPropertiesType CoreDataType = 17
	ReverseEdgeWithPropertiesType CoreDataType = 18
	DoubleEdgeWithPropertiesType  CoreDataType = 19

	SVertexType               CoreDataType = 24
	SVertexWithPropertiesType CoreDataType = 25

	ForwardSEdgeType               CoreDataType = 26
	ReverseSEdgeType               CoreDataType = 27
	DoubleSEdgeType                CoreDataType = 28
	ForwardSEdgeWithPropertiesType CoreDataType = 29
	ReverseSEdgeWithPropertiesType CoreDataType = 30
	DoubleSEdgeWithPropertiesType  CoreDataType = 31
	LinkedMapType                  CoreDataType = 32

	ColumnarBinType CoreDataType = 40
	// ValueType 用于到客户端的列式协议中，客户端识别反序列化成Property还是具体的Int32、Double等类型
	ValueType CoreDataType = 41
)

const (
	VtxIdKey         = "id"
	VtxTypeKey       = "type"
	// 下列常量只在返回给客户端列式协议时使用
	ClientProtoEdgeTypeKey       = "bg__to_client_edge_type"
	ClientProtoEdgeStartVIdKey   = "bg__to_client_start_v_id"
	ClientProtoEdgeStartVTypeKey = "bg__to_client_start_v_type"
	ClientProtoVtxIdKey          = "bg__to_client_vtx_id"   // 边终点id或点id
	ClientProtoVtxTypeKey        = "bg__to_client_vtx_type" // 边终点type或点type
)

type DirectionType int32

const (
	DirectionType_Forward DirectionType = 1
	DirectionType_Reverse DirectionType = 2
	DirectionType_Double  DirectionType = 3
)