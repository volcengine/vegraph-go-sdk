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
	"reflect"
	"sort"
	"strings"

	"github.com/volcengine/vegraph-go-sdk/gerrors"
	"github.com/volcengine/vegraph-go-sdk/provider/protocol"
)

const (
	VERTEX ElementType = iota
	EDGE
	PATH
	PROPERTY
	BOOL
	INT32
	INT64
	FLOAT32
	FLOAT64
	STRING
	BYTES
	LIST
	MAP
	// These three types are not in the data type protocol with server side,
	// start from 1024 should be enough for future new server types.
	PATH_STRUCT = 1024
	LIST_STRUCT = 1025
	MAP_STRUCT  = 1026
	LINKEDMAP   = 1027
)

const (
	gremlinObjectMappingTagKey = "gremlin"

	// used by Vertex
	gremlinVertexIdTagValue   = "id"
	gremlinVertexTypeTagValue = "type"

	// used by Edge
	gremlinEdgeInVTagValue  = "inV"
	gremlinEdgeOutVTagValue = "outV"
	gremlinEdgeTypeTagValue = "type"
)

type Extra struct {
	Cost int64
}

type ElementType int64

type Element interface {
	Tp() ElementType
	// Eq and sortString methods are for internal usage, used in test case.
	Eq(Element, bool) bool
	sortString() string
	String() string
	EncodeTo(w *protocol.BigEndianWriter)
	BindTo(dest interface{}) error
}

type Bool bool

func (Bool) Tp() ElementType {
	return BOOL
}

func (b Bool) Eq(other Element, strict bool) bool {
	val, ok := other.(Bool)
	if !ok {
		return false
	}

	return b == val
}

func (b Bool) String() string {
	if b {
		return "True"
	} else {
		return "False"
	}
}

func (b Bool) sortString() string {
	return b.String()
}

func (b Bool) EncodeTo(w *protocol.BigEndianWriter) {
	if b {
		w.WriteInt8(int8(TrueType))
	} else {
		w.WriteInt8(int8(FalseType))
	}
}

func (b Bool) BindTo(dest interface{}) error {
	switch v := dest.(type) {
	case *bool:
		*v = bool(b)
	default:
		return fmt.Errorf("%w,cannot map an Bool to a %T type variable", gerrors.ErrOrmTypeMismatch, v)
	}
	return nil
}

type Int32 int32

func (Int32) Tp() ElementType {
	return INT32
}

func (i32 Int32) Eq(other Element, strict bool) bool {
	if strict {
		val, ok := other.(Int32)
		if !ok {
			return false
		}
		return i32 == val
	}
	// non-strict mode
	return i32.sortString() == other.sortString()
}

func (i32 Int32) String() string {
	return fmt.Sprintf("%d", i32)
}

func (i32 Int32) sortString() string {
	return i32.String()
}

func (i32 Int32) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(Int32Type))
	w.WriteInt32(int32(i32))
}

func (i32 Int32) BindTo(dest interface{}) error {
	switch v := dest.(type) {
	case *int32:
		*v = int32(i32)
	case *int:
		*v = int(i32)
	case *int64:
		*v = int64(i32)
	default:
		return fmt.Errorf("%w cannot map an Int32 to a %T type variable", gerrors.ErrOrmTypeMismatch, v)
	}
	return nil
}

type Int64 int64

func (Int64) Tp() ElementType {
	return INT64
}

func (i64 Int64) Eq(other Element, strict bool) bool {
	if strict {
		val, ok := other.(Int64)
		if !ok {
			return false
		}
		return i64 == val
	}
	return i64.sortString() == other.sortString()
}

func (i64 Int64) sortString() string {
	return i64.String()
}

func (i64 Int64) String() string {
	return fmt.Sprintf("%d", i64)
}

func (i64 Int64) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(Int64Type))
	w.WriteInt64(int64(i64))
}

func (i64 Int64) BindTo(dest interface{}) error {
	switch v := dest.(type) {
	case *int:
		*v = int(i64)
	case *int64:
		*v = int64(i64)
	default:
		return fmt.Errorf("%w cannot map an Int64 to a %T type variable", gerrors.ErrOrmTypeMismatch, v)
	}
	return nil
}

type Float32 float32

func (Float32) Tp() ElementType {
	return FLOAT32
}

func (f32 Float32) Eq(other Element, strict bool) bool {
	if strict {
		val, ok := other.(Float32)
		if !ok {
			return false
		}
		return f32 == val
	}
	return f32.sortString() == other.sortString()
}

func (f32 Float32) String() string {
	return fmt.Sprintf("%v", float32(f32))
}

func (f32 Float32) sortString() string {
	return f32.String()
}

func (f32 Float32) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(FloatType))
	w.WriteFloat(float32(f32))
}

func (f32 Float32) BindTo(dest interface{}) error {
	switch v := dest.(type) {
	case *float32:
		*v = float32(f32)
	case *float64:
		*v = float64(f32)
	default:
		return fmt.Errorf("%w cannot map a Float32 to a %T type variable", gerrors.ErrOrmTypeMismatch, v)
	}
	return nil
}

type Float64 float64

func (Float64) Tp() ElementType {
	return FLOAT64
}

func (f64 Float64) Eq(other Element, strict bool) bool {
	if strict {
		val, ok := other.(Float64)
		if !ok {
			return false
		}
		return f64 == val
	}
	return f64.sortString() == other.sortString()
}

func (f64 Float64) String() string {
	return fmt.Sprintf("%v", float64(f64))
}

func (f64 Float64) sortString() string {
	return f64.String()
}

func (f64 Float64) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(DoubleType))
	w.WriteDouble(float64(f64))
}

func (f64 Float64) BindTo(dest interface{}) error {
	switch v := dest.(type) {
	case *float64:
		*v = float64(f64)
	default:
		return fmt.Errorf("%w, cannot map a Float64 to a %T type variable", gerrors.ErrOrmTypeMismatch, v)
	}
	return nil
}

type String string

func (String) Tp() ElementType {
	return STRING
}

func (s String) Eq(other Element, strict bool) bool {
	val, ok := other.(String)
	if !ok {
		return false
	}
	return s == val
}

func (s String) String() string {
	return string(s)
}

func (s String) sortString() string {
	return s.String()
}

func (s String) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(StringType))
	w.WriteString(string(s))
}

func (s String) BindTo(dest interface{}) error {
	switch v := dest.(type) {
	case *string:
		*v = string(s)
	default:
		return fmt.Errorf("%w,cannot map a String to a %T type variable", gerrors.ErrOrmTypeMismatch, v)
	}
	return nil
}

type Property struct {
	Key   string
	Value interface{}
}

func (*Property) Tp() ElementType {
	return PROPERTY
}

func (p *Property) Eq(other Element, strict bool) bool {
	val, ok := other.(*Property)
	if !ok {
		return false
	}

	return p.Key == val.Key && p.Value == val.Value
}

func (p *Property) String() string {
	switch p.Value.(type) {
	case string, []byte:
		return fmt.Sprintf("Property{Key:%v, Value:%q}", p.Key, p.Value)
	default:
		return fmt.Sprintf("Property{Key:%v, Value:%v}", p.Key, p.Value)
	}
}

func (p *Property) sortString() string {
	return p.String()
}

func (p *Property) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(PropertyType))
	w.WriteString(p.Key)
	switch v := p.Value.(type) {
	case bool:
		if v {
			w.WriteInt8(int8(TrueType))
		} else {
			w.WriteInt8(int8(FalseType))
		}
	case int32:
		w.WriteInt8(int8(Int32Type))
		w.WriteInt32(v)
	case int64:
		w.WriteInt8(int8(Int64Type))
		w.WriteInt64(v)
	case float32:
		w.WriteInt8(int8(FloatType))
		w.WriteFloat(v)
	case float64:
		w.WriteInt8(int8(DoubleType))
		w.WriteDouble(v)
	case string:
		w.WriteInt8(int8(StringType))
		w.WriteBytes([]byte(v))
	default:
		panic(fmt.Sprintf("unexpected value type: %T", v))
	}
}

func (p Property) BindTo(dest interface{}) error {
	div, dit, err := getDestIndirectValueAndType(dest)
	if err != nil {
		return err
	}
	propertyT := reflect.TypeOf(p.Value)
	propertyV := reflect.ValueOf(p.Value)
	if div.Kind() == reflect.Ptr {
		if !propertyT.AssignableTo(dit.Elem()) {
			return fmt.Errorf("%w, orm type mismatch, cannot map property %s to field type %s", gerrors.ErrOrmTypeMismatch, p.Key, div.Elem().Kind().String())
		}
		div.Set(reflect.New(dit.Elem()))
		div.Elem().Set(propertyV)
		return nil
	}
	if !propertyT.AssignableTo(dit) {
		return fmt.Errorf("%w, orm type mismatch, cannot map property %s (%T) to field type %s", gerrors.ErrOrmTypeMismatch, p.Key, p.Value, div.Kind().String())
	}
	div.Set(propertyV)
	return nil
}

type Vertex struct {
	Id    int64
	Type  int32
	VType VIdTypeType
	SId   string
	SType string
	// vertex property will not be automatically retrieved with vertex
	Properties []*Property
}

func (v *Vertex) Tp() ElementType {
	return VERTEX
}

func (v *Vertex) Eq(other Element, strict bool) bool {
	val, ok := other.(*Vertex)
	if !ok {
		return false
	}

	if v.VType != val.VType {
		return false
	}
	if v.VType != IdTypeInt64Int32 && v.VType != IdTypeStringString {
		panic("not implement yet")
	}
	if (v.VType == IdTypeInt64Int32 && (v.Id != val.Id || v.Type != val.Type)) ||
		(v.VType == IdTypeStringString && (v.SId != val.SId || v.SType != val.SType)) ||
		(len(v.Properties) != len(val.Properties)) {
		return false
	}

	kv := make(map[string]interface{})
	for _, prop := range v.Properties {
		kv[prop.Key] = prop.Value
	}
	for _, prop := range val.Properties {
		v, ok := kv[prop.Key]
		if !ok || v != prop.Value {
			return false
		}
		delete(kv, prop.Key)
	}

	return true
}

// SimpleString return vertex string as vertex(id,type), eg vertex(1,2)
func (v *Vertex) SimpleString() string {
	if v.VType == IdTypeInt64Int32 {
		return fmt.Sprintf("vertex(%d, %d)", v.Id, v.Type)
	}
	return fmt.Sprintf("vertex(%q, %q)", v.SId, v.SType)
}

func (v *Vertex) String() string {
	var b strings.Builder
	if v.VType == IdTypeInt64Int32 {
		b.WriteString(fmt.Sprintf("Vertex{Id:%v, Type:%v", v.Id, v.Type))
	} else if v.VType == IdTypeStringString {
		b.WriteString(fmt.Sprintf("Vertex{SId:%s, SType:%s", v.SId, v.SType))
	} else {
		panic("not implement yet")
	}

	if len(v.Properties) > 0 {
		b.WriteString(", properties:[")
		ppts := make([]string, 0, len(v.Properties))
		for _, pp := range v.Properties {
			ppts = append(ppts, fmt.Sprintf("%v", pp))
		}
		sort.Strings(ppts)
		b.WriteString(strings.Join(ppts, ", "))
		b.WriteString("]")
	}
	b.WriteString("}")
	return b.String()
}

func (v *Vertex) sortString() string {
	return v.String()
}

func (v *Vertex) EncodeTo(w *protocol.BigEndianWriter) {
	lenProperties := len(v.Properties)
	switch v.VType {
	case IdTypeInt64Int32:
		if lenProperties == 0 {
			w.WriteInt8(int8(VertexType))
		} else {
			w.WriteInt8(int8(VertexWithPropertiesType))
		}
		w.WriteInt64(v.Id)
		w.WriteInt32(v.Type)
		if lenProperties == 0 {
			return
		}
		w.WriteInt32(int32(lenProperties))
		// in case of race condition, the length of v.ReturnProperties may possible get less or more,
		// if it changed to be less, an panic is expected, but if it changed to be more, we just ignore
		// the extra properties.
		for i := 0; i < lenProperties; i++ {
			v.Properties[i].EncodeTo(w)
		}
	case IdTypeStringString:
		if lenProperties == 0 {
			// Encoding without properties
			w.WriteInt8(int8(SVertexType))
		} else {
			// Encoding with properties
			w.WriteInt8(int8(SVertexWithPropertiesType))
		}
		w.WriteString(v.SId)
		w.WriteString(v.SType)
		if lenProperties == 0 {
			return
		}
		w.WriteInt16(int16(lenProperties))
		for i := 0; i < lenProperties; i++ {
			v.Properties[i].EncodeTo(w)
		}
	default:
		panic("not implement")
	}

}

// BindTo 暂不支持带Properties的绑定
func (v *Vertex) BindTo(dest interface{}) error {
	div, dit, err := getDestIndirectValueAndType(dest)
	if err != nil {
		return err
	}
	for i := 0; i < dit.NumField(); i++ {
		field, val := dit.Field(i), div.Field(i)
		mapTarget := field.Tag.Get(gremlinObjectMappingTagKey)
		switch mapTarget {
		case gremlinVertexIdTagValue:
			if field.Type.Kind() != reflect.Int64 {
				return fmt.Errorf("%w, cannot map Vertex Id to field %s, because it's type is not int64", gerrors.ErrOrmTypeMismatch, field.Name)
			}
			val.SetInt(v.Id)
		case gremlinVertexTypeTagValue:
			if field.Type.Kind() != reflect.Int32 {
				return fmt.Errorf("%w, cannot map Vertex type to field %s, because it's type is not int32", gerrors.ErrOrmTypeMismatch, field.Name)
			}
			val.SetInt(int64(v.Type))
		}
	}
	return nil
}

type Edge struct {
	OutV       *Vertex
	InV        *Vertex
	Type       string
	Properties []*Property
}

func (*Edge) Tp() ElementType {
	return EDGE
}

func (e *Edge) Eq(other Element, strict bool) bool {
	val, ok := other.(*Edge)
	if !ok {
		return false
	}

	if e.Type != val.Type || !e.OutV.Eq(val.OutV, strict) || !e.InV.Eq(val.InV, strict) || len(e.Properties) != len(val.Properties) {
		return false
	}

	kv := make(map[string]interface{})
	for _, prop := range e.Properties {
		kv[prop.Key] = prop.Value
	}
	for _, prop := range val.Properties {
		v, ok := kv[prop.Key]
		if !ok || v != prop.Value {
			return false
		}
		delete(kv, prop.Key)
	}

	return true
}

func (e *Edge) String() string {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("Edge{OutV:%v, InV:%v, Type:%v",
		e.OutV, e.InV, e.Type))
	if len(e.Properties) > 0 {
		b.WriteString(", properties:[")
		ppts := make([]string, 0, len(e.Properties))
		for _, pp := range e.Properties {
			ppts = append(ppts, fmt.Sprintf("%v", pp))
		}
		sort.Strings(ppts)
		b.WriteString(strings.Join(ppts, ", "))
		b.WriteString("]")
	}
	b.WriteString("}")
	return b.String()
}

func (e *Edge) sortString() string {
	return e.String()
}

func (e *Edge) EncodeTo(w *protocol.BigEndianWriter) {
	// TODO(huyingqian): Edge struct don't record direction
	lenProperties := len(e.Properties)
	if e.OutV.VType == IdTypeInt64Int32 && e.InV.VType == IdTypeInt64Int32 {
		if lenProperties == 0 {
			// encoding without properties
			w.WriteInt8(int8(ForwardEdgeType))
		} else {
			// encoding with properties
			w.WriteInt8(int8(ForwardEdgeWithPropertiesType))
		}
		w.WriteString(e.Type)
		w.WriteInt64(e.OutV.Id)
		w.WriteInt32(e.OutV.Type)
		w.WriteInt64(e.InV.Id)
		w.WriteInt32(e.InV.Type)
		if lenProperties == 0 {
			return
		}
		w.WriteInt32(int32(lenProperties))
		// in case of race condition, the length of e.ReturnProperties may possible get less or more,
		// if it changed to be less, an panic is expected, but if it changed to be more, we just ignore
		// the extra properties.
		for i := 0; i < lenProperties; i++ {
			e.Properties[i].EncodeTo(w)
		}
	} else if e.OutV.VType == IdTypeStringString && e.InV.VType == IdTypeStringString {
		if lenProperties == 0 {
			// encoding without properties
			w.WriteInt8(int8(ForwardSEdgeType))
		} else {
			// encoding with properties
			w.WriteInt8(int8(ForwardSEdgeWithPropertiesType))
		}
		w.WriteString(e.Type)
		w.WriteString(e.OutV.SId)
		w.WriteString(e.OutV.SType)
		w.WriteString(e.InV.SId)
		w.WriteString(e.InV.SType)
		if lenProperties == 0 {
			return
		}
		w.WriteInt16(int16(lenProperties))
		// in case of race condition, length of e.ReturnProperties may possible get less or more,
		// if it changed to be less, an panic is expected, but if it changed to be more, we just ignore
		// the extra properties.
		for i := 0; i < lenProperties; i++ {
			e.Properties[i].EncodeTo(w)
		}
	} else {
		panic("not implement")
	}

}

func (e *Edge) BindTo(dest interface{}) error {
	div, dit, err := getDestIndirectValueAndType(dest)
	if err != nil {
		return err
	}
	for i := 0; i < dit.NumField(); i++ {
		field, val := dit.Field(i), div.Field(i)
		mapTarget := field.Tag.Get(gremlinObjectMappingTagKey)
		switch mapTarget {
		case gremlinEdgeInVTagValue:
			if err := e.InV.BindTo(val.Addr().Interface()); err != nil {
				return fmt.Errorf("map inV of edge failed, %w", err)
			}
		case gremlinEdgeOutVTagValue:
			if err := e.OutV.BindTo(val.Addr().Interface()); err != nil {
				return fmt.Errorf("map outV of edge failed, %w", err)
			}
		case gremlinEdgeTypeTagValue:
			if field.Type.Kind() != reflect.String {
				return fmt.Errorf("%w,cannot map Edge type to field %s, because it's type is not string", gerrors.ErrOrmTypeMismatch, field.Name)
			}
			val.SetString(e.Type)
		}
	}
	return nil
}

// TODO(huyingqian): support Label
type Path []Element

func (Path) Tp() ElementType {
	return PATH
}

func (p Path) Eq(other Element, strict bool) bool {
	val, ok := other.(Path)
	if !ok {
		return false
	}

	if len(p) != len(val) {
		return false
	}

	for i, el := range p {
		if !el.Eq(val[i], strict) {
			return false
		}
	}

	return true
}

func (p Path) String() string {
	var b strings.Builder
	b.WriteString("Path[")
	for i, item := range p {
		b.WriteString(fmt.Sprintf("%v", item))
		if i != len(p)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	return b.String()
}

func (p Path) sortString() string {
	var b strings.Builder
	b.WriteString("Path[")
	for i, item := range p {
		b.WriteString(item.sortString())
		if i != len(p)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	return b.String()
}

func (p Path) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(PathType))
	// encode labels, which is a List
	// TODO(huyingqian): here we can optimize away one byte ListType
	w.WriteInt8(int8(ListType))
	// TODO(huyingqian): support labels
	w.WriteInt32(0)
	// encode items, which is a List
	// TODO(huyingqian): here we can optimize away one byte ListType
	w.WriteInt8(int8(ListType))
	w.WriteInt32(int32(len(p)))
	for _, item := range p {
		item.EncodeTo(w)
	}
}

func (p Path) BindTo(dest interface{}) error {
	panic("to be implemented")
}

type PathStruct struct {
	Elems []Element
}

func (PathStruct) Tp() ElementType {
	return PATH_STRUCT
}

func (ps PathStruct) Eq(other Element, strict bool) bool {
	var val PathStruct
	switch typ := other.(type) {
	case PathStruct:
		val = typ
	case *PathStruct:
		val = *typ

	default:
		return false
	}

	if len(ps.Elems) != len(val.Elems) {
		return false
	}

	for i, el := range ps.Elems {
		if !el.Eq(val.Elems[i], strict) {
			return false
		}
	}
	return true
}

func (ps PathStruct) String() string {
	var b strings.Builder
	b.WriteString("PathStruct{")
	b.WriteString("Elems:[")
	for i, item := range ps.Elems {
		b.WriteString(fmt.Sprintf("%v", item))
		if i != len(ps.Elems)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	b.WriteString("}")
	return b.String()
}

func (ps PathStruct) sortString() string {
	var b strings.Builder
	b.WriteString("PathStruct{")
	b.WriteString("Elems:[")
	for i, item := range ps.Elems {
		b.WriteString(item.sortString())
		if i != len(ps.Elems)-1 {
			b.WriteString(", ")
		}
	}
	b.WriteString("]")
	b.WriteString("}")
	return b.String()
}

func (ps PathStruct) EncodeTo(w *protocol.BigEndianWriter) {
	// Note: when encode, we still use PathType
	w.WriteInt8(int8(PathType))
	// encode labels, which is a List
	// TODO(huyingqian): here we can optimize away one byte ListType
	w.WriteInt8(int8(ListType))
	// TODO(huyingqian): support labels
	w.WriteInt32(0)
	// encode items, which is a List
	// TODO(huyingqian): here we can optimize away one byte ListType
	w.WriteInt8(int8(ListType))
	w.WriteInt32(int32(len(ps.Elems)))
	for _, item := range ps.Elems {
		item.EncodeTo(w)
	}
}

func (ps PathStruct) BindTo(dest interface{}) error {
	panic("to be implemented")
}

type List []Element

func (List) Tp() ElementType {
	return LIST
}

func (l List) Eq(other Element, strict bool) bool {
	val, ok := other.(List)
	if !ok {
		return false
	}

	if len(l) != len(val) {
		return false
	}
	// strict comparing
	if strict {
		for i, el := range l {
			if !el.Eq(val[i], strict) {
				return false
			}
		}
		return true
	}
	// non-strict comparing
	return l.sortString() == other.sortString()
}

// List is used as bulkset
func (l List) String() string {
	strs := make([]string, 0, len(l))
	for _, lt := range l {
		strs = append(strs, fmt.Sprintf("%v", lt))
	}
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("]")
	return b.String()
}

func (l List) sortString() string {
	strs := make([]string, 0, len(l))
	for _, lt := range l {
		strs = append(strs, lt.sortString())
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("]")
	return b.String()
}

func (l List) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(ListType))
	w.WriteInt32(int32(len(l)))
	for _, e := range l {
		e.EncodeTo(w)
	}
}

// BindTo dest is slice or struct
func (l List) BindTo(dest interface{}) error {
	if l == nil || len(l) == 0 {
		return nil
	}
	div, dit, err := getDestIndirectValueAndType(dest)
	if err != nil {
		return err
	}
	var destFieldValues map[string]reflect.Value // 用于存放struct 的 field tag 与 field value的映射
	for i, listElem := range l {
		switch le := listElem.(type) {
		case Map, List, *Vertex, *Edge, Int32, Int64, Float32, Float64, Bool, String:
			switch div.Kind() {
			case reflect.Slice:
				if div.Len() < len(l) {
					div.Set(reflect.MakeSlice(div.Type(), len(l), len(l)))
				}
				indexElem := div.Index(i)
				if !indexElem.CanAddr() {
					return fmt.Errorf("%w , type %T", gerrors.ErrOrmElemUnAddressable, indexElem.Interface())
				}
				if err := le.BindTo(indexElem.Addr().Interface()); err != nil {
					return err
				}
			case reflect.Struct:
				if len(l) > 1 {
					return fmt.Errorf("%w, cannot mapping List of %T to %T, container must be slice", gerrors.ErrOrmTypeMismatch, le, dest)
				}
				if err := le.BindTo(dest); err != nil {
					return err
				}
			}
		case *Property:
			switch div.Kind() {
			case reflect.Slice: // List<*Property> -> []struct
				if destFieldValues == nil {
					if dit.Elem().Kind() != reflect.Struct {
						return fmt.Errorf("%w, cannot mapping List of %s to %T", gerrors.ErrOrmTypeMismatch, dit.Elem().Kind().String(), dest)
					}
					destFieldValues = map[string]reflect.Value{}
					div.Set(reflect.MakeSlice(div.Type(), 1, 1))
					sliceElemV := div.Index(0)
					for i := 0; i < sliceElemV.Type().NumField(); i++ {
						destFieldValues[sliceElemV.Type().Field(i).Tag.Get(gremlinObjectMappingTagKey)] = sliceElemV.Field(i)
					}
				}
				if mappingV, ok := destFieldValues[le.Key]; ok {
					if !mappingV.CanAddr() {
						return fmt.Errorf("%w , type %T", gerrors.ErrOrmElemUnAddressable, mappingV.Interface())
					}
					if err := le.BindTo(mappingV.Addr().Interface()); err != nil {
						return err
					}
				}
			case reflect.Struct: // List<*Property> -> struct
				if destFieldValues == nil {
					destFieldValues = map[string]reflect.Value{}
					for i := 0; i < dit.NumField(); i++ {
						destFieldValues[dit.Field(i).Tag.Get(gremlinObjectMappingTagKey)] = div.Field(i)
					}
				}
				if mappingV, ok := destFieldValues[le.Key]; ok {
					if !mappingV.CanAddr() {
						return fmt.Errorf("%w , type %T", gerrors.ErrOrmElemUnAddressable, mappingV.Interface())
					}
					if err := le.BindTo(mappingV.Addr().Interface()); err != nil {
						return err
					}
				}
			default:
				return fmt.Errorf("%w, cannot mapping List to %T", gerrors.ErrOrmTypeMismatch, div.Interface())
			}
		default:
			return fmt.Errorf("%w, cannot mapping %T in List", gerrors.ErrOrmUnsupportedElemType, le)
		}
	}
	return nil
}

type ListStruct struct {
	Elems []Element
}

func (ListStruct) Tp() ElementType {
	return LIST_STRUCT
}

func (ls ListStruct) Eq(other Element, strict bool) bool {
	var val ListStruct
	switch typ := other.(type) {
	case ListStruct:
		val = typ
	case *ListStruct:
		val = *typ

	default:
		return false
	}

	if len(ls.Elems) != len(val.Elems) {
		return false
	}
	// strict comparing
	if strict {
		for i, el := range ls.Elems {
			if !el.Eq(val.Elems[i], strict) {
				return false
			}
		}
		return true
	}
	// non-strict comparing
	return ls.sortString() == other.sortString()
}

// List is used as bulkset
func (ls ListStruct) String() string {
	strs := make([]string, 0, len(ls.Elems))
	for _, lt := range ls.Elems {
		strs = append(strs, fmt.Sprintf("%v", lt))
	}
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("]")
	return b.String()
}

func (ls ListStruct) sortString() string {
	strs := make([]string, 0, len(ls.Elems))
	for _, lt := range ls.Elems {
		strs = append(strs, lt.sortString())
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("[")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("]")
	return b.String()
}

func (ls ListStruct) EncodeTo(w *protocol.BigEndianWriter) {
	// Note: when encode, we still use ListType
	w.WriteInt8(int8(ListType))
	w.WriteInt32(int32(len(ls.Elems)))
	for _, e := range ls.Elems {
		e.EncodeTo(w)
	}
}

func (ls ListStruct) BindTo(dest interface{}) error {
	panic("to be implemented")
}

type Map map[Element]Element

func (Map) Tp() ElementType {
	return MAP
}

func (m Map) Eq(other Element, strict bool) bool {
	val, ok := other.(Map)
	if !ok {
		return false
	}

	if len(m) != len(val) {
		return false
	}

	flagMap := make(map[Element]bool)
	for k := range val {
		flagMap[k] = false
	}
	for k, v := range m {
		found := false
		for kk, vv := range val {
			// if key has been used or key not equal or value not equal
			if flagMap[kk] || !k.Eq(kk, strict) || !v.Eq(vv, strict) {
				continue
			}
			found = true
			flagMap[kk] = true
		}
		if !found {
			return false
		}
	}

	return true
}

func (m Map) String() string {
	strs := make([]string, 0, len(m))
	for k, v := range m {
		strs = append(strs, fmt.Sprintf("%v: %v", k, v))
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("Map{")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("}")
	return b.String()
}

func (m Map) sortString() string {
	strs := make([]string, 0, len(m))
	for k, v := range m {
		strs = append(strs, fmt.Sprintf("%v: %v", k.sortString(), v.sortString()))
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("Map{")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("}")
	return b.String()
}

func (m Map) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(MapType))
	w.WriteInt32(int32(len(m)))
	for k, v := range m {
		k.EncodeTo(w)
		v.EncodeTo(w)
	}
}

// BindTo dest must be &struct
func (m Map) BindTo(dest interface{}) error {
	if m == nil {
		return nil
	}
	div, dit, err := getDestIndirectValueAndType(dest)
	if err != nil {
		return err
	}
	if div.Kind() != reflect.Struct {
		return fmt.Errorf("%w, Map element only support mapping to struct", gerrors.ErrOrmTypeMismatch)
	}

	var destFieldValues = make(map[string]reflect.Value)
	for i := 0; i < div.NumField(); i++ {
		f := div.Field(i)
		if f.Kind() == reflect.Ptr && f.IsNil() {
			f.Set(reflect.New(f.Type().Elem()))
		}
		destFieldValues[dit.Field(i).Tag.Get(gremlinObjectMappingTagKey)] = f
	}
	for mk, mv := range m {
		if fv, ok := destFieldValues[mk.String()]; ok {
			if fv.Kind() == reflect.Ptr {
				fv = fv.Elem()
			}
			if !fv.CanAddr() {
				return fmt.Errorf("%w, dest cannot addr, dest is %T", gerrors.ErrOrmElemUnAddressable, fv.Interface())
			}
			if err := mv.BindTo(fv.Addr().Interface()); err != nil {
				return err
			}
		}
	}
	return nil
}

type MapStruct struct {
	Elems map[Element]Element
}

func (MapStruct) Tp() ElementType {
	return MAP_STRUCT
}

func (ms MapStruct) Eq(other Element, strict bool) bool {
	var val MapStruct
	switch typ := other.(type) {
	case MapStruct:
		val = typ
	case *MapStruct:
		val = *typ

	default:
		return false
	}

	if len(ms.Elems) != len(val.Elems) {
		return false
	}

	flagMap := make(map[Element]bool)
	for k := range val.Elems {
		flagMap[k] = false
	}
	for k, v := range ms.Elems {
		found := false
		for kk, vv := range val.Elems {
			// if key has been used or key not equal or value not equal
			if flagMap[kk] || !k.Eq(kk, strict) || !v.Eq(vv, strict) {
				continue
			}
			found = true
			flagMap[kk] = true
		}
		if !found {
			return false
		}
	}

	return true
}

func (ms MapStruct) String() string {
	strs := make([]string, 0, len(ms.Elems))
	for k, v := range ms.Elems {
		strs = append(strs, fmt.Sprintf("%v: %v", k, v))
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("Map{")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("}")
	return b.String()
}

func (ms MapStruct) sortString() string {
	strs := make([]string, 0, len(ms.Elems))
	for k, v := range ms.Elems {
		strs = append(strs, fmt.Sprintf("%v: %v", k.sortString(), v.sortString()))
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("Map{")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("}")
	return b.String()
}

func (ms MapStruct) EncodeTo(w *protocol.BigEndianWriter) {
	// Note: when encode, we still use MapType
	w.WriteInt8(int8(MapType))
	w.WriteInt32(int32(len(ms.Elems)))
	for k, v := range ms.Elems {
		k.EncodeTo(w)
		v.EncodeTo(w)
	}
}

func (ms MapStruct) BindTo(dest interface{}) error {
	panic("to be implemented")
}

type LinkedMap struct {
	Elems map[Element]Element
	Keys  []Element
}

func (LinkedMap) Tp() ElementType {
	return LINKEDMAP
}

func (lm LinkedMap) Eq(other Element, strict bool) bool {
	var val LinkedMap
	switch typ := other.(type) {
	case LinkedMap:
		val = typ
	case *LinkedMap:
		val = *typ
	default:
		return false
	}

	if len(val.Keys) != len(lm.Keys) || len(val.Elems) != len(lm.Elems) {
		return false
	}

	for i, key := range lm.Keys {
		if !key.Eq(val.Keys[i], strict) {
			return false
		}
		v, ok := lm.Elems[key]
		otherV, otherOk := val.Elems[val.Keys[i]]
		if (ok && otherOk) || !(ok || otherOk) {
			if ok && !(v.Eq(otherV, strict)) {
				return false
			}
		}
	}
	return true
}

func (lm LinkedMap) String() string {
	strs := make([]string, 0, len(lm.Elems))
	for _, key := range lm.Keys {
		strs = append(strs, fmt.Sprintf("%v: %v", key, lm.Elems[key]))
	}
	var b strings.Builder
	b.WriteString("LinkedMap{")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("}")
	return b.String()
}

func (lm LinkedMap) sortString() string {
	strs := make([]string, 0, len(lm.Elems))
	for k, v := range lm.Elems {
		strs = append(strs, fmt.Sprintf("%v: %v", k.sortString(), v.sortString()))
	}
	sort.Strings(strs)
	var b strings.Builder
	b.WriteString("LinkedMap{")
	b.WriteString(strings.Join(strs, ", "))
	b.WriteString("}")
	return b.String()
}

func (lm LinkedMap) EncodeTo(w *protocol.BigEndianWriter) {
	w.WriteInt8(int8(LinkedMapType))
	w.WriteInt32(int32(len(lm.Elems)))
	for _, key := range lm.Keys {
		key.EncodeTo(w)
		lm.Elems[key].EncodeTo(w)
	}
}

func (lm LinkedMap) BindTo(dest interface{}) error {
	panic("to be implemented")
}

// getDestIndirectValueAndType
// [param] dest short for destination, dest must be pointer
// [return] div: short for dest indirect value. dit: short for dest indirect type
func getDestIndirectValueAndType(dest interface{}) (div reflect.Value, dit reflect.Type, err error) {
	if dk := reflect.TypeOf(dest).Kind(); dk != reflect.Ptr {
		return div, dit, fmt.Errorf("%w,orm object cannot be %v, must be pointer", gerrors.ErrOrmTypeMismatch, dk)
	}

	div = reflect.Indirect(reflect.ValueOf(dest)) // div: dest indirect value
	dit = div.Type()                              //dit: dest indirect type
	if div.Kind() == reflect.Ptr {                // handle pointer of pointer, such as **User{}
		if div.IsNil() {
			div.Set(reflect.New(dit.Elem()))
		}
		div = div.Elem()
		dit = div.Type()
	}
	return div, dit, nil
}
