package structure

import (
	"math"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/volcengine/vegraph-go-sdk/kitex_gen/bytegraph"
	"github.com/volcengine/vegraph-go-sdk/provider/protocol"
)

var (
	props = []*Property{
		{"abc", int32(1024)},
		{"int64", int64(1234567890)},
		{"bool", true},
		{"float32", 123.456},
		{"double", float64(123.4567890)},
		{"string", "abcdefghijklmnopqrstuvwxyz"},
	}
)

func testElementEncode(t *testing.T, e Element) {
	w := &protocol.BigEndianWriter{}
	e.EncodeTo(w)

	r := &protocol.BigEndianReader{}
	r.Reset(w.Bytes(), false)
	got, err := Decode(r)
	assert.NoError(t, err)
	assert.Truef(t, reflect.DeepEqual(e, got), "expected %+v got %+v", e, got)
}

func TestInt64Encode(t *testing.T) {
	tests := []Element{
		Int64(0),
		Int64(1),
		Int64(math.MinInt64),
		Int64(math.MaxInt64),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func TestInt32Encode(t *testing.T) {
	tests := []Element{
		Int32(0),
		Int32(-1231),
		Int32(math.MinInt32),
		Int32(math.MaxInt32),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func TestFloat64Encode(t *testing.T) {
	tests := []Element{
		Float64(float64(0)),
		Float64(float64(1)),
		Float64(float64(100.25)),
		Float64(float64(300.5)),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func TestStringEncode(t *testing.T) {
	tests := []Element{
		String("123"),
		String(""),
		String("awdawefcdzxcv"),
		String([]byte{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6}),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func TestBoolEncode(t *testing.T) {
	tests := []Element{
		Bool(true),
		Bool(false),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func TestVertexEncode(t *testing.T) {
	tests := []Element{
		&Vertex{Id: 0, Type: 0},
		&Vertex{Id: 123, Type: 456},
		&Vertex{Id: math.MaxInt64, Type: math.MaxInt32},
		&Vertex{Id: math.MinInt64, Type: math.MinInt32, Properties: props},
		&Vertex{Id: math.MaxInt64, Type: math.MinInt32, Properties: props},
		&Vertex{SId: "id", SType: "type", VType: IdTypeStringString},
		&Vertex{SId: "1234567890", SType: "knows", VType: IdTypeStringString},
		&Vertex{SId: "abcdefghijklml!@#$%^&*()_+", SType: "!@#$%^&( )", VType: IdTypeStringString},
		&Vertex{SId: "1234567890", SType: "knows", VType: IdTypeStringString, Properties: props},
		&Vertex{SId: "abcdefghijklml!@#$%^&*()_+", SType: "!@#$%^&( )", VType: IdTypeStringString, Properties: props},
		&Vertex{SId: string([]byte{0x0, 0x1, 0x2, 0xEE, 0xFF}), SType: string([]byte{0x0, 0x1, 0x2, 0xEE, 0xEF}), VType: IdTypeStringString, Properties: props},
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func newPropertyWithoutError(t *testing.T, key string, val interface{}) *Property {
	p := &Property{key, val}
	return p
}

func TestPropertyEncode(t *testing.T) {
	tests := []Element{
		newPropertyWithoutError(t, "abc", int64(1234)),
		newPropertyWithoutError(t, "int32", int32(1234)),
		newPropertyWithoutError(t, "bool", true),
		newPropertyWithoutError(t, "bool", false),
		newPropertyWithoutError(t, "str", "strstrstrstrtrue"),
		newPropertyWithoutError(t, "float64", 123.5),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func newFullEdge(id1 int64, type1 int32, id2 int64, type2 int32, edgetype string, dir bytegraph.DirectionType, ppts []*Property) *Edge {
	e := &Edge{
		OutV:       &Vertex{Id: id1, Type: type1},
		InV:        &Vertex{Id: id2, Type: type2},
		Type:       edgetype,
		Properties: ppts,
	}
	if dir == bytegraph.DirectionType_Reverse {
		e.OutV, e.InV = e.InV, e.OutV
	}
	return e
}

func newFullEdgeSIdSType(id1, type1, id2, type2 string, edgetype string, dir bytegraph.DirectionType, ppts []*Property) *Edge {
	e := &Edge{
		OutV:       &Vertex{SId: id1, SType: type1, VType: IdTypeStringString},
		InV:        &Vertex{SId: id2, SType: type2, VType: IdTypeStringString},
		Type:       edgetype,
		Properties: ppts,
	}
	if dir == bytegraph.DirectionType_Reverse {
		e.OutV, e.InV = e.InV, e.OutV
	}
	return e
}

func TestEdgeEncode(t *testing.T) {
	tests := []Element{
		newFullEdge(1, 2, 3, 4, "asdf", bytegraph.DirectionType_Double, nil),
		newFullEdge(math.MaxInt64, math.MaxInt32, math.MinInt64, math.MinInt32, "", bytegraph.DirectionType_Forward, nil),
		newFullEdge(0, 0, 0, 0, string([]byte{0x0, 0x1, 0x2}), bytegraph.DirectionType_Reverse, props),
		newFullEdge(math.MinInt64, math.MinInt32, math.MinInt64, math.MinInt32, "", bytegraph.DirectionType_Forward, props),
		newFullEdgeSIdSType("1234567890", "software", "abcdefhijklml", "!@#$%^&*( )_+", "", bytegraph.DirectionType_Forward, nil),
		newFullEdgeSIdSType(string([]byte{0x0, 0x1, 0x2, 0xEE, 0xEF}), string([]byte{0xEE, 0xEF}), string([]byte{0x0, 0x1, 0x2, 0xEE, 0xEF}), string([]byte{0xEE, 0xEF}), string([]byte{0x0, 0x1, 0x2}), bytegraph.DirectionType_Reverse, nil),
		newFullEdgeSIdSType("1234567890", "software", "abcdefhijklml", "!@#$%^&*( )_+", "", bytegraph.DirectionType_Forward, props),
		newFullEdgeSIdSType(string([]byte{0x0, 0x1, 0x2, 0xEE, 0xEF}), string([]byte{0xEE, 0xEF}), string([]byte{0x0, 0x1, 0x2, 0xEE, 0xEF}), string([]byte{0xEE, 0xEF}), string([]byte{0x0, 0x1, 0x2}), bytegraph.DirectionType_Reverse, props),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func newList(elems ...Element) List {
	var l List
	if len(elems) == 0 {
		return l
	}
	l = make([]Element, 0, len(elems))
	for i := 0; i < len(elems); i++ {
		l = append(l, elems[i])
	}
	return l
}

func TestListEncode(t *testing.T) {
	tests := []Element{
		newList(),
		newList(Int64(math.MaxInt64), newPropertyWithoutError(t, "str", "string"), newList(Bool(true))),
		newList(Int32(math.MinInt32), Float64(1.5), String("foobar"), Bool(false)),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}

func newPath(elems ...Element) Path {
	var l Path
	if len(elems) == 0 {
		return l
	}
	for i := 0; i < len(elems); i++ {
		l = append(l, elems[i])
	}
	return l
}

func TestPathEncode(t *testing.T) {
	tests := []Element{
		newPath(),
		newPath(Int64(0)),
		newPath(Int64(math.MaxInt64), newPropertyWithoutError(t, "str", "string"), newPath(&Vertex{Id: math.MaxInt64, Type: math.MaxInt32}, newList(Bool(true)))),
		newPath(Int32(math.MinInt32), Float64(1.5), String("foobar"), Bool(false)),
		newPath(
			&Vertex{Id: math.MaxInt64, Type: math.MaxInt32},
			newFullEdge(math.MaxInt64, math.MaxInt32, math.MinInt64, math.MinInt32, "foobaredge", bytegraph.DirectionType_Double, nil),
			&Vertex{Id: math.MinInt64, Type: math.MinInt32, Properties: props}),
		newFullEdge(math.MaxInt64, math.MaxInt32, math.MinInt64, math.MinInt32, "baredge", bytegraph.DirectionType_Forward, props),
	}
	for _, tt := range tests {
		testElementEncode(t, tt)
	}
}
