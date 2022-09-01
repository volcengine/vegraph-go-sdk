package structure

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

var (
	ev0 = String("key")
	ev1 = Int64(31415926)

	epp0 = &Property{
		Key:   "age",
		Value: 18,
	}

	epp1 = &Property{
		Key:   "weight",
		Value: -80.5,
	}

	epp2 = &Property{
		Key:   "name",
		Value: "marko",
	}

	epp3 = &Property{
		Key:   "duration",
		Value: 0.0000001,
	}

	epp4 = &Property{
		Key:   "randomness",
		Value: nil,
	}

	evt0 = &Vertex{
		Id:         10001,
		Type:       10,
		Properties: []*Property{epp0, epp1},
	}

	evt00 = &Vertex{
		Id:         10001,
		Type:       10,
		Properties: []*Property{epp0, epp1},
	}

	evt1 = &Vertex{
		Id:         10002,
		Type:       10,
		Properties: []*Property{epp2, epp3},
	}

	evt2 = &Vertex{
		Id:         10003,
		Type:       10,
		Properties: []*Property{},
	}

	evt3 = &Vertex{
		Id:   10004,
		Type: 10,
	}

	eeg = &Edge{
		OutV:       evt0,
		InV:        evt1,
		Type:       "knows",
		Properties: []*Property{epp3, epp4},
	}

	eph = Path{
		evt0, evt1, evt2, evt3, eeg,
	}

	elt = List{
		evt0, evt1, evt2, evt3, eeg,
	}

	elt1 = List{
		evt3, evt2, evt1, evt0, eeg,
	}

	emp = Map{
		ev0: elt,
		ev1: eph,
	}

	cases = []Element{
		ev0,
		ev1,
		epp0,
		epp1,
		epp2,
		epp3,
		epp4,
		evt0,
		evt1,
		evt2,
		evt3,
		eeg,
		eph,
		elt,
		emp,
	}
)

type EqSuite struct {
	suite.Suite
}

func (this *EqSuite) SetupSuite() {
}

func (this *EqSuite) TestEq() {
	for _, el := range cases {
		this.Truef(el.Eq(el, true), "%v", el)
	}
}

type Case struct {
	Left            Element
	StringValue     string
	sortStringValue string
	Other           Element
	equalNonStrict  bool
	equalStrict     bool
}

func (this *EqSuite) TestSortString() {
	for _, cs := range []Case{
		{
			Left:            Int32(18),
			StringValue:     "18",
			sortStringValue: "18",
			Other:           Int32(18),
			equalNonStrict:  true,
			equalStrict:     true,
		},
		{
			Left:            Int32(18),
			StringValue:     "18",
			sortStringValue: "18",
			Other:           Int64(18),
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            Float32(18),
			StringValue:     "18",
			sortStringValue: "18",
			Other:           Float64(18),
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            elt,
			StringValue:     "[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10004, Type:10}, Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}]",
			sortStringValue: "[Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10004, Type:10}]",
			Other:           elt,
			equalNonStrict:  true,
			equalStrict:     true,
		},
		{
			Left:            elt,
			StringValue:     "[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10004, Type:10}, Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}]",
			sortStringValue: "[Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10004, Type:10}]",
			Other:           elt1,
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            eph,
			StringValue:     "Path[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10004, Type:10}, Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}]",
			sortStringValue: "Path[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10004, Type:10}, Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}]",
			Other:           elt,
			equalNonStrict:  false,
			equalStrict:     false,
		},
		{
			Left:            List{evt0, evt1, evt0},
			StringValue:     "[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}]",
			sortStringValue: "[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]",
			Other:           List{evt0, evt0, evt1},
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            List{List{evt0, evt1, evt0}},
			StringValue:     "[[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}]]",
			sortStringValue: "[[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]]",
			Other:           List{List{evt0, evt0, evt1}},
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            Map{evt0: List{evt0, evt1, evt2, evt0, eeg}},
			StringValue:     "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}]}",
			sortStringValue: "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Edge{OutV:Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, InV:Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Type:knows, properties:[Property{Key:duration, Value:1e-07}, Property{Key:randomness, Value:<nil>}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}]}",
			Other:           Map{evt0: List{evt0, evt0, evt2, evt1, eeg}},
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            Map{evt0: List{evt0, evt1}, evt1: List{evt1, evt2}},
			StringValue:     "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}], Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}]}",
			sortStringValue: "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}], Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}]}",
			Other:           Map{evt1: List{evt1, evt2}, evt0: List{evt0, evt1}},
			equalNonStrict:  true,
			equalStrict:     true,
		},
		{
			Left:            Map{evt0: List{evt0, evt1, evt0}, evt1: List{evt1, evt1, evt2}},
			StringValue:     "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}], Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}]}",
			sortStringValue: "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}], Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}, Vertex{Id:10003, Type:10}]}",
			Other:           Map{evt1: List{evt1, evt2, evt1}, evt0: List{evt0, evt0, evt1}},
			equalNonStrict:  true,
			equalStrict:     false,
		},
		{
			Left:            Map{evt0: List{evt1}},
			StringValue:     "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]}",
			sortStringValue: "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]}",
			Other:           Map{evt00: List{evt1}},
			equalNonStrict:  true,
			equalStrict:     true,
		},
		{
			Left:            MapStruct{Elems: map[Element]Element{evt0: List{evt1}}},
			StringValue:     "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]}",
			sortStringValue: "Map{Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}: [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]}",
			Other:           &MapStruct{Elems: map[Element]Element{evt0: List{evt1}}},
			equalNonStrict:  true,
			equalStrict:     true,
		},
		{
			Left:            ListStruct{Elems: []Element{evt0, List{evt1}}},
			StringValue:     "[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]]",
			sortStringValue: "[Vertex{Id:10001, Type:10, properties:[Property{Key:age, Value:18}, Property{Key:weight, Value:-80.5}]}, [Vertex{Id:10002, Type:10, properties:[Property{Key:duration, Value:1e-07}, Property{Key:name, Value:\"marko\"}]}]]",
			Other:           &ListStruct{Elems: []Element{evt0, List{evt1}}},
			equalNonStrict:  true,
			equalStrict:     true,
		},
	} {
		this.Equal(cs.StringValue, cs.Left.String(), "%v", cs.Left)
		this.Equal(cs.sortStringValue, cs.Left.sortString(), "%v", cs.Left)
		this.Equal(cs.equalNonStrict, cs.Left.Eq(cs.Other, false), "%v : %v", cs.Other, cs.Left)
		this.Equal(cs.equalStrict, cs.Left.Eq(cs.Other, true), "%v : %v", cs.Other, cs.Left)
	}
}
func TestEqSuite(t *testing.T) {
	s := &EqSuite{}
	suite.Run(t, s)
}
