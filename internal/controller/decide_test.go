package controller

import "testing"

func TestDecide(t *testing.T) {
	a := NodeState{Name: "a", InstanceID: "1"}
	b := NodeState{Name: "b", InstanceID: "2"}
	c := NodeState{Name: "c", InstanceID: "3"}
	ok := func(n NodeState) NodeState { n.Eligible = true; return n }

	tests := []struct {
		name   string
		holder string
		nodes  []NodeState
		assign bool
		target string
	}{
		{"healthy holder stays", "3", []NodeState{ok(a), ok(b), ok(c)}, false, ""},
		{"unhealthy holder moves to first eligible", "1", []NodeState{a, ok(c), ok(b)}, true, "b"},
		{"unassigned gets first eligible", "", []NodeState{ok(c), ok(b)}, true, "b"},
		{"unknown holder moves", "99", []NodeState{ok(c)}, true, "c"},
		{"nothing eligible keeps holder", "1", []NodeState{a, b, c}, false, ""},
		{"nothing eligible keeps unassigned", "", []NodeState{a, b}, false, ""},
		{"no nodes", "1", nil, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Decide(tt.holder, tt.nodes)
			if d.Assign != tt.assign || (tt.assign && d.Target.Name != tt.target) {
				t.Fatalf("got %+v, want assign=%v target=%q", d, tt.assign, tt.target)
			}
			if d.Reason == "" {
				t.Fatal("empty reason")
			}
		})
	}
}

func TestDecideDoesNotReorderInput(t *testing.T) {
	nodes := []NodeState{{Name: "b"}, {Name: "a"}}
	Decide("", nodes)
	if nodes[0].Name != "b" {
		t.Fatal("input was sorted in place")
	}
}
