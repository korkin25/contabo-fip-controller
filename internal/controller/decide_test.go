package controller

import "testing"

func TestDecide(t *testing.T) {
	a := NodeState{Name: "a", InstanceID: "1"}
	b := NodeState{Name: "b", InstanceID: "2"}
	c := NodeState{Name: "c", InstanceID: "3"}
	ok := func(n NodeState) NodeState { n.Eligible = true; return n }

	tests := []struct {
		name    string
		holder  string
		nodes   []NodeState
		exclude map[string]bool
		assign  bool
		target  string
	}{
		{"healthy holder stays", "3", []NodeState{ok(a), ok(b), ok(c)}, nil, false, ""},
		{"unhealthy holder moves to first eligible", "1", []NodeState{a, ok(c), ok(b)}, nil, true, "b"},
		{"unassigned gets first eligible", "", []NodeState{ok(c), ok(b)}, nil, true, "b"},
		{"unknown holder moves", "99", []NodeState{ok(c)}, nil, true, "c"},
		{"nothing eligible keeps holder", "1", []NodeState{a, b, c}, nil, false, ""},
		{"nothing eligible keeps unassigned", "", []NodeState{a, b}, nil, false, ""},
		{"no nodes", "1", nil, nil, false, ""},
		{"quarantined target is skipped", "1", []NodeState{a, ok(b), ok(c)}, map[string]bool{"2": true}, true, "c"},
		{"all eligible quarantined keeps holder", "1", []NodeState{a, ok(b)}, map[string]bool{"2": true}, false, ""},
		{"quarantined holder still stays", "3", []NodeState{ok(b), ok(c)}, map[string]bool{"3": true}, false, ""},
		{"quarantine does not hide other nodes from unassigned", "", []NodeState{ok(b), ok(c)}, map[string]bool{"2": true}, true, "c"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := Decide(tt.holder, tt.nodes, tt.exclude)
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
	Decide("", nodes, nil)
	if nodes[0].Name != "b" {
		t.Fatal("input was sorted in place")
	}
}
