package controller

import (
	"cmp"
	"slices"
)

// NodeState is what the controller knows about one node in one cycle.
type NodeState struct {
	Name       string
	InstanceID string
	// Eligible means every watched Service has a ready local endpoint on
	// the node, after rise/fall debouncing.
	Eligible bool
}

// Decision is the outcome of Decide.
type Decision struct {
	// Assign is true when the floating IP must move to Target.
	Assign bool
	Target NodeState
	Reason string
}

// Decide picks where the floating IP should be. holder is the instance the
// IP is assigned to now ("" when unassigned).
//
// The rules are sticky and never unassign: a healthy holder keeps the IP;
// otherwise the first eligible node by name gets it; with no eligible node
// nothing changes, because moving the IP to a node that cannot serve it does
// not help and a wrong verdict of a partitioned controller must not take
// traffic away from a node that still serves it.
func Decide(holder string, nodes []NodeState) Decision {
	nodes = slices.Clone(nodes)
	slices.SortFunc(nodes, func(a, b NodeState) int { return cmp.Compare(a.Name, b.Name) })

	if holder != "" {
		for _, n := range nodes {
			if n.InstanceID == holder && n.Eligible {
				return Decision{Reason: "holder " + n.Name + " is eligible"}
			}
		}
	}
	for _, n := range nodes {
		if n.Eligible {
			reason := "floating IP is unassigned"
			if holder != "" {
				reason = "holder instance " + holder + " is not eligible"
			}
			return Decision{Assign: true, Target: n, Reason: reason}
		}
	}
	return Decision{Reason: "no eligible node"}
}
