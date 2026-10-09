package controller

import (
	"cmp"
	"fmt"
	"slices"
)

// ReasonNoEligible is the Decision reason when no node can take the IP.
const ReasonNoEligible = "no eligible node"

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
// IP is assigned to now ("" when unassigned). exclude names instances that
// must not become the target, e.g. quarantined ones; it never takes the IP
// away from an eligible holder.
//
// The rules are sticky and never unassign: a healthy holder keeps the IP;
// otherwise the first eligible, not excluded node by name gets it; with no
// such node nothing changes, because moving the IP to a node that cannot
// serve it does not help and a wrong verdict of a partitioned controller
// must not take traffic away from a node that still serves it.
func Decide(holder string, nodes []NodeState, exclude map[string]bool) Decision {
	nodes = slices.Clone(nodes)
	slices.SortFunc(nodes, func(a, b NodeState) int { return cmp.Compare(a.Name, b.Name) })

	if holder != "" {
		for _, n := range nodes {
			if n.InstanceID == holder && n.Eligible {
				return Decision{Reason: "holder " + n.Name + " is eligible"}
			}
		}
	}
	skipped := 0
	for _, n := range nodes {
		if n.Eligible && exclude[n.InstanceID] {
			skipped++
			continue
		}
		if n.Eligible {
			reason := "floating IP is unassigned"
			if holder != "" {
				reason = "holder instance " + holder + " is not eligible"
			}
			return Decision{Assign: true, Target: n, Reason: reason}
		}
	}
	if skipped > 0 {
		return Decision{Reason: fmt.Sprintf("every eligible node (%d) is quarantined", skipped)}
	}
	return Decision{Reason: ReasonNoEligible}
}
