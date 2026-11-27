/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package machineset

import (
	"sort"

	pkgerrors "github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/internal/util/inplace"
	"sigs.k8s.io/cluster-api/util/conditions"
)

type deletePriority float64

const (
	mustDelete        deletePriority = 100.0
	shouldDeleteFirst deletePriority = 80.0
	shouldDelete      deletePriority = 75.0
	betterDelete      deletePriority = 50.0
	couldDelete       deletePriority = 20.0
)

// deletionOrder orders healthy Machines, which all have the couldDelete priority.
// It reports whether Machine a should be deleted before Machine b. A nil deletionOrder has no preference.
type deletionOrder func(a, b *clusterv1.Machine) bool

var (
	// randomDeletionOrder has no preference between healthy Machines.
	randomDeletionOrder deletionOrder

	// newestDeletionOrder deletes the most recently created healthy Machines first.
	newestDeletionOrder deletionOrder = createdAfter

	// oldestDeletionOrder deletes the least recently created healthy Machines first.
	oldestDeletionOrder deletionOrder = createdBefore
)

// deletionPriority returns the priority for deleting a Machine. Machines with a higher priority are deleted first.
func deletionPriority(machine *clusterv1.Machine) deletePriority {
	// Deleting machines must go first, otherwise deletion code will delete more machines while previously deleted machines
	// are still deleting.
	if !machine.DeletionTimestamp.IsZero() {
		return mustDelete
	}
	// If user expressed the intent to delete a machines, respect it by deleting this machine first when scaling down.
	if _, ok := machine.Annotations[clusterv1.DeleteMachineAnnotation]; ok {
		return shouldDeleteFirst
	}
	// If there is machine still updating in progress and the MS is scaling down, consider this machine next
	// so the system avoids to complete unnecessary in-place updates (drop machines not at the desired state first).
	if inplace.IsUpdateInProgress(machine) {
		return shouldDelete
	}
	// If there are machines not healthy, get rid of them next, because this will unblock the rollout
	// while respecting the maxUnhealthy requirement.
	if !isMachineHealthy(machine) {
		return betterDelete
	}
	return couldDelete
}

// createdAfter reports whether Machine a was created after Machine b.
// A Machine without a creation timestamp is considered newer than any Machine with one.
func createdAfter(a, b *clusterv1.Machine) bool {
	if a.CreationTimestamp.IsZero() || b.CreationTimestamp.IsZero() {
		return a.CreationTimestamp.IsZero() && !b.CreationTimestamp.IsZero()
	}
	return a.CreationTimestamp.After(b.CreationTimestamp.Time)
}

// createdBefore reports whether Machine a was created before Machine b.
// A Machine without a creation timestamp is considered newer than any Machine with one.
func createdBefore(a, b *clusterv1.Machine) bool {
	return createdAfter(b, a)
}

type sortableMachines struct {
	machines []*clusterv1.Machine
	order    deletionOrder
}

func (m sortableMachines) Len() int      { return len(m.machines) }
func (m sortableMachines) Swap(i, j int) { m.machines[i], m.machines[j] = m.machines[j], m.machines[i] }
func (m sortableMachines) Less(i, j int) bool {
	machineI, machineJ := m.machines[i], m.machines[j]
	priorityI, priorityJ := deletionPriority(machineI), deletionPriority(machineJ)
	if priorityI != priorityJ {
		return priorityJ < priorityI // high to low
	}
	// Note: Healthy Machines are ordered by comparing their creation timestamps instead of by a priority derived from
	// their age, because such a priority loses precision for Machines that are months or years old.
	if priorityI == couldDelete && m.order != nil {
		if m.order(machineI, machineJ) {
			return true
		}
		if m.order(machineJ, machineI) {
			return false
		}
	}
	// In cases where the priority is identical, it should be ensured that the same machine order is returned each time.
	// Ordering by name is a simple way to do this.
	return machineI.Name < machineJ.Name
}

func getMachinesToMovePrioritized(filteredMachines []*clusterv1.Machine, order deletionOrder) []*clusterv1.Machine {
	return getMachinesToDeletePrioritized(filteredMachines, len(filteredMachines), order)
}

func getMachinesToDeletePrioritized(filteredMachines []*clusterv1.Machine, diff int, order deletionOrder) []*clusterv1.Machine {
	if diff <= 0 {
		return []*clusterv1.Machine{}
	}

	sortable := sortableMachines{
		machines: filteredMachines,
		order:    order,
	}
	sort.Sort(sortable)

	if diff >= len(filteredMachines) {
		return sortable.machines
	}
	return sortable.machines[:diff]
}

func getDeletionOrder(ms *clusterv1.MachineSet) (deletionOrder, error) {
	// Map the Spec.Order value to the appropriate deletion order
	switch ms.Spec.Deletion.Order {
	case clusterv1.RandomMachineSetDeletionOrder:
		return randomDeletionOrder, nil
	case clusterv1.NewestMachineSetDeletionOrder:
		return newestDeletionOrder, nil
	case clusterv1.OldestMachineSetDeletionOrder:
		return oldestDeletionOrder, nil
	case "":
		return randomDeletionOrder, nil
	default:
		return nil, pkgerrors.Errorf("Unsupported deletion order %s. Must be one of 'Random', 'Newest', or 'Oldest'", ms.Spec.Deletion.Order)
	}
}

func isMachineHealthy(machine *clusterv1.Machine) bool {
	if !machine.Status.NodeRef.IsDefined() {
		return false
	}
	// Note: for the sake of prioritization, we are not making any assumption about Health when ConditionUnknown.
	if conditions.IsFalse(machine, clusterv1.MachineNodeHealthyCondition) {
		return false
	}
	healthCheckCondition := conditions.Get(machine, clusterv1.MachineHealthCheckSucceededCondition)
	if healthCheckCondition != nil && healthCheckCondition.Status == metav1.ConditionFalse {
		return false
	}
	return true
}
