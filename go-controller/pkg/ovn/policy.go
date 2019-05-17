package ovn

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/dbtransaction"
	"github.com/TomCodeLV/OVSDB-golang-lib/pkg/helpers"
	"github.com/openvswitch/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"
)

func (oc *Controller) syncNetworkPoliciesPortGroup(
	networkPolicies []interface{}) {
	expectedPolicies := make(map[string]map[string]bool)
	for _, npInterface := range networkPolicies {
		policy, ok := npInterface.(*knet.NetworkPolicy)
		if !ok {
			logrus.Errorf("Spurious object in syncNetworkPolicies: %v",
				npInterface)
			continue
		}
		expectedPolicies[policy.Namespace] = map[string]bool{
			policy.Name: true}
	}

	err := oc.forEachAddressSetUnhashedName(func(addrSetName, namespaceName,
		policyName string) {
		if policyName != "" &&
			!expectedPolicies[namespaceName][policyName] {
			// policy doesn't exist on k8s. Delete the port group
			portGroupName := fmt.Sprintf("%s_%s", namespaceName, policyName)
			hashedLocalPortGroup := hashedPortGroup(portGroupName)
			oc.deletePortGroup(hashedLocalPortGroup)

			// delete the address sets for this policy from OVN
			oc.deleteAddressSet(hashedAddressSet(addrSetName))
		}
	})
	if err != nil {
		logrus.Errorf("Error in syncing network policies: %v", err)
	}
}

func (oc *Controller) getACLUUIDFromMap(acls map[string]interface{}, filters map[string]interface{}) string {
	for aclUUID, acl := range acls {
		found := true
		for filterID, filter := range filters {
			if filterID == "external_ids" {
				if externalIds, ok := acl.(map[string]interface{})["external_ids"]; ok {
					for key, val := range filter.(map[string]string) {
						externalID := externalIds.(map[string]interface{})[key]
						if externalID == nil || externalID.(string) != val {
							found = false
							break
						}
					}
					if !found {
						break
					}
				} else {
					found = false
					break
				}
			} else if acl.(map[string]interface{})[filterID] != filter {
				found = false
				break
			}
		}

		if found {
			return aclUUID
		}
	}

	return ""
}

func (oc *Controller) insertACL(row map[string]interface{}, portGroupUUID string) error {
	var err error

	retry := true
	for retry {
		txn := oc.ovnNBDB.Transaction("OVN_Northbound")
		newACLID := txn.Insert(dbtransaction.Insert{
			Table: "ACL",
			Row:   row,
		})
		txn.InsertReferences(dbtransaction.InsertReferences{
			Table:           "Port_Group",
			WhereId:         portGroupUUID,
			ReferenceColumn: "acls",
			InsertIdsList:   []string{newACLID},
			Wait:            true,
			Cache:           oc.ovnNbCache,
		})
		_, err, retry = txn.Commit()
	}

	return err
}

func (oc *Controller) addACLAllow(np *namespacePolicy,
	match, l4Match string, ipBlockCidr bool, gressNum int,
	policyType knet.PolicyType) {
	var direction, action string
	direction = toLport
	if policyType == knet.PolicyTypeIngress {
		action = "allow-related"
	} else {
		action = "allow"
	}

	acls := oc.ovnNbCache.GetMap("ACL", "uuid")
	uuid := oc.getACLUUIDFromMap(acls, map[string]interface{}{
		"external_ids": map[string]string{
			"l4Match":      l4Match,
			"ipblock_cidr": fmt.Sprintf("%t", ipBlockCidr),
			"namespace":    np.namespace,
			"policy":       np.name,
			fmt.Sprintf("%s_num", policyType): fmt.Sprintf("%d", gressNum),
			"policy_type":                     fmt.Sprintf("%s", policyType),
		},
	})

	if uuid != "" {
		return
	}

	// strip out match value from 'match="some match value"'
	r, _ := regexp.Compile("match=\"(.*)\"")
	matches := r.FindStringSubmatch(match)
	if matches != nil {
		match = matches[1]
	}

	err := oc.insertACL(map[string]interface{}{
		"priority":  defaultAllowPriorityInt,
		"direction": direction,
		"match":     match,
		"action":    action,
		"external_ids": helpers.MakeOVSDBMap(map[string]interface{}{
			"l4Match":      l4Match,
			"ipblock_cidr": fmt.Sprintf("%t", ipBlockCidr),
			"namespace":    np.namespace,
			"policy":       np.name,
			fmt.Sprintf("%s_num", policyType): fmt.Sprintf("%d", gressNum),
			"policy_type":                     fmt.Sprintf("%s", policyType),
		}),
	}, np.portGroupUUID)

	if err != nil {
		logrus.Errorf("failed to create the acl allow rule for namespace=%s, policy=%s, (%v)",
			np.namespace, np.name, err)
	}
}

func (oc *Controller) modifyACLAllow(namespace, policy,
	oldMatch string, newMatch string, gressNum int,
	policyType knet.PolicyType) {
	// strip out match value from 'match="some match value"'
	r, _ := regexp.Compile("match=\"(.*)\"")
	matches := r.FindStringSubmatch(oldMatch)
	if matches != nil {
		oldMatch = matches[1]
	}

	acls := oc.ovnNbCache.GetMap("ACL", "uuid")
	uuid := oc.getACLUUIDFromMap(acls, map[string]interface{}{
		"match": oldMatch,
		"external_ids": map[string]string{
			"namespace": namespace,
			"policy":    policy,
			fmt.Sprintf("%s_num", policyType): fmt.Sprintf("%d", gressNum),
			"policy_type":                     fmt.Sprintf("%s", policyType),
		},
	})

	if uuid != "" {
		// We already have an ACL. We will update it.
		r, _ := regexp.Compile("match=\"(.*)\"")
		matches := r.FindStringSubmatch(newMatch)
		var newMatch string
		if matches != nil {
			newMatch = matches[1]
		}

		var err error
		retry := true
		for retry {
			txn := oc.ovnNBDB.Transaction("OVN_Northbound")
			txn.Update(dbtransaction.Update{
				Table: "ACL",
				Where: [][]interface{}{{"_uuid", "==", []string{"uuid", uuid}}},
				Row: map[string]interface{}{
					"match": newMatch,
				},
			})
			_, err, retry = txn.Commit()
		}

		if err != nil {
			logrus.Errorf("failed to modify the allow-from rule for namespace=%s, policy=%s, (%v)",
				namespace, policy, err)
		}
	}
}

func (oc *Controller) addIPBlockACLDeny(np *namespacePolicy,
	except, priority string, gressNum int, policyType knet.PolicyType) {
	var match, l3Match, direction, lportMatch string
	direction = toLport
	if policyType == knet.PolicyTypeIngress {
		lportMatch = fmt.Sprintf("outport == @%s", np.portGroupName)
		l3Match = fmt.Sprintf("ip4.src == %s", except)
		match = fmt.Sprintf("%s && %s", lportMatch, l3Match)
	} else {
		lportMatch = fmt.Sprintf("inport == @%s", np.portGroupName)
		l3Match = fmt.Sprintf("ip4.dst == %s", except)
		match = fmt.Sprintf("%s && %s", lportMatch, l3Match)
	}

	acls := oc.ovnNbCache.GetMap("ACL", "uuid")
	uuid := oc.getACLUUIDFromMap(acls, map[string]interface{}{
		"match":  match,
		"action": "drop",
		"external_ids": map[string]string{
			"ipblock-deny-policy-type":        fmt.Sprintf("%s", policyType),
			"namespace":                       np.namespace,
			fmt.Sprintf("%s_num", policyType): fmt.Sprintf("%d", gressNum),
			"policy": np.name,
		},
	})

	if uuid != "" {
		return
	}

	priorityInt, _ := strconv.Atoi(priority)

	err := oc.insertACL(map[string]interface{}{
		"priority":  priorityInt,
		"direction": direction,
		"match":     match,
		"action":    "drop",
		"external_ids": helpers.MakeOVSDBMap(map[string]interface{}{
			"namespace": np.namespace,
			"policy":    np.name,
			fmt.Sprintf("%s_num", policyType): fmt.Sprintf("%d", gressNum),
			"ipblock-deny-policy-type":        fmt.Sprintf("%s", policyType),
		}),
	}, np.portGroupUUID)

	if err != nil {
		logrus.Errorf("failed to create the acl allow rule for namespace=%s, policy=%s, (%v)",
			np.namespace, np.name, err)
	}
}

func (oc *Controller) addACLDenyPortGroup(portGroupUUID, portGroupName,
	priority string, policyType knet.PolicyType) error {
	var match, direction string
	direction = toLport
	if policyType == knet.PolicyTypeIngress {
		match = fmt.Sprintf("outport == @%s", portGroupName)
	} else {
		match = fmt.Sprintf("inport == @%s", portGroupName)
	}

	acls := oc.ovnNbCache.GetMap("ACL", "uuid")
	uuid := oc.getACLUUIDFromMap(acls, map[string]interface{}{
		"match":  match,
		"action": "drop",
		"external_ids": map[string]string{
			"default-deny-policy-type": fmt.Sprintf("%s", policyType),
		},
	})

	if uuid != "" {
		return nil
	}

	priorityInt, _ := strconv.Atoi(priority)
	err := oc.insertACL(map[string]interface{}{
		"priority":  priorityInt,
		"direction": direction,
		"match":     match,
		"action":    "drop",
		"external_ids": helpers.MakeOVSDBMap(map[string]interface{}{
			"default-deny-policy-type": fmt.Sprintf("%s", policyType),
		}),
	}, portGroupUUID)

	if err != nil {
		return fmt.Errorf("error executing create ACL command for "+
			"policy type %s (%v)", policyType, err)
	}
	return nil
}

func (oc *Controller) addToACLDeny(portGroup, logicalPort string) {
	logicalPortUUID := oc.getLogicalPortUUID(logicalPort)
	if logicalPortUUID == "" {
		return
	}

	pg := oc.ovnNbCache.GetMap("Port_Group", "uuid", portGroup)

	if m, ok := pg["ports"].(map[string]interface{}); !ok || m[logicalPortUUID] == nil {
		var err error

		retry := true
		for retry {
			txn := oc.ovnNBDB.Transaction("OVN_Northbound")
			txn.InsertReferences(dbtransaction.InsertReferences{
				Table:                 "Port_Group",
				WhereId:               portGroup,
				ReferenceColumn:       "ports",
				InsertExistingIdsList: []string{logicalPortUUID},
				Wait:  true,
				Cache: oc.ovnNbCache,
			})
			_, err, retry = txn.Commit()
		}

		if err != nil {
			logrus.Errorf("Failed to add logicalPort %s to portGroup %s (%v)", logicalPort, portGroup, err)
		}
	}
}

func (oc *Controller) deleteFromACLDeny(portGroup, logicalPort string) {
	logicalPortUUID := oc.getLogicalPortUUID(logicalPort)
	if logicalPortUUID == "" {
		return
	}

	pg := oc.ovnNbCache.GetMap("Port_Group", "uuid", portGroup)

	if m, ok := pg["ports"].(map[string]interface{}); ok && m[logicalPortUUID] != nil {
		var err error

		retry := true
		for retry {
			txn := oc.ovnNBDB.Transaction("OVN_Northbound")
			txn.DeleteReferences(dbtransaction.DeleteReferences{
				Table:           "Port_Group",
				WhereId:         portGroup,
				ReferenceColumn: "ports",
				DeleteIdsList:   []string{logicalPortUUID},
				Wait:            true,
				Cache:           oc.ovnNbCache,
			})
			_, err, retry = txn.Commit()
		}

		if err != nil {
			logrus.Errorf("Failed to delete logicalPort %s to portGroup %s (%v)", logicalPort, portGroup, err)
		}
	}
}

func (oc *Controller) localPodAddACL(np *namespacePolicy,
	gress *gressPolicy) {
	l3Match := gress.getL3MatchFromAddressSet()

	var lportMatch, cidrMatch string
	if gress.policyType == knet.PolicyTypeIngress {
		lportMatch = fmt.Sprintf("outport == @%s", np.portGroupName)
	} else {
		lportMatch = fmt.Sprintf("inport == @%s", np.portGroupName)
	}

	// If IPBlock CIDR is not empty and except string [] is not empty,
	// add deny acl rule with priority ipBlockDenyPriority (1010).
	if len(gress.ipBlockCidr) > 0 && len(gress.ipBlockExcept) > 0 {
		except := fmt.Sprintf("{%s}", strings.Join(gress.ipBlockExcept, ", "))
		oc.addIPBlockACLDeny(np, except, ipBlockDenyPriority, gress.idx,
			gress.policyType)
	}

	if len(gress.portPolicies) == 0 {
		match := fmt.Sprintf("match=\"%s && %s\"", l3Match,
			lportMatch)
		l4Match := noneMatch

		if len(gress.ipBlockCidr) > 0 {
			// Add ACL allow rule for IPBlock CIDR
			cidrMatch = gress.getMatchFromIPBlock(lportMatch, l4Match)
			oc.addACLAllow(np, cidrMatch, l4Match,
				true, gress.idx, gress.policyType)
		}
		oc.addACLAllow(np, match, l4Match,
			false, gress.idx, gress.policyType)
	}
	for _, port := range gress.portPolicies {
		l4Match, err := port.getL4Match()
		if err != nil {
			continue
		}
		match := fmt.Sprintf("match=\"%s && %s && %s\"",
			l3Match, l4Match, lportMatch)
		if len(gress.ipBlockCidr) > 0 {
			// Add ACL allow rule for IPBlock CIDR
			cidrMatch = gress.getMatchFromIPBlock(lportMatch, l4Match)
			oc.addACLAllow(np, cidrMatch, l4Match,
				true, gress.idx, gress.policyType)
		}
		oc.addACLAllow(np, match, l4Match,
			false, gress.idx, gress.policyType)
	}
}

func (oc *Controller) createDefaultDenyPortGroup(policyType knet.PolicyType) {
	var portGroupName string
	if policyType == knet.PolicyTypeIngress {
		if oc.portGroupIngressDeny != "" {
			return
		}
		portGroupName = "ingressDefaultDeny"
	} else if policyType == knet.PolicyTypeEgress {
		if oc.portGroupEgressDeny != "" {
			return
		}
		portGroupName = "egressDefaultDeny"
	}
	portGroupUUID, err := oc.createPortGroup(portGroupName, portGroupName)
	if err != nil {
		logrus.Errorf("Failed to create port_group for %s (%v)",
			portGroupName, err)
		return
	}
	err = oc.addACLDenyPortGroup(portGroupUUID, portGroupName,
		defaultDenyPriority, policyType)
	if err != nil {
		logrus.Errorf("Failed to create default deny port group %v", err)
		return
	}

	if policyType == knet.PolicyTypeIngress {
		oc.portGroupIngressDeny = portGroupUUID
	} else if policyType == knet.PolicyTypeEgress {
		oc.portGroupEgressDeny = portGroupUUID
	}
}

func (oc *Controller) localPodAddDefaultDeny(
	policy *knet.NetworkPolicy, logicalPort string) {

	oc.lspMutex.Lock()
	defer oc.lspMutex.Unlock()

	oc.createDefaultDenyPortGroup(knet.PolicyTypeIngress)
	oc.createDefaultDenyPortGroup(knet.PolicyTypeEgress)

	// Default deny rule.
	// 1. Any pod that matches a network policy should get a default
	// ingress deny rule.  This is irrespective of whether there
	// is a ingress section in the network policy. But, if
	// PolicyTypes in the policy has only "egress" in it, then
	// it is a 'egress' only network policy and we should not
	// add any default deny rule for ingress.
	// 2. If there is any "egress" section in the policy or
	// the PolicyTypes has 'egress' in it, we add a default
	// egress deny rule.

	// Handle condition 1 above.
	if !(len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) {
		if oc.lspIngressDenyCache[logicalPort] == 0 {
			oc.addToACLDeny(oc.portGroupIngressDeny, logicalPort)
		}
		oc.lspIngressDenyCache[logicalPort]++
	}

	// Handle condition 2 above.
	if (len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) ||
		len(policy.Spec.Egress) > 0 || len(policy.Spec.PolicyTypes) == 2 {
		if oc.lspEgressDenyCache[logicalPort] == 0 {
			oc.addToACLDeny(oc.portGroupEgressDeny, logicalPort)
		}
		oc.lspEgressDenyCache[logicalPort]++
	}
}

func (oc *Controller) localPodDelDefaultDeny(
	policy *knet.NetworkPolicy, logicalPort string) {
	oc.lspMutex.Lock()
	defer oc.lspMutex.Unlock()

	if !(len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) {
		if oc.lspIngressDenyCache[logicalPort] > 0 {
			oc.lspIngressDenyCache[logicalPort]--
			if oc.lspIngressDenyCache[logicalPort] == 0 {
				oc.deleteFromACLDeny(oc.portGroupIngressDeny, logicalPort)
			}
		}
	}

	if (len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) ||
		len(policy.Spec.Egress) > 0 || len(policy.Spec.PolicyTypes) == 2 {
		if oc.lspEgressDenyCache[logicalPort] > 0 {
			oc.lspEgressDenyCache[logicalPort]--
			if oc.lspEgressDenyCache[logicalPort] == 0 {
				oc.deleteFromACLDeny(oc.portGroupEgressDeny, logicalPort)
			}
		}
	}
}

func (oc *Controller) handleLocalPodSelectorAddFunc(
	policy *knet.NetworkPolicy, np *namespacePolicy,
	obj interface{}) {
	pod := obj.(*kapi.Pod)

	ipAddress := oc.getIPFromOvnAnnotation(pod.Annotations["ovn"])
	if ipAddress == "" {
		return
	}

	logicalSwitch := pod.Spec.NodeName
	if logicalSwitch == "" {
		return
	}

	// Get the logical port name.
	logicalPort := fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
	logicalPortUUID := oc.getLogicalPortUUID(logicalPort)
	if logicalPortUUID == "" {
		return
	}

	np.Lock()
	defer np.Unlock()

	if np.deleted {
		return
	}

	if np.localPods[logicalPort] {
		return
	}

	oc.localPodAddDefaultDeny(policy, logicalPort)

	if np.portGroupUUID == "" {
		return
	}

	pg := oc.ovnNbCache.GetMap("Port_Group", "uuid", np.portGroupUUID)

	if m, ok := pg["ports"].(map[string]interface{}); !ok || m[logicalPortUUID] == nil {
		var err error

		retry := true
		for retry {
			txn := oc.ovnNBDB.Transaction("OVN_Northbound")
			txn.InsertReferences(dbtransaction.InsertReferences{
				Table:                 "Port_Group",
				WhereId:               np.portGroupUUID,
				ReferenceColumn:       "ports",
				InsertExistingIdsList: []string{logicalPortUUID},
				Wait:  true,
				Cache: oc.ovnNbCache,
			})
			_, err, retry = txn.Commit()
		}

		if err != nil {
			logrus.Errorf("Failed to add logicalPort %s to portGroup %s (%v)",
				logicalPort, np.portGroupUUID, err)
		}
	}

	np.localPods[logicalPort] = true
}

func (oc *Controller) handleLocalPodSelectorDelFunc(
	policy *knet.NetworkPolicy, np *namespacePolicy,
	obj interface{}) {
	pod := obj.(*kapi.Pod)

	logicalSwitch := pod.Spec.NodeName
	if logicalSwitch == "" {
		return
	}

	// Get the logical port name.
	logicalPort := fmt.Sprintf("%s_%s", pod.Namespace, pod.Name)
	logicalPortUUID := oc.getLogicalPortUUID(logicalPort)

	np.Lock()
	defer np.Unlock()

	if np.deleted {
		return
	}

	if !np.localPods[logicalPort] {
		return
	}
	delete(np.localPods, logicalPort)
	oc.localPodDelDefaultDeny(policy, logicalPort)

	if logicalPortUUID == "" || np.portGroupUUID == "" {
		return
	}

	pg := oc.ovnNbCache.GetMap("Port_Group", "uuid", np.portGroupUUID)

	if m, ok := pg["ports"].(map[string]interface{}); ok && m[logicalPortUUID] != nil {
		var err error

		retry := true
		for retry {
			txn := oc.ovnNBDB.Transaction("OVN_Northbound")
			txn.DeleteReferences(dbtransaction.DeleteReferences{
				Table:           "Port_Group",
				WhereId:         np.portGroupUUID,
				ReferenceColumn: "ports",
				DeleteIdsList:   []string{logicalPortUUID},
				Wait:            true,
				Cache:           oc.ovnNbCache,
			})
			_, err, retry = txn.Commit()
		}

		if err != nil {
			logrus.Errorf("Failed to delete logicalPort %s to portGroup %s (%v)",
				logicalPort, np.portGroupUUID, err)
		}
	}
}

func (oc *Controller) handleLocalPodSelector(
	policy *knet.NetworkPolicy, np *namespacePolicy) {

	h, err := oc.watchFactory.AddFilteredPodHandler(policy.Namespace,
		&policy.Spec.PodSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				oc.handleLocalPodSelectorAddFunc(policy, np, obj)
			},
			DeleteFunc: func(obj interface{}) {
				oc.handleLocalPodSelectorDelFunc(policy, np, obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oc.handleLocalPodSelectorAddFunc(policy, np, newObj)
			},
		}, nil)
	if err != nil {
		logrus.Errorf("error watching local pods for policy %s in namespace %s: %v",
			policy.Name, policy.Namespace, err)
		return
	}

	np.podHandlerList = append(np.podHandlerList, h)
}

func (oc *Controller) handlePeerNamespaceAndPodSelector(
	policy *knet.NetworkPolicy,
	namespaceSelector *metav1.LabelSelector,
	podSelector *metav1.LabelSelector,
	addressSet string,
	addressMap map[string]bool,
	gress *gressPolicy,
	np *namespacePolicy) {

	namespaceHandler, err := oc.watchFactory.AddFilteredNamespaceHandler("",
		namespaceSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				namespace := obj.(*kapi.Namespace)
				np.Lock()
				defer np.Unlock()

				if np.deleted {
					return
				}

				podHandler, err := oc.watchFactory.AddFilteredPodHandler(namespace.Name,
					podSelector,
					cache.ResourceEventHandlerFuncs{
						AddFunc: func(obj interface{}) {
							oc.handlePeerPodSelectorAddUpdate(policy, np, addressMap, addressSet, obj)
						},
						DeleteFunc: func(obj interface{}) {
							oc.handlePeerPodSelectorDelete(policy, np, addressMap, addressSet, obj)
						},
						UpdateFunc: func(oldObj, newObj interface{}) {
							oc.handlePeerPodSelectorAddUpdate(policy, np, addressMap, addressSet, newObj)
						},
					}, nil)
				if err != nil {
					logrus.Errorf("error watching pods in namespace %s for policy %s: %v", namespace.Name, policy.Name, err)
					return
				}
				np.podHandlerList = append(np.podHandlerList, podHandler)
			},
			DeleteFunc: func(obj interface{}) {
				return
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				return
			},
		}, nil)
	if err != nil {
		logrus.Errorf("error watching namespaces for policy %s: %v",
			policy.Name, err)
		return
	}
	np.nsHandlerList = append(np.nsHandlerList, namespaceHandler)
}

func (oc *Controller) handlePeerPodSelectorAddUpdate(
	policy *knet.NetworkPolicy, np *namespacePolicy,
	addressMap map[string]bool, addressSet string,
	obj interface{}) {

	pod := obj.(*kapi.Pod)
	ipAddress := oc.getIPFromOvnAnnotation(pod.Annotations["ovn"])
	if ipAddress == "" || addressMap[ipAddress] {
		return
	}

	np.Lock()
	defer np.Unlock()
	if np.deleted {
		return
	}

	addressMap[ipAddress] = true
	addresses := make([]string, 0, len(addressMap))
	for k := range addressMap {
		addresses = append(addresses, k)
	}
	oc.setAddressSet(addressSet, addresses)

}

func (oc *Controller) handlePeerPodSelectorDelete(
	policy *knet.NetworkPolicy, np *namespacePolicy,
	addressMap map[string]bool, addressSet string,
	obj interface{}) {

	pod := obj.(*kapi.Pod)

	ipAddress := oc.getIPFromOvnAnnotation(pod.Annotations["ovn"])
	if ipAddress == "" {
		return
	}

	np.Lock()
	defer np.Unlock()
	if np.deleted {
		return
	}

	if !addressMap[ipAddress] {
		return
	}

	delete(addressMap, ipAddress)

	addresses := make([]string, 0, len(addressMap))
	for k := range addressMap {
		addresses = append(addresses, k)
	}
	oc.setAddressSet(addressSet, addresses)
}

func (oc *Controller) handlePeerPodSelector(
	policy *knet.NetworkPolicy, podSelector *metav1.LabelSelector,
	addressSet string, addressMap map[string]bool, np *namespacePolicy) {

	h, err := oc.watchFactory.AddFilteredPodHandler(policy.Namespace,
		podSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				oc.handlePeerPodSelectorAddUpdate(policy, np, addressMap, addressSet, obj)
			},
			DeleteFunc: func(obj interface{}) {
				oc.handlePeerPodSelectorDelete(policy, np, addressMap, addressSet, obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				oc.handlePeerPodSelectorAddUpdate(policy, np, addressMap, addressSet, newObj)
			},
		}, nil)
	if err != nil {
		logrus.Errorf("error watching peer pods for policy %s in namespace %s: %v",
			policy.Name, policy.Namespace, err)
		return
	}

	np.podHandlerList = append(np.podHandlerList, h)

}

func (oc *Controller) handlePeerNamespaceSelectorModify(
	gress *gressPolicy, np *namespacePolicy, oldl3Match, newl3Match string) {

	var lportMatch string
	if gress.policyType == knet.PolicyTypeIngress {
		lportMatch = fmt.Sprintf("outport == @%s", np.portGroupName)
	} else {
		lportMatch = fmt.Sprintf("inport == @%s", np.portGroupName)
	}
	if len(gress.portPolicies) == 0 {
		oldMatch := fmt.Sprintf("match=\"%s && %s\"", oldl3Match,
			lportMatch)
		newMatch := fmt.Sprintf("match=\"%s && %s\"", newl3Match,
			lportMatch)
		oc.modifyACLAllow(np.namespace, np.name,
			oldMatch, newMatch, gress.idx, gress.policyType)
	}
	for _, port := range gress.portPolicies {
		l4Match, err := port.getL4Match()
		if err != nil {
			continue
		}
		oldMatch := fmt.Sprintf("match=\"%s && %s && %s\"",
			oldl3Match, l4Match, lportMatch)
		newMatch := fmt.Sprintf("match=\"%s && %s && %s\"",
			newl3Match, l4Match, lportMatch)
		oc.modifyACLAllow(np.namespace, np.name,
			oldMatch, newMatch, gress.idx, gress.policyType)
	}
}

func (oc *Controller) handlePeerNamespaceSelector(
	policy *knet.NetworkPolicy,
	namespaceSelector *metav1.LabelSelector,
	gress *gressPolicy, np *namespacePolicy) {

	h, err := oc.watchFactory.AddFilteredNamespaceHandler("",
		namespaceSelector,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				namespace := obj.(*kapi.Namespace)
				np.Lock()
				defer np.Unlock()
				if np.deleted {
					return
				}
				hashedAddressSet := hashedAddressSet(namespace.Name)
				oldL3Match, newL3Match, added := gress.addAddressSet(hashedAddressSet)
				if added {
					oc.handlePeerNamespaceSelectorModify(gress,
						np, oldL3Match, newL3Match)
				}
			},
			DeleteFunc: func(obj interface{}) {
				namespace := obj.(*kapi.Namespace)
				np.Lock()
				defer np.Unlock()
				if np.deleted {
					return
				}
				hashedAddressSet := hashedAddressSet(namespace.Name)
				oldL3Match, newL3Match, removed := gress.delAddressSet(hashedAddressSet)
				if removed {
					oc.handlePeerNamespaceSelectorModify(gress,
						np, oldL3Match, newL3Match)
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				return
			},
		}, nil)
	if err != nil {
		logrus.Errorf("error watching namespaces for policy %s: %v",
			policy.Name, err)
		return
	}

	np.nsHandlerList = append(np.nsHandlerList, h)

}

// AddNetworkPolicy adds network policy and create corresponding acl rules
func (oc *Controller) addNetworkPolicyPortGroup(policy *knet.NetworkPolicy) {
	logrus.Infof("Adding network policy %s in namespace %s", policy.Name,
		policy.Namespace)

	if oc.namespacePolicies[policy.Namespace] != nil &&
		oc.namespacePolicies[policy.Namespace][policy.Name] != nil {
		return
	}

	err := oc.waitForNamespaceEvent(policy.Namespace)
	if err != nil {
		logrus.Errorf("failed to wait for namespace %s event (%v)",
			policy.Namespace, err)
		return
	}

	np := &namespacePolicy{}
	np.name = policy.Name
	np.namespace = policy.Namespace
	np.ingressPolicies = make([]*gressPolicy, 0)
	np.egressPolicies = make([]*gressPolicy, 0)
	np.podHandlerList = make([]*factory.Handler, 0)
	np.nsHandlerList = make([]*factory.Handler, 0)
	np.localPods = make(map[string]bool)

	// Create a port group for the policy. All the pods that this policy
	// selects will be eventually added to this port group.
	readableGroupName := fmt.Sprintf("%s_%s", policy.Namespace, policy.Name)
	np.portGroupName = hashedPortGroup(readableGroupName)

	np.portGroupUUID, err = oc.createPortGroup(readableGroupName,
		np.portGroupName)
	if err != nil {
		logrus.Errorf("Failed to create port_group for network policy %s in "+
			"namespace %s", policy.Name, policy.Namespace)
		return
	}

	// Go through each ingress rule.  For each ingress rule, create an
	// addressSet for the peer pods.
	for i, ingressJSON := range policy.Spec.Ingress {
		logrus.Debugf("Network policy ingress is %+v", ingressJSON)

		ingress := newGressPolicy(knet.PolicyTypeIngress, i)

		// Each ingress rule can have multiple ports to which we allow traffic.
		for _, portJSON := range ingressJSON.Ports {
			ingress.addPortPolicy(&portJSON)
		}

		hashedLocalAddressSet := ""
		// peerPodAddressMap represents the IP addresses of all the peer pods
		// for this ingress.
		peerPodAddressMap := make(map[string]bool)
		if len(ingressJSON.From) != 0 {
			// localPeerPods represents all the peer pods in the same
			// namespace from which we need to allow traffic.
			localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
				policy.Name, "ingress", i)

			hashedLocalAddressSet = hashedAddressSet(localPeerPods)
			oc.createAddressSet(localPeerPods, hashedLocalAddressSet, nil)
			ingress.addAddressSet(hashedLocalAddressSet)
		}

		for _, fromJSON := range ingressJSON.From {
			// Add IPBlock to ingress network policy
			if fromJSON.IPBlock != nil {
				ingress.addIPBlock(fromJSON.IPBlock)
			}
		}

		oc.localPodAddACL(np, ingress)

		for _, fromJSON := range ingressJSON.From {
			if fromJSON.NamespaceSelector != nil && fromJSON.PodSelector != nil {
				// For each rule that contains both peer namespace selector and
				// peer pod selector, we create a watcher for each matching namespace
				// that populates the addressSet
				oc.handlePeerNamespaceAndPodSelector(policy,
					fromJSON.NamespaceSelector, fromJSON.PodSelector,
					hashedLocalAddressSet, peerPodAddressMap, ingress, np)

			} else if fromJSON.NamespaceSelector != nil {
				// For each peer namespace selector, we create a watcher that
				// populates ingress.peerAddressSets
				oc.handlePeerNamespaceSelector(policy,
					fromJSON.NamespaceSelector, ingress, np)
			} else if fromJSON.PodSelector != nil {
				// For each peer pod selector, we create a watcher that
				// populates the addressSet
				oc.handlePeerPodSelector(policy, fromJSON.PodSelector,
					hashedLocalAddressSet, peerPodAddressMap, np)
			}
		}
		np.ingressPolicies = append(np.ingressPolicies, ingress)
	}

	// Go through each egress rule.  For each egress rule, create an
	// addressSet for the peer pods.
	for i, egressJSON := range policy.Spec.Egress {
		logrus.Debugf("Network policy egress is %+v", egressJSON)

		egress := newGressPolicy(knet.PolicyTypeEgress, i)

		// Each egress rule can have multiple ports to which we allow traffic.
		for _, portJSON := range egressJSON.Ports {
			egress.addPortPolicy(&portJSON)
		}

		hashedLocalAddressSet := ""
		// peerPodAddressMap represents the IP addresses of all the peer pods
		// for this egress.
		peerPodAddressMap := make(map[string]bool)
		if len(egressJSON.To) != 0 {
			// localPeerPods represents all the peer pods in the same
			// namespace to which we need to allow traffic.
			localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
				policy.Name, "egress", i)

			hashedLocalAddressSet = hashedAddressSet(localPeerPods)
			oc.createAddressSet(localPeerPods, hashedLocalAddressSet, nil)
			egress.addAddressSet(hashedLocalAddressSet)
		}

		for _, toJSON := range egressJSON.To {
			// Add IPBlock to egress network policy
			if toJSON.IPBlock != nil {
				egress.addIPBlock(toJSON.IPBlock)
			}
		}

		oc.localPodAddACL(np, egress)

		for _, toJSON := range egressJSON.To {
			if toJSON.NamespaceSelector != nil && toJSON.PodSelector != nil {
				// For each rule that contains both peer namespace selector and
				// peer pod selector, we create a watcher for each matching namespace
				// that populates the addressSet
				oc.handlePeerNamespaceAndPodSelector(policy,
					toJSON.NamespaceSelector, toJSON.PodSelector,
					hashedLocalAddressSet, peerPodAddressMap, egress, np)

			} else if toJSON.NamespaceSelector != nil {
				// For each peer namespace selector, we create a watcher that
				// populates egress.peerAddressSets
				go oc.handlePeerNamespaceSelector(policy,
					toJSON.NamespaceSelector, egress, np)
			} else if toJSON.PodSelector != nil {
				// For each peer pod selector, we create a watcher that
				// populates the addressSet
				oc.handlePeerPodSelector(policy, toJSON.PodSelector,
					hashedLocalAddressSet, peerPodAddressMap, np)
			}
		}
		np.egressPolicies = append(np.egressPolicies, egress)
	}

	oc.namespacePolicies[policy.Namespace][policy.Name] = np

	// For all the pods in the local namespace that this policy
	// effects, add them to the port group.
	oc.handleLocalPodSelector(policy, np)

	return
}

func (oc *Controller) deleteNetworkPolicyPortGroup(
	policy *knet.NetworkPolicy) {
	logrus.Infof("Deleting network policy %s in namespace %s",
		policy.Name, policy.Namespace)

	if oc.namespacePolicies[policy.Namespace] == nil ||
		oc.namespacePolicies[policy.Namespace][policy.Name] == nil {
		logrus.Errorf("Delete network policy %s in namespace %s "+
			"received without getting a create event",
			policy.Name, policy.Namespace)
		return
	}
	np := oc.namespacePolicies[policy.Namespace][policy.Name]

	np.Lock()
	defer np.Unlock()

	// Mark the policy as deleted.
	np.deleted = true

	// Go through each ingress rule.  For each ingress rule, delete the
	// addressSet for the local peer pods.
	for i := range np.ingressPolicies {
		localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
			policy.Name, "ingress", i)
		hashedAddressSet := hashedAddressSet(localPeerPods)
		oc.deleteAddressSet(hashedAddressSet)
	}
	// Go through each egress rule.  For each egress rule, delete the
	// addressSet for the local peer pods.
	for i := range np.egressPolicies {
		localPeerPods := fmt.Sprintf("%s.%s.%s.%d", policy.Namespace,
			policy.Name, "egress", i)
		hashedAddressSet := hashedAddressSet(localPeerPods)
		oc.deleteAddressSet(hashedAddressSet)
	}

	// We should now stop all the handlers go routines.
	oc.shutdownHandlers(np)

	for logicalPort := range np.localPods {
		oc.localPodDelDefaultDeny(policy, logicalPort)
	}

	// Delete the port group
	oc.deletePortGroup(np.portGroupName)

	oc.namespacePolicies[policy.Namespace][policy.Name] = nil

	return
}
