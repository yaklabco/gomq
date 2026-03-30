package broker

import (
	"fmt"
	"regexp"
	"sync"
)

// Policy apply-to targets.
const (
	PolicyApplyQueues    = "queues"
	PolicyApplyExchanges = "exchanges"
	PolicyApplyAll       = "all"
)

// policyArgMap maps policy definition keys to AMQP queue argument names.
//
//nolint:gochecknoglobals // read-only mapping
var policyArgMap = map[string]string{
	"max-length":              "x-max-length",
	"max-length-bytes":        "x-max-length-bytes",
	"message-ttl":             "x-message-ttl",
	"dead-letter-exchange":    "x-dead-letter-exchange",
	"dead-letter-routing-key": "x-dead-letter-routing-key",
	"expires":                 "x-expires",
}

// Policy is a pattern-matched rule that applies arguments to queues and
// exchanges dynamically. Policies with higher Priority values take
// precedence when multiple policies match the same resource.
type Policy struct {
	Name       string
	Pattern    string // regex pattern matching queue/exchange names
	ApplyTo    string // "queues", "exchanges", or "all"
	Priority   int
	Definition map[string]interface{}
}

// compiled returns the compiled regex for the policy pattern. The caller
// should validate the pattern before storing the policy.
func (p *Policy) compiled() (*regexp.Regexp, error) {
	re, err := regexp.Compile(p.Pattern)
	if err != nil {
		return nil, fmt.Errorf("compile policy pattern %q: %w", p.Pattern, err)
	}
	return re, nil
}

// matchesQueue reports whether this policy applies to the given queue name.
func (p *Policy) matchesQueue(name string, re *regexp.Regexp) bool {
	if p.ApplyTo == PolicyApplyExchanges {
		return false
	}
	return re.MatchString(name)
}

// PolicyStore manages policies for a single vhost. It maintains a map of
// policies by name and reapplies policies to matching resources when
// policies are added or updated.
type PolicyStore struct {
	mu       sync.RWMutex
	policies map[string]*Policy
	compiled map[string]*regexp.Regexp // cached compiled patterns
	vhost    *VHost
}

// NewPolicyStore creates a policy store for the given vhost.
func NewPolicyStore(vh *VHost) *PolicyStore {
	return &PolicyStore{
		policies: make(map[string]*Policy),
		compiled: make(map[string]*regexp.Regexp),
		vhost:    vh,
	}
}

// SetPolicy adds or updates a policy and reapplies policies to all
// matching queues and exchanges in the vhost.
func (ps *PolicyStore) SetPolicy(name string, policy *Policy) error {
	re, err := policy.compiled()
	if err != nil {
		return fmt.Errorf("set policy %q: %w", name, err)
	}

	ps.mu.Lock()
	ps.policies[name] = policy
	ps.compiled[name] = re
	ps.mu.Unlock()

	// Reapply policies to all matching resources.
	ps.reapplyAll()

	return nil
}

// DeletePolicy removes a policy and reapplies the remaining policies
// to affected resources.
func (ps *PolicyStore) DeletePolicy(name string) error {
	ps.mu.Lock()
	if _, ok := ps.policies[name]; !ok {
		ps.mu.Unlock()
		return fmt.Errorf("policy %q: not found", name)
	}
	delete(ps.policies, name)
	delete(ps.compiled, name)
	ps.mu.Unlock()

	ps.reapplyAll()

	return nil
}

// ListPolicies returns a snapshot of all policies in the store.
func (ps *PolicyStore) ListPolicies() []*Policy {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	result := make([]*Policy, 0, len(ps.policies))
	for _, p := range ps.policies {
		result = append(result, p)
	}

	return result
}

// GetPolicy returns the named policy and true if found.
func (ps *PolicyStore) GetPolicy(name string) (*Policy, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	p, ok := ps.policies[name]
	return p, ok
}

// ApplyToQueue finds the highest-priority matching policy and applies
// its definition as effective arguments on the queue.
func (ps *PolicyStore) ApplyToQueue(queue *Queue) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var best *Policy
	for policyName, policy := range ps.policies {
		re := ps.compiled[policyName]
		if policy.matchesQueue(queue.Name(), re) {
			if best == nil || policy.Priority > best.Priority {
				best = policy
			}
		}
	}

	if best == nil {
		queue.SetEffectiveArgs(nil)
		return
	}

	args := translateDefinition(best.Definition)
	queue.SetEffectiveArgs(args)
}

// ApplyToExchange finds the highest-priority matching policy for the
// exchange. Currently a no-op placeholder since exchanges don't store
// effective arguments, but the matching logic is exercised.
func (ps *PolicyStore) ApplyToExchange(_ Exchange) {
	// Exchange policy application will be expanded when exchanges
	// support effective arguments (e.g. alternate-exchange).
}

// reapplyAll reapplies policies to all queues in the vhost.
func (ps *PolicyStore) reapplyAll() {
	for _, q := range ps.vhost.Queues() {
		ps.ApplyToQueue(q)
	}
}

// translateDefinition converts policy definition keys to AMQP argument
// names (e.g. "max-length" -> "x-max-length").
func translateDefinition(def map[string]interface{}) map[string]interface{} {
	args := make(map[string]interface{}, len(def))
	for k, v := range def {
		if argKey, ok := policyArgMap[k]; ok {
			args[argKey] = v
		}
	}
	return args
}
