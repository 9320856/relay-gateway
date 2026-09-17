package protocol

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrUnknownModelPolicy   = errors.New("unknown model policy")
	ErrModelPolicyOperation = errors.New("model policy does not support operation")
)

// ModelPolicy is the deliberately narrow extension point for provider
// semantics that cannot be expressed by a declarative Profile. Policies only
// transform the already-merged request body and must return a new value.
type ModelPolicy interface {
	Apply(operation string, body any) (any, error)
}

var modelPolicies = struct {
	sync.RWMutex
	items map[string]ModelPolicy
}{items: map[string]ModelPolicy{}}

// RegisterModelPolicy registers a named policy for Profile compilation and
// execution. Names are case-insensitive and registration is process-local.
func RegisterModelPolicy(name string, policy ModelPolicy) error {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return errors.New("model policy name is required")
	}
	if policy == nil {
		return errors.New("model policy is required")
	}
	modelPolicies.Lock()
	defer modelPolicies.Unlock()
	if _, exists := modelPolicies.items[name]; exists {
		return fmt.Errorf("model policy %q is already registered", name)
	}
	modelPolicies.items[name] = policy
	return nil
}

// ResolveModelPolicy returns a registered policy. Empty names mean that the
// operation has no provider-specific transformation.
func ResolveModelPolicy(name string) (ModelPolicy, error) {
	name = strings.ToLower(strings.TrimSpace(name))
	if name == "" {
		return nil, nil
	}
	modelPolicies.RLock()
	policy, ok := modelPolicies.items[name]
	modelPolicies.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownModelPolicy, name)
	}
	return policy, nil
}

// ApplyModelPolicy applies a named policy to a request body. It is exported
// for protocol front-ends that preserve a non-JSON wire format (such as
// multipart/form-data) while still using the same Profile policy semantics.
// JSON-based executors call the internal helper directly.
func ApplyModelPolicy(operation, policy string, body any) (any, error) {
	return applyModelPolicy(operation, policy, body)
}

func applyModelPolicy(operation, name string, body any) (any, error) {
	policy, err := ResolveModelPolicy(name)
	if err != nil {
		return nil, err
	}
	if policy == nil {
		return body, nil
	}
	return policy.Apply(operation, body)
}
