package registry

import (
	"context"
	"errors"
	"reflect"
)

type ActivityFunc func(ctx context.Context, input any) (any, error)

type Registry struct {
	workflows  map[string]reflect.Value
	activities map[string]ActivityFunc
}

func NewRegistry() *Registry {
	return &Registry{
		workflows:  make(map[string]reflect.Value),
		activities: make(map[string]ActivityFunc),
	}
}

func (r *Registry) RegisterWorkflow(name string, fn any) {
	r.workflows[name] = reflect.ValueOf(fn)
}

func (r *Registry) GetWorkflow(name string) (reflect.Value, error) {
	fn, ok := r.workflows[name]
	if !ok {
		return reflect.Value{}, errors.New("workflow not found")
	}
	return fn, nil
}

func (r *Registry) RegisterActivity(name string, fn ActivityFunc) {
	r.activities[name] = fn
}

func (r *Registry) GetActivity(name string) (ActivityFunc, error) {
	fn, ok := r.activities[name]
	if !ok {
		return nil, errors.New("activity not found")
	}
	return fn, nil
}
