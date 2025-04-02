package jrpc1

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
)

func newCallHandler(_fn any) (*callHandler, error) {
	fnType := reflect.TypeOf(_fn)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("not a function")
	}

	fn := reflect.ValueOf(_fn)
	name := runtime.FuncForPC(fn.Pointer()).Name()

	if fnType.NumOut() != 2 || fnType.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		return nil, errors.New(`"` + name + `": call handler must return error`)
	}

	var isVariadic bool
	if fnType.IsVariadic() {
		isVariadic = true
	}

	in := make([]reflect.Type, 0)
	usesConnection := false
	if fnType.NumIn() > 0 {
		start := 0
		if fnType.In(0) == reflect.TypeOf(&ClientConn{}) {
			usesConnection = true
			start = 1
		}

		for i := start; i < fnType.NumIn(); i++ {
			inType := fnType.In(i)
			if inType.Kind() == reflect.Interface {
				return nil, errors.New(`"` + name + `": interface type as call parameter not supported`)
			}
			in = append(in, inType)
		}
		if isVariadic {
			vType := in[len(in)-1].Elem()
			if vType.Kind() == reflect.Interface {
				return nil, errors.New(`"` + name + `": interface type as call parameter not supported`)
			}
			in[len(in)-1] = vType
		}
	}

	out := fnType.Out(0)
	if out.Kind() == reflect.Interface {
		return nil, errors.New(`"` + name + `": interface type as handler return value not supported`)
	}

	return &callHandler{
		name:       name,
		isVariadic: isVariadic,
		usesConn:   usesConnection,
		fn:         fn,
		ins:        in,
		out:        out,
	}, nil
}

type callHandler struct {
	name       string
	isVariadic bool
	usesConn   bool
	fn         reflect.Value
	ins        []reflect.Type
	out        reflect.Type
}

func (h *callHandler) fillArgs(c *ClientConn, args []jsonValueType) ([]reflect.Value, error) {
	var ins []reflect.Value
	if h.usesConn {
		ins = append(ins, reflect.ValueOf(c))
	}
	for i := range args {
		argV := reflect.New(h.ins[i])
		if err := json.Unmarshal(args[i], argV.Interface()); err != nil {
			return nil, fmt.Errorf("%q: argument #%d: %w", h.name, i, err)
		}
		ins = append(ins, argV.Elem())
	}
	end := len(h.ins)
	if h.isVariadic {
		end--
	}
	for i := len(ins); i < end; i++ {
		argV := reflect.New(h.ins[i])
		ins = append(ins, argV.Elem())
	}

	return ins, nil
}

func (h *callHandler) call(c *ClientConn, args []jsonValueType) (jsonValueType, error) {
	if len(args) > len(h.ins) && !h.isVariadic {
		return nil, fmt.Errorf("%q: too many arguments", h.name)
	}

	var vIn []reflect.Value
	// check if variadic args is of correct type
	if len(args) > len(h.ins) || len(args) == len(h.ins) && h.isVariadic {
		vStart := len(h.ins) - 1
		vType := h.ins[vStart]
		for i := vStart; i < len(args); i++ {
			argV := reflect.New(vType)
			if err := json.Unmarshal(args[i], argV.Interface()); err != nil {
				return nil, fmt.Errorf("%q: argument #%d: %w", h.name, i, err)
			}
			vIn = append(vIn, argV.Elem())
		}
		args = args[:vStart]
	}

	ins, err := h.fillArgs(c, args)
	if err != nil {
		return nil, err
	}

	ins = append(ins, vIn...)

	out := h.fn.Call(ins)
	if !out[1].IsNil() {
		return nil, out[1].Interface().(error)
	}

	resJSON, err := json.Marshal(out[0].Interface())
	if err != nil {
		return nil, fmt.Errorf("%q: marshal result: %w", h.name, err)
	}

	return resJSON, nil
}
