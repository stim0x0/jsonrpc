package v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime"
)

func newNotificationHandler(_fn any) (*notificationHandler, error) {
	fnType := reflect.TypeOf(_fn)
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("not a function")
	}

	fn := reflect.ValueOf(_fn)
	name := runtime.FuncForPC(fn.Pointer()).Name()

	if fnType.NumOut() != 0 {
		return nil, errors.New(`"` + name + `": call handler must have no return value or return error`)
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

	return &notificationHandler{
		name:           name,
		isVariadic:     isVariadic,
		usesConnection: usesConnection,
		fn:             fn,
		ins:            in,
	}, nil
}

type notificationHandler struct {
	name           string
	isVariadic     bool
	usesConnection bool
	fn             reflect.Value
	ins            []reflect.Type
	//out        reflect.Type
}

func (h *notificationHandler) fillArgs(c *ClientConn, args []jsonValueType) ([]reflect.Value, error) {
	var ins []reflect.Value
	if h.usesConnection {
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

func (h *notificationHandler) call(c *ClientConn, args []jsonValueType) error {
	if len(args) > len(h.ins) && !h.isVariadic {
		return fmt.Errorf("%q: too many arguments", h.name)
	}

	var vIn []reflect.Value
	// check if variadic args is of correct type
	if len(args) > len(h.ins) || len(args) == len(h.ins) && h.isVariadic {
		vStart := len(h.ins) - 1
		vType := h.ins[vStart]
		for i := vStart; i < len(args); i++ {
			argV := reflect.New(vType)
			if err := json.Unmarshal(args[i], argV.Interface()); err != nil {
				return fmt.Errorf("%q: argument #%d: %w", h.name, i, err)
			}
			vIn = append(vIn, argV.Elem())
		}
		args = args[:vStart]
	}

	ins, err := h.fillArgs(c, args)
	if err != nil {
		return err
	}

	ins = append(ins, vIn...)

	h.fn.Call(ins)

	return nil
}
