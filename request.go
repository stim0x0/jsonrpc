package jsonrpc

import (
	"strconv"
)

// request represent JSON-RPC request.
type request struct {
	ID     *string          `json:"id"`     // ID is request Id.
	Method string           `json:"method"` // Method name of method to call
	Params []any            `json:"params"` // Params is method parameters array
	res    chan<- *Response // res is channel to pass results of method invocation
}

func newRequest(_id uint64, _action *action) *request {
	if _action.params == nil {
		//need empty array (not nil) for ovsdb-server to reply
		_action.params = []any{}
	}
	var id *string = nil

	if _action.action == requestAction {
		_idS := strconv.FormatUint(_id, 10)
		id = &_idS
	}

	return &request{
		ID:     id,
		Method: _action.method,
		Params: _action.params,
		res:    _action.respChan,
	}
}
