package v1

import (
	"encoding/json"
)

// jRequest represent JSON-RPC request.
// It is used to send requests over wire
type jRequest struct {
	Id     jsonValueType     `json:"id"`     // Id is request Id.
	Method string            `json:"method"` // Method name of method to call
	Params []json.RawMessage `json:"params"` // Params is method parameters array
}

// lRequest represent JSON-RPC request
//type lRequest struct {
//	*jRequest
//	res    chan<- Response
//	ctx    context.Context
//	cancel context.CancelCauseFunc
//err    error
//}

//func newRequest(_id uint64, _action *action) *lRequest {
//	var id *string = nil
//
//	if _action.action == requestAction {
//		_idS := strconv.FormatUint(_id, 10)
//		id = &_idS
//	}
//
//	return &lRequest{
//		jRequest: &jRequest{Id: id,
//			Method: _action.method,
//			Params: _action.params,
//		},
//		res: _action.respChan,
//	}
//}
