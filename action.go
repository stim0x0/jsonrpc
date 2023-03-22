package jsonrpc

import "context"

type actionType int

const (
	requestAction actionType = iota
	responseAction
	notificationAction
	setNotificationHandlerAction
	setCallHandlerAction
	dropPendingRequestAction
)

type NotificationHandler func(params []byte)

type CallHandler func(response *Response, respChan chan<- *Response)

type action struct {
	action      actionType // 0 - call, 1 - set notify handler, 3 -send notification
	method      string
	params      []any
	idChan      chan<- string       // id is channel to pass request id
	respChan    chan<- *Response    // respChan is channel to pass results of method invocation
	ctx         context.Context     // ctx is request context
	handler     NotificationHandler // handler is a callback function to handle notification
	callHandler CallHandler         // callHandler is a callback function to handle call
	callResp    *Response           // callResp is a response to call
	hId         string              // notify handler id
}
