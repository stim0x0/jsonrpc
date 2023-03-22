package jsonrpc

import (
	"encoding/json"
	"fmt"
)

// Response can be JSON-RPC response, or JSON-RPC request notification
type Response struct {
	ID *string `json:"id"` // ID Non-null for response; null for request notification.

	// used for requests
	Res json.RawMessage `json:"result,omitempty"` // Res here is raw results from JSON-RPC server.
	Err interface{}     `json:"error,omitempty"`

	// used for notifications
	Method string          `json:"method,omitempty"` // Method is method name notification.
	Params json.RawMessage `json:"params,omitempty"` // Params is notification object
}

// Err returns response error if any
func (r *Response) Error() error {
	if r.Err != nil {
		// TODO: improve error handling
		return fmt.Errorf("received JSON-RPC error: %#v", r.Err)
	}
	return nil
}

// IsNotification returns true if response is a notification
func (r *Response) IsNotification() bool {
	return r.ID == nil
}

func (r *Response) IsCall() bool {
	return r.ID != nil && r.Method != "" && r.Params != nil && r.Res == nil && r.Err == nil
}
