package v1

import (
	"bytes"
	"encoding/json"
	"errors"
)

// jResponse can be JSON-RPC response, JSON-RPC request notification or server call
type jResponse struct {
	//Id *string `json:"id"` // Id Non-null for response; null for request notification.
	jRequest
	// used for requests
	Res jsonValueType `json:"result,omitempty"` // Res here is raw results from JSON-RPC server.
	Err jsonValueType `json:"error,omitempty"`
}

func (r *jResponse) isCall() bool {
	return !bytes.Equal(r.Id, []byte("null")) && r.Method != ""
}

// IsNotification returns true if response is a notification
func (r *jResponse) isNotification() bool {
	return bytes.Equal(r.Id, []byte("null"))
}

func (r *jResponse) GetErr() []byte {
	return r.Err
}
func (r *jResponse) GetResult() []byte {
	return r.Res
}
func (r *jResponse) Error() error {
	if r.Err == nil {
		return nil
	}

	var str *string
	if err := json.Unmarshal(r.Err, &str); err == nil {
		if str == nil {
			return nil
		}
		return errors.New(*str)
	}
	var obj map[string]any
	if err := json.Unmarshal(r.Err, &obj); err == nil {
		if err, ok := obj["error"]; ok {
			if str, ok := err.(string); ok {
				return errors.New(str)
			}
		}
		if err, ok := obj["message"]; ok {
			if str, ok := err.(string); ok {
				return errors.New(str)
			}
		}
		if err, ok := obj["code"]; ok {
			if str, ok := err.(string); ok {
				return errors.New(str)
			}
		}
	}

	return errors.New(string(r.Err))
}
