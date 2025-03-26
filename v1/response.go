package v1

import (
	"bytes"
	"errors"
)

func newResponse(id *string, res jsonValueType, err error) *jResponse {
	resp := jResponse{
		jRequest: jRequest{
			Id: []byte(*id),
		},
		Res: res,
	}
	if err != nil {
		resp.Err = jsonValueType(`"` + err.Error() + `"`)
	}
	return &resp
}

// jResponse can be JSON-RPC response, JSON-RPC request notification or server call
type jResponse struct {
	//Id *string `json:"id"` // Id Non-null for response; null for request notification.
	jRequest
	// used for requests
	Res jsonValueType `json:"result,omitempty"` // Res here is raw results from JSON-RPC server.
	Err jsonValueType `json:"error,omitempty"`
}

// getErr returns response error if any
func (r *jResponse) getErr() error {
	if r.Err != nil {
		// TODO: improve error handling
		return errors.New(string(r.Err))
	}
	return nil
}

func (r *jResponse) isCall() bool {
	return !bytes.Equal(r.Id, []byte("null")) && r.Method != ""
}

// IsNotification returns true if response is a notification
func (r *jResponse) isNotification() bool {
	return bytes.Equal(r.Id, []byte("null"))
}

func (r *jResponse) Error() []byte {
	return r.Err
}
func (r *jResponse) Result() []byte {
	return r.Res
}
