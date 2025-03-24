package jsonrpc

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResponse(t *testing.T) {
	t.Run("Error", func(t *testing.T) {
		t.Run("returns nil when no error exists", func(t *testing.T) {
			resp := Response{}
			assert.Nil(t, resp.Error())
		})

		t.Run("returns error message when error exists", func(t *testing.T) {
			resp := Response{
				Err: "test error",
			}
			assert.Error(t, resp.Error())
			assert.Contains(t, resp.Error().Error(), "test error")
		})

		t.Run("handles different error types", func(t *testing.T) {
			errorTypes := []interface{}{
				"string error",
				map[string]interface{}{"code": 100, "message": "structured error"},
				123,
			}

			for _, errValue := range errorTypes {
				resp := Response{Err: errValue}
				assert.Error(t, resp.Error())
				assert.Contains(t, resp.Error().Error(), "received JSON-RPC error")
			}
		})
	})

	t.Run("IsNotification", func(t *testing.T) {
		t.Run("returns true for notifications (ID is nil)", func(t *testing.T) {
			resp := Response{
				ID:     nil,
				Method: "test_notification",
				Params: json.RawMessage(`["param1", "param2"]`),
			}
			assert.True(t, resp.IsNotification())
		})

		t.Run("returns false for responses (ID is not nil)", func(t *testing.T) {
			id := "123"
			resp := Response{
				ID:  &id,
				Res: json.RawMessage(`{"result": "success"}`),
			}
			assert.False(t, resp.IsNotification())
		})
	})

	t.Run("IsCall", func(t *testing.T) {
		t.Run("returns true for server calls", func(t *testing.T) {
			id := "123"
			resp := Response{
				ID:     &id,
				Method: "test_method",
				Params: json.RawMessage(`["param1", "param2"]`),
			}
			assert.True(t, resp.IsCall())
		})

		t.Run("returns false for responses", func(t *testing.T) {
			id := "123"
			resp := Response{
				ID:  &id,
				Res: json.RawMessage(`{"result": "success"}`),
			}
			assert.False(t, resp.IsCall())
		})

		t.Run("returns false for notifications", func(t *testing.T) {
			resp := Response{
				ID:     nil,
				Method: "test_notification",
				Params: json.RawMessage(`["param1", "param2"]`),
			}
			assert.False(t, resp.IsCall())
		})

		t.Run("returns false when missing required fields", func(t *testing.T) {
			id := "123"
			testCases := []struct {
				name     string
				response Response
			}{
				{
					name: "missing Method",
					response: Response{
						ID:     &id,
						Params: json.RawMessage(`["param1", "param2"]`),
					},
				},
				{
					name: "missing Params",
					response: Response{
						ID:     &id,
						Method: "test_method",
					},
				},
				{
					name: "contains Res field",
					response: Response{
						ID:     &id,
						Method: "test_method",
						Params: json.RawMessage(`["param1", "param2"]`),
						Res:    json.RawMessage(`{"result": "success"}`),
					},
				},
				{
					name: "contains Err field",
					response: Response{
						ID:     &id,
						Method: "test_method",
						Params: json.RawMessage(`["param1", "param2"]`),
						Err:    "some error",
					},
				},
			}

			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					assert.False(t, tc.response.IsCall())
				})
			}
		})
	})
}
