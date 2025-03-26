package v1

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewResponse(t *testing.T) {
	t.Run("response with result", func(t *testing.T) {
		id := "123"
		result := jsonValueType(`{"key":"value"}`)

		resp := newResponse(&id, result, nil)

		assert.Equal(t, &id, resp.Id)
		assert.Equal(t, result, resp.Res)
		assert.Nil(t, resp.Err)
	})

	t.Run("response with error", func(t *testing.T) {
		id := "456"
		err := errors.New("test error")

		resp := newResponse(&id, nil, err)

		assert.Equal(t, &id, resp.Id)
		assert.Nil(t, resp.Res)
		assert.Equal(t, jsonValueType(`"test error"`), resp.Err)
	})

	t.Run("notification response", func(t *testing.T) {
		var id *string = nil
		result := jsonValueType(`"notification"`)

		resp := newResponse(id, result, nil)

		assert.Nil(t, resp.Id)
		assert.Equal(t, result, resp.Res)
		assert.Nil(t, resp.Err)
	})
}

func TestJResponse_GetErr(t *testing.T) {
	t.Run("with error", func(t *testing.T) {
		resp := jResponse{
			Err: jsonValueType(`"something went wrong"`),
		}

		err := resp.getErr()

		assert.Error(t, err)
		assert.Equal(t, `"something went wrong"`, err.Error())
	})

	t.Run("without error", func(t *testing.T) {
		resp := jResponse{
			Res: jsonValueType(`{"status":"success"}`),
		}

		err := resp.getErr()

		assert.NoError(t, err)
		assert.Nil(t, err)
	})
}

func TestJResponse_IsCall(t *testing.T) {
	t.Run("is call", func(t *testing.T) {
		id := "call-123"
		resp := jResponse{
			jRequest: jRequest{
				Id:     jsonValueType(id),
				Method: "test.method",
				Params: []jsonValueType{jsonValueType(`42`)},
			},
		}

		assert.True(t, resp.isCall())
	})

	t.Run("is not call - missing method", func(t *testing.T) {
		id := "not-call-1"
		resp := jResponse{
			jRequest: jRequest{
				Id:     jsonValueType(id),
				Params: []jsonValueType{jsonValueType(`42`)},
			},
		}

		assert.False(t, resp.isCall())
	})

	t.Run("is not call - missing params", func(t *testing.T) {
		id := "not-call-2"
		resp := jResponse{
			jRequest: jRequest{
				Id:     jsonValueType(id),
				Method: "test.method",
			},
		}

		assert.False(t, resp.isCall())
	})

	t.Run("is not call - has result", func(t *testing.T) {
		id := "not-call-3"
		resp := jResponse{
			jRequest: jRequest{
				Id:     jsonValueType(id),
				Method: "test.method",
				Params: []jsonValueType{jsonValueType(`42`)},
			},
			Res: jsonValueType(`"some result"`),
		}

		assert.False(t, resp.isCall())
	})

	t.Run("is not call - has error", func(t *testing.T) {
		id := "not-call-4"
		resp := jResponse{
			jRequest: jRequest{
				Id:     jsonValueType(id),
				Method: "test.method",
				Params: []jsonValueType{jsonValueType(`42`)},
			},
			Err: jsonValueType(`"some error"`),
		}

		assert.False(t, resp.isCall())
	})
}

func TestJResponse_IsNotification(t *testing.T) {
	t.Run("is notification", func(t *testing.T) {
		resp := jResponse{
			jRequest: jRequest{
				Id:     nil,
				Method: "test.notification",
				Params: []jsonValueType{jsonValueType(`42`)},
			},
		}

		assert.True(t, resp.isNotification())
	})

	t.Run("is not notification", func(t *testing.T) {
		id := "123"
		resp := jResponse{
			jRequest: jRequest{
				Id:     jsonValueType(id),
				Method: "test.method",
				Params: []jsonValueType{jsonValueType(`42`)},
			},
		}

		assert.False(t, resp.isNotification())
	})
}

func TestJResponse_Error(t *testing.T) {
	t.Run("with error", func(t *testing.T) {
		errValue := jsonValueType(`"test error"`)
		resp := jResponse{
			Err: errValue,
		}

		assert.Equal(t, []byte(errValue), resp.Error())
	})

	t.Run("without error", func(t *testing.T) {
		resp := jResponse{
			Res: jsonValueType(`{"status":"success"}`),
		}

		assert.Nil(t, resp.Error())
	})
}

func TestJResponse_Result(t *testing.T) {
	t.Run("with result", func(t *testing.T) {
		resValue := jsonValueType(`{"status":"success"}`)
		resp := jResponse{
			Res: resValue,
		}

		assert.Equal(t, []byte(resValue), resp.Result())
	})

	t.Run("without result", func(t *testing.T) {
		resp := jResponse{
			Err: jsonValueType(`"test error"`),
		}

		assert.Nil(t, resp.Result())
	})
}
