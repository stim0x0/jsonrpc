package v1

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCallHandler(t *testing.T) {
	t.Run("valid functions", func(t *testing.T) {
		// Simple function
		h, err := newCallHandler(func(a int) (string, error) {
			return fmt.Sprintf("%d", a), nil
		})
		require.NoError(t, err)
		assert.NotNil(t, h)
		assert.Equal(t, 1, len(h.ins))

		// Function with complex types
		type testStruct struct {
			Field string
		}
		h, err = newCallHandler(func(s testStruct) (*testStruct, error) {
			return &testStruct{Field: s.Field + "!"}, nil
		})
		require.NoError(t, err)
		assert.NotNil(t, h)

		// Variadic function
		h, err = newCallHandler(func(prefix string, values ...int) (string, error) {
			return prefix, nil
		})
		require.NoError(t, err)
		assert.NotNil(t, h)
		assert.True(t, h.isVariadic)
	})

	t.Run("invalid functions", func(t *testing.T) {
		// Not a function
		h, err := newCallHandler(42)
		assert.Error(t, err)
		assert.Nil(t, h)
		assert.Contains(t, err.Error(), "not a function")

		// Wrong return types
		h, err = newCallHandler(func(a int) int {
			return a
		})
		assert.Error(t, err)
		assert.Nil(t, h)
		assert.Contains(t, err.Error(), "must return error")

		// Interface parameter
		h, err = newCallHandler(func(a interface{}) (string, error) {
			return "", nil
		})
		assert.Error(t, err)
		assert.Nil(t, h)
		assert.Contains(t, err.Error(), "interface type as call parameter not supported")

		// Interface return
		h, err = newCallHandler(func() (interface{}, error) {
			return nil, nil
		})
		assert.Error(t, err)
		assert.Nil(t, h)
		assert.Contains(t, err.Error(), "interface type as handler return value not supported")

		// Variadic with interface type
		h, err = newCallHandler(func(prefix string, args ...interface{}) (string, error) {
			return prefix, nil
		})
		assert.Error(t, err)
		assert.Nil(t, h)
		assert.Contains(t, err.Error(), "interface type as call parameter not supported")
	})
}

func basicCall(a, b int) (int, error) {
	return a + b, nil
}
func TestCallHandler_Call(t *testing.T) {
	t.Run("basic call", func(t *testing.T) {
		// Create handler for a simple function that adds two numbers
		h, err := newCallHandler(basicCall)
		require.NoError(t, err)

		// Call with correct arguments
		args := []json.RawMessage{
			json.RawMessage(`42`),
			json.RawMessage(`58`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`100`), res)
	})

	t.Run("too many arguments", func(t *testing.T) {
		h, err := newCallHandler(func(a int) (string, error) {
			return fmt.Sprintf("%d", a), nil
		})
		require.NoError(t, err)

		// Call with too many args
		args := []json.RawMessage{
			json.RawMessage(`42`),
			json.RawMessage(`58`),
		}
		res, err := h.call(nil, args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "too many arguments")
		assert.Nil(t, res)
	})

	t.Run("variadic calls", func(t *testing.T) {
		h, err := newCallHandler(func(prefix string, values ...int) (string, error) {
			result := prefix
			for _, v := range values {
				result += fmt.Sprintf("%d", v)
			}
			return result, nil
		})
		require.NoError(t, err)

		// No variadic args
		args := []json.RawMessage{
			json.RawMessage(`"test"`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"test"`), res)

		// With variadic args
		args = []json.RawMessage{
			json.RawMessage(`"test"`),
			json.RawMessage(`1`),
			json.RawMessage(`2`),
			json.RawMessage(`3`),
		}
		res, err = h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"test123"`), res)
	})

	t.Run("unmarshal error", func(t *testing.T) {
		h, err := newCallHandler(func(a int) (string, error) {
			return fmt.Sprintf("%d", a), nil
		})
		require.NoError(t, err)

		// Invalid JSON for argument
		args := []json.RawMessage{
			json.RawMessage(`"not an int"`),
		}
		res, err := h.call(nil, args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "argument #0")
		assert.Nil(t, res)
	})

	t.Run("function returns error", func(t *testing.T) {
		expectedErr := errors.New("test error")
		h, err := newCallHandler(func(a int) (string, error) {
			return "", expectedErr
		})
		require.NoError(t, err)

		args := []json.RawMessage{
			json.RawMessage(`42`),
		}
		res, err := h.call(nil, args)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, res)
	})

	t.Run("marshal error", func(t *testing.T) {
		// Create a function that returns a channel which can't be marshaled to JSON
		h, err := newCallHandler(func(a int) (chan int, error) {
			return make(chan int), nil
		})
		require.NoError(t, err)

		args := []json.RawMessage{
			json.RawMessage(`42`),
		}
		res, err := h.call(nil, args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "marshal result")
		assert.Nil(t, res)
	})

	t.Run("default values for missing arguments", func(t *testing.T) {
		calls := 0
		h, err := newCallHandler(func(a, b int) (string, error) {
			calls++
			return fmt.Sprintf("%d,%d", a, b), nil
		})
		require.NoError(t, err)

		// Call with fewer arguments
		args := []json.RawMessage{
			json.RawMessage(`42`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"42,0"`), res)
		assert.Equal(t, 1, calls)
	})

	t.Run("struct arguments", func(t *testing.T) {
		type person struct {
			Name string
			Age  int
		}
		h, err := newCallHandler(func(p person) (string, error) {
			return fmt.Sprintf("%s is %d", p.Name, p.Age), nil
		})
		require.NoError(t, err)

		args := []json.RawMessage{
			json.RawMessage(`{"Name":"Alice","Age":30}`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"Alice is 30"`), res)
	})

	t.Run("slice arguments", func(t *testing.T) {
		h, err := newCallHandler(func(nums []int) (int, error) {
			sum := 0
			for _, n := range nums {
				sum += n
			}
			return sum, nil
		})
		require.NoError(t, err)

		args := []json.RawMessage{
			json.RawMessage(`[1, 2, 3, 4, 5]`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`15`), res)

		// Empty slice
		args = []json.RawMessage{
			json.RawMessage(`[]`),
		}
		res, err = h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`0`), res)

		// Invalid JSON for slice
		args = []json.RawMessage{
			json.RawMessage(`"not a slice"`),
		}
		res, err = h.call(nil, args)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "argument #0")
		assert.Nil(t, res)
	})

	t.Run("pointer arguments", func(t *testing.T) {
		// Function that accepts a pointer argument
		h, err := newCallHandler(func(n *int) (string, error) {
			if n == nil {
				return "nil", nil
			}
			return fmt.Sprintf("*%d", *n), nil
		})
		require.NoError(t, err)

		// Valid pointer value
		args := []json.RawMessage{
			json.RawMessage(`42`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"*42"`), res)

		// Null (nil pointer)
		args = []json.RawMessage{
			json.RawMessage(`null`),
		}
		res, err = h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"nil"`), res)

		// Pointer to struct
		type config struct {
			Enabled bool
			Count   int
		}
		h, err = newCallHandler(func(cfg *config) (string, error) {
			if cfg == nil {
				return "no config", nil
			}
			return fmt.Sprintf("enabled=%t, count=%d", cfg.Enabled, cfg.Count), nil
		})
		require.NoError(t, err)

		args = []json.RawMessage{
			json.RawMessage(`{"Enabled":true,"Count":5}`),
		}
		res, err = h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"enabled=true, count=5"`), res)

		args = []json.RawMessage{
			json.RawMessage(`null`),
		}
		res, err = h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"no config"`), res)
	})

	t.Run("variadic pointer to struct with pointers, slices and maps", func(t *testing.T) {
		type Address struct {
			Street string
			City   string
		}

		type Config struct {
			Name    string
			Debug   bool
			Address *Address
		}

		type Item struct {
			ID       int
			Tags     []string
			Metadata map[string]interface{}
			RefCount *int
		}

		processFunc := func(cfg *Config, items ...*Item) (string, error) {
			if cfg == nil {
				return "no config", nil
			}

			result := fmt.Sprintf("Config: %s (debug=%t)", cfg.Name, cfg.Debug)
			if cfg.Address != nil {
				result += fmt.Sprintf(", Address: %s, %s", cfg.Address.Street, cfg.Address.City)
			}

			for i, item := range items {
				result += fmt.Sprintf("\nItem %d: ", i)
				if item == nil {
					result += "nil"
					continue
				}

				result += fmt.Sprintf("ID=%d, Tags=%v", item.ID, item.Tags)

				if item.Metadata != nil {
					result += fmt.Sprintf(", Metadata=%v", item.Metadata)
				}

				if item.RefCount != nil {
					result += fmt.Sprintf(", RefCount=%d", *item.RefCount)
				}
			}

			return result, nil
		}

		h, err := newCallHandler(processFunc)
		require.NoError(t, err)

		// Test with complex nested structures
		args := []json.RawMessage{
			json.RawMessage(`{
            	"Name": "TestConfig", 
            	"Debug": true,
            	"Address": {
            		"Street": "123 Main St",
            		"City": "Example City"
            	}
            }`),
			json.RawMessage(`{
				"ID": 1,
				"Tags": ["important", "urgent"],
				"Metadata": {"owner": "admin", "priority": 5},
				"RefCount": 42
			}`),
			json.RawMessage(`{
				"ID": 2,
				"Tags": ["normal"],
				"Metadata": {"owner": "user"}
			}`),
			json.RawMessage(`null`),
		}
		res, err := h.call(nil, args)
		require.NoError(t, err)

		expected := `"Config: TestConfig (debug=true), Address: 123 Main St, Example City` +
			`\nItem 0: ID=1, Tags=[important urgent], Metadata=map[owner:admin priority:5], RefCount=42` +
			`\nItem 1: ID=2, Tags=[normal], Metadata=map[owner:user]` +
			`\nItem 2: nil"`

		assert.JSONEq(t, expected, string(res))

		// Test with nil config and no items
		args = []json.RawMessage{
			json.RawMessage(`null`),
		}
		res, err = h.call(nil, args)
		require.NoError(t, err)
		assert.Equal(t, json.RawMessage(`"no config"`), res)
	})
}
