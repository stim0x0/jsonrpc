package v1

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource:   true,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	})))

	m.Run()
}

// pipeConnPair creates a pair of connected net.Conn objects using net.Pipe
type pipeConnPair struct {
	client net.Conn
	server net.Conn
}

func newPipeConnPair() *pipeConnPair {
	client, server := net.Pipe()
	return &pipeConnPair{client: client, server: server}
}

func (p *pipeConnPair) Close() {
	p.client.Close()
	p.server.Close()
}

// readJSON reads a complete JSON message from the connection
func readJSON(conn net.Conn, timeout time.Duration) (map[string]interface{}, error) {
	if timeout > 0 {
		err := conn.SetReadDeadline(time.Now().Add(timeout))
		if err != nil {
			return nil, err
		}
		defer conn.SetReadDeadline(time.Time{})
	}

	var msg map[string]interface{}
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return nil, err
	}
	return msg, nil
}

// writeJSON writes a JSON message to the connection
func writeJSON(conn net.Conn, data interface{}) error {
	return json.NewEncoder(conn).Encode(data)
}

func TestNewConnection(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	conn := NewConnection(pipes.client, nil)

	require.NotNil(t, conn)
	assert.NotNil(t, conn.log)
	assert.NotNil(t, conn.ctx)
	assert.NotNil(t, conn.cancel)
	assert.NotNil(t, conn.notificationHandlers)
	assert.NotNil(t, conn.callHandlers)
	assert.NotNil(t, conn.pendingRequests)
	assert.NotNil(t, conn.reqId)
}

func TestClientConn_Notify(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Read notification from server side
		msg, err := readJSON(pipes.server, time.Second)
		if err != nil {
			pipes.Close()
			t.Error("Server error reading message:", err)
			return
		}

		assert.Nil(t, msg["id"])
		assert.Equal(t, "test.notification", msg["method"])

		params, ok := msg["params"].([]interface{})
		require.True(t, ok, "params should be an array")
		assert.Equal(t, float64(42), params[0])
		assert.Equal(t, "string param", params[1])
	}()
	// Test sending notification
	client := NewConnection(pipes.client, nil)
	ctx := context.Background()
	err := client.Notify(ctx, "test.notification", 42, "string param")
	require.NoError(t, err)
	wg.Wait()
}

func TestClientConn_Send(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Read request from server side
		msg, err := readJSON(pipes.server, time.Second)
		if err != nil {
			pipes.Close()
			t.Error("Server error reading message:", err)
			return
		}

		id := msg["id"].(string)

		assert.Equal(t, "1", msg["id"])
		assert.Equal(t, "test.method", msg["method"])

		params, ok := msg["params"].([]interface{})
		require.True(t, ok, "params should be an array")
		assert.Equal(t, float64(42), params[0])
		assert.Equal(t, "string param", params[1])

		// Send response back from server
		responseMsg := map[string]interface{}{
			"id":     id,
			"result": map[string]string{"status": "success"},
			"error":  nil,
		}

		err = writeJSON(pipes.server, responseMsg)
		require.NoError(t, err)

	}()

	// Test sending request
	client := NewConnection(pipes.client, nil)
	ctx := context.Background()
	respChan, err := client.Send(ctx, "test.method", 42, "string param")
	require.NoError(t, err)

	wg.Wait()

	// Verify client receives the response
	select {
	case resp := <-respChan:
		assert.NotNil(t, resp)
		assert.Nil(t, resp.GetErr())

		var result map[string]string
		err = json.Unmarshal(resp.GetResult(), &result)
		require.NoError(t, err)
		assert.Equal(t, "success", result["status"])

	case <-time.After(time.Second):
		t.Fatal("timeout waiting for response")
	}
}

func TestClientConn_SendTimeout(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// Read 1st request from server side don't answer
		msg, err := readJSON(pipes.server, time.Second)
		if err != nil {
			pipes.Close()
			t.Error("Server error reading message:", err)
			return
		}

		assert.Equal(t, "1", msg["id"])
		assert.Equal(t, "test.slow", msg["method"])

		params, ok := msg["params"].([]interface{})
		require.True(t, ok, "params should be an array")
		assert.Equal(t, "param", params[0])

		// Read another request from the server side
		msg, err = readJSON(pipes.server, 10*time.Second)
		if err != nil {
			t.Errorf("Server error reading second message: %v", err)
			return
		}

		// Send response back
		responseMsg := map[string]interface{}{
			"id":     msg["id"],
			"result": "success after timeout",
			"error":  nil,
		}
		err = writeJSON(pipes.server, responseMsg)
		if err != nil {
			t.Errorf("Server error writing response: %v", err)
		}
	}()

	client := NewConnection(pipes.client, nil, WithDefaultTimeout(500*time.Millisecond))

	// Use a short timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Send request that will timeout
	respChan, err := client.Send(ctx, "test.slow", "param")
	require.NoError(t, err)

	// Don't read from the server side to simulate a slow/hung server

	// Verify operation times out
	select {
	case resp := <-respChan:
		t.Fatalf("Should not have received response: %v", resp)
	case <-time.After(1 * time.Second):
		// Expected behavior - no response received
		// We would check the request is removed from pendingRequests, but that's an implementation detail
	}

	// Verify the client-side context is properly handled
	select {
	case <-ctx.Done():
		assert.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	default:
		t.Fatal("Context should be canceled due to timeout")
	}

	// Verify that we can still use the connection for other operations
	// after a timeout occurred (timeout shouldn't close the connection)

	// Send another request - this should work
	newCtx := context.Background()
	resp, err := client.Call(newCtx, "test.afterTimeout", "param")
	require.NoError(t, err)
	assert.NotNil(t, resp)

	var result string
	err = json.Unmarshal(resp.GetResult(), &result)
	require.NoError(t, err)
	assert.Equal(t, "success after timeout", result)

	wg.Wait()
}

func TestClientConn_Call(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	var wg sync.WaitGroup
	wg.Add(1)
	// Start a goroutine to handle the server side
	go func() {
		defer wg.Done()
		// Read request
		msg, err := readJSON(pipes.server, time.Second)
		if err != nil {
			t.Error("Server error reading message:", err)
			return
		}

		// Send response
		responseMsg := map[string]interface{}{
			"id":     msg["id"],
			"result": "call result",
			"error":  nil,
		}

		err = writeJSON(pipes.server, responseMsg)
		if err != nil {
			t.Error("Server error writing response:", err)
		}
	}()

	// Make synchronous call
	client := NewConnection(pipes.client, nil)
	ctx := context.Background()
	resp, err := client.Call(ctx, "test.call", "param1", 123)

	require.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Nil(t, resp.GetErr())

	var result string
	err = json.Unmarshal(resp.GetResult(), &result)
	require.NoError(t, err)
	assert.Equal(t, "call result", result)
}

func TestClientConn_CallCanceled(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	// Make synchronous call
	client := NewConnection(pipes.client, nil)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resp, err := client.Call(ctx, "test.call", "param1", 123)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, resp)
}

func TestClientConn_HandleNotification(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	client := NewConnection(pipes.client, nil)

	// Set up notification handler
	receivedValue := 0
	var wg sync.WaitGroup
	wg.Add(1)

	err := client.HandleNotification("test.event", func(value int) {
		receivedValue = value
		wg.Done()
	})
	require.NoError(t, err)

	// Send notification from server
	notificationMsg := map[string]interface{}{
		"id":     nil,
		"method": "test.event",
		"params": []interface{}{42},
	}

	err = writeJSON(pipes.server, notificationMsg)
	require.NoError(t, err)

	// Wait for notification handler to be called
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		assert.Equal(t, 42, receivedValue)
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for notification handler")
	}
}

func TestClientConn_HandleCall(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	client := NewConnection(pipes.client, nil)

	// Register call handler
	err := client.HandleCall("test.add", func(a, b int) (int, error) {
		return a + b, nil
	})
	require.NoError(t, err)

	// Send call from server
	callMsg := map[string]interface{}{
		"id":     "server-123",
		"method": "test.add",
		"params": []interface{}{23, 19},
	}

	err = writeJSON(pipes.server, callMsg)
	require.NoError(t, err)

	// Read response from client
	msg, err := readJSON(pipes.server, 100*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, "server-123", msg["id"])
	assert.Equal(t, float64(42), msg["result"])
	assert.Nil(t, msg["error"])
}

func TestClientConn_HandleCall_Error(t *testing.T) {
	pipes := newPipeConnPair()
	defer pipes.Close()

	client := NewConnection(pipes.client, nil)

	// Register call handler that returns error
	err := client.HandleCall("test.divide", func(a, b int) (int, error) {
		if b == 0 {
			return 0, errors.New("division by zero")
		}
		return a / b, nil
	})
	require.NoError(t, err)

	// Send call from server that will cause error
	callMsg := map[string]interface{}{
		"id":     "err-123",
		"method": "test.divide",
		"params": []interface{}{42, 0},
	}

	err = writeJSON(pipes.server, callMsg)
	require.NoError(t, err)

	// Read error response from client
	msg, err := readJSON(pipes.server, 100*time.Millisecond)
	require.NoError(t, err)

	assert.Equal(t, "err-123", msg["id"])
	assert.Nil(t, msg["result"])
	assert.Equal(t, "division by zero", msg["error"])
}

func TestClientConn_LifecycleAndConcurrency(t *testing.T) {
	pipes := newPipeConnPair()
	client := NewConnection(pipes.client, nil)

	// Check that Done() returns a valid channel
	select {
	case <-client.Done():
		t.Fatal("connection should not be done yet")
	default:
		// Expected behavior
	}

	// Test multiple concurrent calls
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Server handler for this request
			go func() {
				msg, err := readJSON(pipes.server, 100*time.Millisecond)
				if err != nil {
					return
				}

				// Echo back the params as result
				responseMsg := map[string]interface{}{
					"id":     msg["id"],
					"result": msg["params"],
					"error":  nil,
				}

				writeJSON(pipes.server, responseMsg)
			}()

			ctx := context.Background()
			resp, err := client.Call(ctx, "test.echo", i)
			if err != nil {
				t.Errorf("Call error: %v", err)
				return
			}

			var result []interface{}
			err = json.Unmarshal(resp.GetResult(), &result)
			if err != nil {
				t.Errorf("Unmarshal error: %v", err)
				return
			}

			assert.Equal(t, float64(i), result[0])
		}(i)
	}

	wg.Wait()

	// Close the connection and verify Done channel is closed
	err := client.Close()
	require.NoError(t, err)
	pipes.Close()

	select {
	case <-client.Done():
		// Expected behavior
	case <-time.After(time.Second):
		t.Fatal("Done channel not closed after connection Close")
	}
}
