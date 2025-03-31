package v1

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerConn_NotificationHandler verifies that a notification sent from a client
// triggers the registered server notification handler.
func TestServerConn_NotificationHandler(t *testing.T) {
	// Start a TCP listener on an ephemeral port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// Create a ServerConn with a test logger.
	srv := NewServerConnection(ln, nil)
	// Use a channel to capture handler invocation.
	done := make(chan any, 1)

	// Register a notification handler.
	// The handler receives a ClientConn and the notification parameters.
	err = srv.HandleNotification("notify.test", func(c *ClientConn, param int) {
		// Expect a single parameter with an integer value.
		done <- param
	})
	require.NoError(t, err)

	// As a client, connect to the server.
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Create a ClientConn instance on the client side.
	client := NewConnection(conn, nil)

	// Give the server a moment to accept the connection and register handlers.
	time.Sleep(100 * time.Millisecond)

	// Send a notification from the client.
	ctx := context.Background()
	err = client.Notify(ctx, "notify.test", 42)
	require.NoError(t, err)

	// Wait for the server handler to be invoked.
	select {
	case val := <-done:
		// Check that the parameter received by the server handler is correct.
		assert.Equal(t, 42, val)
	case <-time.After(time.Second):
		t.Fatal("Notification handler was not invoked in time")
	}

	// Cleanup: close the client connection and server.
	_ = client.Close()
	_ = srv.Close()
}

// TestServerConn_CallHandler verifies that when the client calls a method,
// the server call handler is invoked and a correct response is returned.
func TestServerConn_CallHandler(t *testing.T) {
	// Start a TCP listener on an ephemeral port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	// Create a server connection.
	//logger := slog.New(slog.NewTextHandler(nil, nil))
	srv := NewServerConnection(ln, nil)

	// Register a call handler that processes the incoming call.
	// The handler receives (c *ClientConn, id string, params jsonValueType, respChan chan<- *jResponse)
	// and sends back a response containing the parameter multiplied by 10.
	err = srv.HandleCall("call.test", func(c *ClientConn, val int) (int, error) {
		return 10 * val, nil
	})
	require.NoError(t, err)

	// As a client, connect to the server.
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()

	// Create a ClientConn.
	client := NewConnection(conn, nil)

	// Give the server time to accept the connection and register handlers.
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	// Make a synchronous call from the client.
	resp, err := client.Call(ctx, "call.test", 7)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify that the response has no error.
	assert.Nil(t, resp.Error())

	// Expect the result to be 70 (7*10).
	var result int
	err = json.Unmarshal(resp.Result(), &result)
	require.NoError(t, err)
	assert.Equal(t, 70, result)

	// Cleanup.
	_ = client.Close()
	_ = srv.Close()
}

// TestServerConn_MultipleClients verifies that the ServerConn accepts multiple clients and handles
// notifications correctly.
func TestServerConn_MultipleClients(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	srv := NewServerConnection(ln, nil)
	defer srv.Close()

	var wg sync.WaitGroup
	mu := sync.Mutex{}
	received := []int{}

	// Register a notification handler that appends received integers.
	err = srv.HandleNotification("multiclient.notify", func(v int) {
		mu.Lock()
		received = append(received, v)
		mu.Unlock()
	})
	require.NoError(t, err)

	// Create 3 clients.
	numClients := 3
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go func(val int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", ln.Addr().String())
			require.NoError(t, err)
			defer conn.Close()

			client := NewConnection(conn, nil)
			time.Sleep(100 * time.Millisecond)
			err = client.Notify(context.Background(), "multiclient.notify", val)
			require.NoError(t, err)
			_ = client.Close()
		}(i + 100)
	}
	wg.Wait()

	// Allow some time for all notifications to be processed.
	time.Sleep(100 * time.Millisecond)

	// Verify that all expected values were received.
	assert.ElementsMatch(t, []int{100, 101, 102}, received)
}
