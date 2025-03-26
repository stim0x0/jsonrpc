package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionClientServer(t *testing.T) {
	// Create a server
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	l := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	srv := NewServerConnection(listener, l)
	defer func(srv *ServerConnection) {
		err := srv.Close()
		if err != nil {
			t.Logf("Error closing server: %v", err)
		}
	}(srv)

	serverAddr := listener.Addr().String()
	t.Logf("Server listening on %s", serverAddr)

	// Server connection handling
	srv.HandleCall("echo", func(c *Connection, req *Response, resp chan<- *Response) {
		resp <- &Response{
			ID:  req.ID,
			Res: req.Params,
		}
	})
	srv.HandleCall("slow", func(c *Connection, req *Response, resp chan<- *Response) {
		time.Sleep(200 * time.Millisecond)
		resp <- &Response{
			ID:  req.ID,
			Res: json.RawMessage(`"slow response"`),
		}
	})
	srv.HandleCall("error", func(c *Connection, req *Response, resp chan<- *Response) {
		resp <- &Response{
			ID:  req.ID,
			Err: "test error",
		}
	})

	notifications := []string{}
	srv.Handle("notification", func(c *Connection, params []byte) {
		var data []string
		err := json.Unmarshal(params, &data)
		if err == nil && len(data) > 0 {
			notifications = append(notifications, data[0])
		}
	})

	i := 3
	srv.HandleCall("subscribe", func(c *Connection, req *Response, resp chan<- *Response) {
		go func(n int) {
			for ii := range n {
				time.Sleep(100 * time.Millisecond)
				c.Notify(context.Background(), "subscription", fmt.Sprintf("subscription %d", n), strconv.Itoa(ii))
			}
		}(i)
		resp <- &Response{
			ID:  req.ID,
			Res: json.RawMessage(fmt.Sprintf(`"subscription %d"`, i)),
		}
		i += 2
	})

	// Ensure server is ready
	time.Sleep(100 * time.Millisecond)

	// Create client connection
	clientConn, err := NewClient("tcp", serverAddr, l)
	require.NoError(t, err)
	defer clientConn.Close()

	// Test concurrent method calls with different behaviors
	t.Run("Concurrent method calls", func(t *testing.T) {
		var wg sync.WaitGroup
		results := make(map[string]interface{})
		errors := make(map[string]error)
		var mu sync.Mutex

		// Helper to record results
		recordResult := func(method string, result json.RawMessage, err error) {
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errors[method] = err
			} else {
				var data interface{}
				json.Unmarshal(result, &data)
				results[method] = data
			}
		}

		// Run multiple concurrent calls
		for i := 0; i < 3; i++ {
			wg.Add(3) // 3 different methods

			// Echo method
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				data := fmt.Sprintf("echo data %d", id)
				res, err := clientConn.Call(ctx, "echo", data)
				recordResult(fmt.Sprintf("echo-%d", id), res, err)
			}(i)

			// Slow method
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				res, err := clientConn.Call(ctx, "slow")
				recordResult(fmt.Sprintf("slow-%d", id), res, err)
			}(i)

			// Error method
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				res, err := clientConn.Call(ctx, "error")
				recordResult(fmt.Sprintf("error-%d", id), res, err)
			}(i)
		}

		// Also send notifications concurrently
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer cancel()

				err := clientConn.Notify(ctx, "notification", fmt.Sprintf("notification %d", id))
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		// Verify results
		assert.Len(t, results, 6) // 3 echo calls + 3 slow calls should succeed
		assert.Len(t, errors, 3)  // 3 error calls should fail

		// Check echo results contain our sent data
		for i := 0; i < 3; i++ {
			echoKey := fmt.Sprintf("echo-%d", i)
			assert.Contains(t, results, echoKey)
			assert.Contains(t, results[echoKey], fmt.Sprintf("echo data %d", i))
		}

		// Check slow results
		for i := 0; i < 3; i++ {
			slowKey := fmt.Sprintf("slow-%d", i)
			assert.Contains(t, results, slowKey)
			assert.Equal(t, "slow response", results[slowKey])
		}

		// Check error results
		for i := 0; i < 3; i++ {
			errorKey := fmt.Sprintf("error-%d", i)
			assert.Contains(t, errors, errorKey)
			assert.Contains(t, errors[errorKey].Error(), "test error")
		}

		// Check notifications
		assert.Len(t, notifications, 5)
		for i := 0; i < 5; i++ {
			expected := fmt.Sprintf("notification %d", i)
			assert.Contains(t, notifications, expected)
		}
	})

	// Test async request handling
	t.Run("Asynchronous requests", func(t *testing.T) {
		var wg sync.WaitGroup
		wg.Add(1)

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		go func() {
			defer wg.Done()

			// Send async request
			respChan, id, err := clientConn.Send(ctx, "slow")
			require.NoError(t, err)
			require.NotEmpty(t, id)

			// Wait for response
			select {
			case resp := <-respChan:
				assert.NoError(t, resp.Error())
				assert.Equal(t, json.RawMessage(`"slow response"`), resp.Res)
			case <-time.After(500 * time.Millisecond):
				t.Error("Timeout waiting for response")
			}
		}()

		wg.Wait()
	})

	// Test request cancellation
	t.Run("Request cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())

		respChan, id, err := clientConn.Send(ctx, "slow")
		require.NoError(t, err)
		require.NotEmpty(t, id)

		// Cancel the request before it completes
		cancel()
		clientConn.DropPending(id)

		// Verify we get a cancellation response
		select {
		case resp := <-respChan:
			assert.Error(t, resp.Error())
			assert.Contains(t, resp.Error().Error(), "cancelled")
		case <-time.After(500 * time.Millisecond):
			t.Error("Timeout waiting for cancellation response")
		}
	})

	// Test subscription handling
	t.Run("Subscription", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		var serverNotification []struct{ subsName, msg string }
		var wg sync.WaitGroup
		wg.Add(8)
		clientConn.Handle("subscription", func(params []byte) {
			defer wg.Done()
			var data []string
			err := json.Unmarshal(params, &data)
			if err != nil || len(data) != 2 {
				t.Logf("Failed to unmarshal subscription params: %v", err)
				return
			}
			serverNotification = append(serverNotification, struct {
				subsName string
				msg      string
			}{
				subsName: data[0],
				msg:      data[1],
			})
		})

		resp, err := clientConn.Call(ctx, "subscribe")
		require.NoError(t, err)
		require.Equal(t, `"subscription 3"`, string(resp))

		resp, err = clientConn.Call(ctx, "subscribe")
		require.NoError(t, err)
		require.Equal(t, `"subscription 5"`, string(resp))

		wg.Wait()

		assert.Len(t, serverNotification, 8)
		for i := range 3 {
			assert.Contains(t, serverNotification, struct {
				subsName string
				msg      string
			}{
				subsName: "subscription 3",
				msg:      strconv.Itoa(i),
			})
		}
		for i := range 5 {
			assert.Contains(t, serverNotification, struct {
				subsName string
				msg      string
			}{
				subsName: "subscription 5",
				msg:      strconv.Itoa(i),
			})
		}
	})
}
