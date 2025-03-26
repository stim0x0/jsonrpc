package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockConn implements the net.Conn interface for testing
type mockConn struct {
	net.Conn
	writeFn    func(b []byte) (int, error)
	closeFn    func() error
	setDeadFn  func(t time.Time) error
	setReadFn  func(t time.Time) error
	setWriteFn func(t time.Time) error
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.writeFn != nil {
		return m.writeFn(b)
	}
	return len(b), nil
}

func (m *mockConn) Close() error {
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

func (m *mockConn) SetDeadline(t time.Time) error {
	if m.setDeadFn != nil {
		return m.setDeadFn(t)
	}
	return nil
}

func (m *mockConn) SetReadDeadline(t time.Time) error {
	if m.setReadFn != nil {
		return m.setReadFn(t)
	}
	return nil
}

func (m *mockConn) SetWriteDeadline(t time.Time) error {
	if m.setWriteFn != nil {
		return m.setWriteFn(t)
	}
	return nil
}

func TestNewConnection(t *testing.T) {
	// Save original net.Dial function to restore later
	origDial := netDial
	defer func() { netDial = origDial }()

	// Test successful connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	require.NotNil(t, conn)
	defer conn.Close()

	assert.NoError(t, conn.Error())

	// Test connection error
	expectedErr := errors.New("connection error")
	netDial = func(network, addr string) (net.Conn, error) {
		return nil, expectedErr
	}

	conn, err = NewClient("tcp", "localhost:1234", nil)
	assert.Error(t, err)
	assert.Nil(t, conn)
	assert.Contains(t, err.Error(), expectedErr.Error())
}

func TestConnectionError(t *testing.T) {
	// Test no error initially
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	conn := &Connection{
		conn: &rawConnection{Conn: client},
	}
	assert.NoError(t, conn.Error())

	// Test error when in failed state
	conn.conn.failState.Store(true)
	assert.Error(t, conn.Error())
	assert.Contains(t, conn.Error().Error(), "failed connection")
}

func TestConnectionClose(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)

	err = conn.Close()
	assert.NoError(t, err)

	// Verify connection is closed by trying to write to it
	_, err = client.Write([]byte("test"))
	assert.Error(t, err)
}

func TestConnectionCall(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Set up server-side handler for successful call
	go func() {
		decoder := json.NewDecoder(server)
		var request request
		err := decoder.Decode(&request)
		require.NoError(t, err)

		// Verify request
		assert.Equal(t, "test_method", request.Method)
		assert.Equal(t, []any{"param1", "param2"}, request.Params)
		assert.NotNil(t, request.ID)

		// Send response
		response := Response{
			ID:  request.ID,
			Res: json.RawMessage(`"success"`),
		}
		encoder := json.NewEncoder(server)
		err = encoder.Encode(response)
		require.NoError(t, err)
	}()

	// Make the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := conn.Call(ctx, "test_method", "param1", "param2")
	require.NoError(t, err)
	assert.Equal(t, json.RawMessage(`"success"`), result)
}

func TestConnectionCallError(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Set up server-side handler for error response
	go func() {
		decoder := json.NewDecoder(server)
		var request request
		err := decoder.Decode(&request)
		require.NoError(t, err)

		// Send error response
		response := Response{
			ID:  request.ID,
			Err: "test error",
		}
		encoder := json.NewEncoder(server)
		err = encoder.Encode(response)
		require.NoError(t, err)
	}()

	// Make the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := conn.Call(ctx, "test_method", "param")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "test error")
	assert.Nil(t, result)
}

func TestConnectionCallTimeout(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Set up server-side handler that never responds
	go func() {
		decoder := json.NewDecoder(server)
		var request request
		_ = decoder.Decode(&request)
		// Don't respond, let the context timeout
	}()

	// Make the call with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := conn.Call(ctx, "test_method")
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Nil(t, result)
}

func TestConnectionNotify(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Set up server-side handler
	go func() {
		decoder := json.NewDecoder(server)
		var request request
		err := decoder.Decode(&request)
		require.NoError(t, err)

		// Verify notification
		assert.Equal(t, "test_notification", request.Method)
		assert.Equal(t, []any{"event_data"}, request.Params)
		assert.Nil(t, request.ID)
	}()

	// Send notification
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err = conn.Notify(ctx, "test_notification", "event_data")
	assert.NoError(t, err)
}

func TestConnectionHandle(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Setup notification channel
	receivedCh := make(chan []byte, 1)
	err = conn.Handle("test_event", func(params []byte) {
		receivedCh <- params
	})
	require.NoError(t, err)

	// Send notification from server
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure handler is registered
		notification := Response{
			Method: "test_event",
			Params: json.RawMessage(`["notification_data"]`),
		}
		encoder := json.NewEncoder(server)
		err := encoder.Encode(notification)
		require.NoError(t, err)
	}()

	// Wait for notification
	select {
	case params := <-receivedCh:
		assert.Equal(t, []byte(`["notification_data"]`), params)
	case <-time.After(time.Second):
		t.Fatal("Notification not received within timeout")
	}
}

func TestConnectionHandleCall(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Setup call handler
	err = conn.HandleCall("echo", func(req *Response, resp chan<- *Response) {
		// Echo back the request ID with a result
		resp <- &Response{
			ID:  req.ID,
			Res: json.RawMessage(`"echoed"`),
		}
	})
	require.NoError(t, err)

	// Send call from server and verify response
	go func() {
		time.Sleep(100 * time.Millisecond) // Small delay to ensure handler is registered
		id := "server_call_123"
		call := Response{
			ID:     &id,
			Method: "echo",
			Params: json.RawMessage(`["test"]`),
		}
		encoder := json.NewEncoder(server)
		err := encoder.Encode(call)
		require.NoError(t, err)

		// Read response
		decoder := json.NewDecoder(server)
		var response Response
		err = decoder.Decode(&response)
		require.NoError(t, err)

		// Verify response
		assert.Equal(t, id, *response.ID)
		assert.Equal(t, json.RawMessage(`"echoed"`), response.Res)
	}()

	// Allow time for the call to complete
	time.Sleep(time.Second)
}

func TestConnectionSendAndDrop(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	origDial := netDial
	defer func() { netDial = origDial }()
	netDial = func(network, addr string) (net.Conn, error) {
		return client, nil
	}

	conn, err := NewClient("tcp", "localhost:1234", nil)
	require.NoError(t, err)
	defer conn.Close()

	// Capture request without responding
	go func() {
		decoder := json.NewDecoder(server)
		var request request
		_ = decoder.Decode(&request)
		// Don't respond, let the test drop the request
	}()

	// Send asynchronous request
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	respChan, id, err := conn.Send(ctx, "test_method", "param")
	require.NoError(t, err)
	require.NotEmpty(t, id)

	// Drop the pending request
	conn.DropPending(id)

	// Check that we get a cancellation response
	select {
	case resp := <-respChan:
		assert.Error(t, resp.Error())
		assert.Contains(t, resp.Error().Error(), "cancelled")
	case <-time.After(time.Second):
		t.Fatal("No response received after dropping the request")
	}
}

func TestRawConnectionWrite(t *testing.T) {
	// Test incomplete write with timeout
	mockNetConn := &mockConn{
		writeFn: func(b []byte) (int, error) {
			// Return partial write with deadline error
			return 1, os.ErrDeadlineExceeded
		},
	}

	rawConn := &rawConnection{Conn: mockNetConn}
	_, err := rawConn.Write([]byte("test data"))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete write")
	assert.True(t, rawConn.failState.Load())

	// Test complete write
	mockNetConn = &mockConn{
		writeFn: func(b []byte) (int, error) {
			// Return complete write
			return len(b), nil
		},
	}

	rawConn = &rawConnection{Conn: mockNetConn}
	n, err := rawConn.Write([]byte("test data"))
	assert.NoError(t, err)
	assert.Equal(t, len("test data"), n)
	assert.False(t, rawConn.failState.Load())
}

/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////

//type mockConn struct {
//	writeFn    func(b []byte) (int, error)
//	closeFn    func() error
//	readFn     func(b []byte) (int, error)
//	localAddr  net.Addr
//	remoteAddr net.Addr
//}
//
//func (m *mockConn) Read(b []byte) (int, error) {
//	if m.readFn != nil {
//		return m.readFn(b)
//	}
//	return 0, io.EOF
//}
//
//func (m *mockConn) Write(b []byte) (int, error) {
//	if m.writeFn != nil {
//		return m.writeFn(b)
//	}
//	return len(b), nil
//}
//
//func (m *mockConn) Close() error {
//	if m.closeFn != nil {
//		return m.closeFn()
//	}
//	return nil
//}
//
//func (m *mockConn) LocalAddr() net.Addr {
//	return m.localAddr
//}
//
//func (m *mockConn) RemoteAddr() net.Addr {
//	return m.remoteAddr
//}
//
//func (m *mockConn) SetDeadline(t time.Time) error {
//	return nil
//}
//
//func (m *mockConn) SetReadDeadline(t time.Time) error {
//	return nil
//}
//
//func (m *mockConn) SetWriteDeadline(t time.Time) error {
//	return nil
//}

func TestRawConnection(t *testing.T) {
	t.Run("Write with complete data", func(t *testing.T) {
		mock := &mockConn{
			writeFn: func(b []byte) (int, error) {
				return len(b), nil
			},
		}

		conn := &rawConnection{Conn: mock}
		testData := []byte("test data")

		n, err := conn.Write(testData)

		assert.NoError(t, err)
		assert.Equal(t, len(testData), n)
		assert.False(t, conn.failState.Load(), "failState should not be set on successful write")
	})

	t.Run("Write with normal error", func(t *testing.T) {
		expectedErr := errors.New("network error")
		mock := &mockConn{
			writeFn: func(b []byte) (int, error) {
				return 0, expectedErr
			},
		}

		conn := &rawConnection{Conn: mock}
		testData := []byte("test data")

		n, err := conn.Write(testData)

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Equal(t, 0, n)
		assert.False(t, conn.failState.Load(), "failState should not be set on normal error")
	})

	t.Run("Write with deadline error and no bytes written", func(t *testing.T) {
		mock := &mockConn{
			writeFn: func(b []byte) (int, error) {
				return 0, os.ErrDeadlineExceeded
			},
		}

		conn := &rawConnection{Conn: mock}
		testData := []byte("test data")

		n, err := conn.Write(testData)

		assert.Error(t, err)
		assert.Equal(t, os.ErrDeadlineExceeded, err)
		assert.Equal(t, 0, n)
		assert.False(t, conn.failState.Load(), "failState should not be set when no data written")
	})

	t.Run("Write with deadline error and all bytes written", func(t *testing.T) {
		// This case doesn't make much sense in reality (error but all bytes written),
		// but we test it for completeness
		testData := []byte("test data")
		mock := &mockConn{
			writeFn: func(b []byte) (int, error) {
				return len(b), os.ErrDeadlineExceeded
			},
		}

		conn := &rawConnection{Conn: mock}

		n, err := conn.Write(testData)

		assert.Error(t, err)
		assert.Equal(t, os.ErrDeadlineExceeded, err)
		assert.Equal(t, len(testData), n)
		assert.False(t, conn.failState.Load(), "failState should not be set when all data written")
	})

	t.Run("Write with deadline error and partial write", func(t *testing.T) {
		testData := []byte("test data")
		mock := &mockConn{
			writeFn: func(b []byte) (int, error) {
				return 4, os.ErrDeadlineExceeded // Write only "test" of "test data"
			},
		}

		conn := &rawConnection{Conn: mock}

		n, err := conn.Write(testData)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "incomplete write")
		assert.ErrorIs(t, err, os.ErrDeadlineExceeded)
		assert.Equal(t, 4, n)
		assert.True(t, conn.failState.Load(), "failState should be set on partial write with deadline error")
	})

	t.Run("Check error propagation after failState is set", func(t *testing.T) {
		mock := &mockConn{}
		conn := &rawConnection{Conn: mock}

		// Manually set the fail state
		conn.failState.Store(true)

		// Next write should still use the underlying connection but keep the fail state
		testData := []byte("more data")
		mock.writeFn = func(b []byte) (int, error) {
			return len(b), nil
		}

		n, err := conn.Write(testData)

		assert.NoError(t, err)
		assert.Equal(t, len(testData), n)
		assert.True(t, conn.failState.Load(), "failState should remain set after subsequent successful writes")
	})
}
