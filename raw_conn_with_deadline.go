package jsonrpc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
)

// rawConnection wraps net.Conn and adds error state.
// The structure tracks connection status, marking it as problematic when incomplete data write occurs.
type rawConnection struct {
	net.Conn              // Встроенный интерфейс net.Conn для сетевых операций
	failState atomic.Bool // Атомарный флаг, указывающий на наличие ошибки при записи
}

// Write performs data writing to the connection and checks for errors.
//  1. Delegates the data write operation to the embedded connection c.Conn
//  2. Tracks a specific situation – an incomplete write when a timeout expires
//
// If a timeout error occurs (os.ErrDeadlineExceeded) and only part of the data is written
// (not all and not zero bytes), then:
//  1. The connection is marked as problematic using the atomic flag failState
//  2. The returned error is wrapped with additional context "incomplete write"
//
// This mechanism allows tracking the connection status and detecting situations
// when it is impossible to ensure the integrity of the transmitted data, which is critical for the JSON-RPC protocol.
func (c *rawConnection) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if errors.Is(err, os.ErrDeadlineExceeded) && n < len(b) && n != 0 {
		c.failState.Store(true)
		err = fmt.Errorf("incomplete write: %w", err)
	}
	return n, err
}
