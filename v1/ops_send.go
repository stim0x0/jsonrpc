package v1

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"time"
)

func (c *ClientConn) doSendRequest(ctx context.Context, id, method string, req []byte) error {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()
	l := c.log.With(slog.String("id", id), slog.String("method", method))

	deadline, ok := ctx.Deadline()
	if !ok {
		ctx, _ = context.WithTimeout(ctx, c.defaultTimeout)
		deadline = time.Now().Add(c.defaultTimeout)
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		l.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	_, err := io.Copy(c.conn, bytes.NewReader(req))
	if err != nil {
		c.cancel()
		return err
	}

	l.Debug("sent request", slog.String("request", string(req)))
	return nil
}

// doSendCallResponse sends a JSON-RPC response back to the server for a received call.
// It sets a write deadline for the operation, encodes the response, and handles any errors
// that occur during sending.
//
// Parameters:
//   - c: The ClientConn instance that owns this call response
//   - act: The action containing call response details
//   - enc: JSON encoder to use for writing the response
//
// Returns error if the connection enters a failed state during response sending,
// signaling that the broker should terminate its event loop. Otherwise, returns true.
func (c *ClientConn) doSendResponse(id jsonValueType, method string, resp []byte) error {
	c.sendLock.Lock()
	defer c.sendLock.Unlock()

	deadline := time.Now().Add(c.defaultTimeout)
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}

	_, err := io.Copy(c.conn, bytes.NewReader(resp))
	if err != nil {
		c.cancel()
		return err
	}

	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		c.log.Debug("sent request",
			slog.String("id", string(id)),
			slog.String("method", method),
			slog.String("request", string(resp)))
	}
	return nil
}
