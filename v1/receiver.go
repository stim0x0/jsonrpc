package v1

import (
	"encoding/json"
	"log/slog"
)

// receiver is a goroutine that processes incoming JSON-RPC messages from the connection.
//
// It continuously reads from the connection, decodes messages, and routes them to the appropriate channel
// based on message type:
//   - Responses to client requests go to responseChan
//   - Server notifications go to notificationChan
//   - Server calls go to callChan
//
// The receiver handles error conditions during message reading and decoding:
//   - For fatal errors (EOF, deadline exceeded with connection failure, closed connection),
//     it marks the connection as failed and terminates
//   - For non-fatal decoding errors, it logs the error and continues processing
//
// When the receiver terminates (due to connection errors), it properly closes all output channels
// and decrements the connection's wait group counter.
// Parameters:
//   - c: The ClientConn instance that owns this receiver
//   - responseChan: Channel for sending received responses to pending client requests
//   - notificationChan: Channel for sending received server notifications
//   - callChan: Channel for sending received server calls
func (c *ClientConn) receiver() {
	defer c.wg.Done()
	defer c.log.Debug("receiver closed")
	defer c.cancel()
	c.log.Debug("receiver started")

	dec := json.NewDecoder(c.conn)

	for {
		var resp jResponse
		if err := dec.Decode(&resp); err != nil {
			//if errors.Is(err, io.EOF) ||
			//	(errors.Is(err, os.ErrDeadlineExceeded) && c.conn.failState.Load()) ||
			//	strings.Contains(err.Error(), "use of closed network connection") ||
			//	strings.Contains(err.Error(), "io: read/write on closed pipe") {
			//	c.log.Debug("broken connection", slog.String("error", err.Error()))
			//	c.conn.failState.Store(true)
			//	return
			//}
			_ = c.conn.Close()
			select {
			case <-c.ctx.Done():
				return
			default:
			}
			//c.conn.failState.Store(true)
			c.log.Error("error on receive", slog.String("error", err.Error()))
			//if err := c.conn.Close(); err != nil {
			//	c.log.Warn("fail to close connection", slog.String("error", err.Error()))
			//}
			return
		}

		//if resp.Err != nil && bytes.Equal(resp.Err, []byte("null")) {
		//	resp.Err = nil
		//}
		if resp.isNotification() {
			// notification
			go c.dispatchNotification(&resp.jRequest)
			continue
		}
		if resp.isCall() {
			// call
			go c.dispatchCall(&resp.jRequest)
			continue
		}
		// response
		go c.dispatchResponse(&resp)
	}
}

func (c *ClientConn) dispatchNotification(req *jRequest) {
	l := c.log.With(slog.String("method", req.Method))
	l.Debug("notification handler called", slog.Any("params", req.Params))

	c.handlersTablesLock.RLock()
	handler, ok := c.notificationHandlers[req.Method]
	c.handlersTablesLock.RUnlock()
	if !ok {
		l.Warn("notification handler not found")
		return
	}

	if err := handler.call(c, req.Params); err != nil {
		l.Error("notification handler error", slog.String("error", err.Error()))
	}

	l.Debug("notification handler finished")
}

func (c *ClientConn) dispatchCall(req *jRequest) {
	l := c.log.With(slog.String("id", string(req.Id)), slog.String("method", req.Method))
	l.Debug("call handler called", slog.Any("params", req.Params))

	c.handlersTablesLock.RLock()
	handler, ok := c.callHandlers[req.Method]
	c.handlersTablesLock.RUnlock()
	if !ok {
		l.Warn("call handler not found")
		return
	}

	res, err := handler.call(c, req.Params)
	resp := []byte(`{"id":` + string(req.Id) + `,"result":` + string(res) + `}`)
	if err != nil {
		l.Error("call handler error", slog.String("error", err.Error()))
		resp = []byte(`{"id":` + string(req.Id) + `,"error":"` + err.Error() + `"}`)
	}
	if err := c.doSendResponse(req.Id, req.Method, resp); err != nil {
		l.Error("failed to send response", slog.String("error", err.Error()))
	}
}

func (c *ClientConn) dispatchResponse(resp *jResponse) {
	resp.Id = resp.Id[1 : len(resp.Id)-1]
	l := c.log.With(slog.String("id", string(resp.Id)))
	l.Debug("response handler called",
		slog.Any("result", resp.Res),
		slog.Any("error", resp.Error()))

	c.pendingRequestsLock.Lock()
	defer c.pendingRequestsLock.Unlock()
	respChan, ok := c.pendingRequests[string(resp.Id)]
	if !ok {
		l.Warn("no pending response found")
		return
	}

	respChan <- resp
	close(respChan)
	delete(c.pendingRequests, string(resp.Id))
}
