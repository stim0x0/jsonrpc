package jsonrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"
)

var netDial = net.Dial

// NewClient creates a new JSON-RPC connection over the specified network protocol and address.
//
// Parameters:
//   - network: The network protocol to use (e.g., "tcp", "unix").
//   - addr: The address to connect to.
//   - _log: Optional logger. If nil, the default logger will be used.
//
// Returns:
//   - A new Connection instance ready to send and receive JSON-RPC messages.
//   - An error if the connection could not be established.
//
// The connection establishes communication channels for different message types
// and starts background goroutines for message processing.
func NewClient(network, addr string, _log *slog.Logger) (*Connection, error) {
	if _log == nil {
		_log = slog.Default()
	}

	c, err := netDial(network, addr)
	if err != nil {
		return nil, fmt.Errorf("не удалось установить соединение: %w", err)
	}

	_log.Debug("connected", slog.String("uri", fmt.Sprintf("%s://%s", network, addr)))

	return NewConnection(c, _log), nil
}

func NewConnection(c net.Conn, _log *slog.Logger) *Connection {
	// Create communication channels for message exchange
	const chanBufferSize = 200
	actionChan := make(chan *action, chanBufferSize)
	notificationChan := make(chan *Response, chanBufferSize)
	responseChan := make(chan *Response, chanBufferSize)
	callChan := make(chan *Response, chanBufferSize)

	ctx, cancel := context.WithCancel(context.Background())
	conn := &Connection{
		defaultTimeout: 5 * time.Second,
		conn:           &rawConnection{Conn: c},
		actionChan:     actionChan,
		log:            _log,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Start background goroutines for message processing
	conn.wg.Add(2)
	go broker(conn, actionChan, responseChan, notificationChan, callChan)
	go receiver(conn, responseChan, notificationChan, callChan)

	return conn
}

// Connection represents a JSON-RPC client that facilitates sending and receiving messages.
// It supports both synchronous and asynchronous calls, notifications, and
// processing incoming notifications/calls from the server.
type Connection struct {
	defaultTimeout time.Duration      // defaultTimeout default timeout for requests
	conn           *rawConnection     // conn basic net connection with error state
	mu             sync.RWMutex       // mu locks outgoing requests and Close function
	actionChan     chan *action       // actionChan channel for sending actions to the broker
	wg             sync.WaitGroup     // wg wait group for goroutines
	log            *slog.Logger       // log is a logger
	ctx            context.Context    // ctx is a context for the connection
	cancel         context.CancelFunc // cancel is a cancel function for the context
}

// broker is the central goroutine that manages message routing for a JSON-RPC connection.
// It handles both outgoing and incoming message flows:
//  1. Outgoing: processes actions from actionChan to send requests, notifications, and server call responses
//  2. Incoming: processes messages from responseChan, notificationChan, and callChan
//
// The broker maintains several state maps:
//   - pendingRequests: tracks outgoing requests awaiting responses
//   - pendingCalls: tracks ongoing server calls being processed
//   - notificationHandlers: registered handlers for incoming notifications
//   - callHandlers: registered handlers for incoming server calls
//
// When the connection closes, the broker ensures proper cleanup by:
//   - Notifying all pending requests with appropriate errors
//   - Cancelling any ongoing server calls
//
// Parameters:
//   - c: The Connection instance that owns this broker
//   - responseChan: Channel for incoming responses to client requests
//   - notificationChan: Channel for incoming server notifications
//   - callChan: Channel for incoming server calls
//
// The broker terminates when any channel is closed or when a critical connection error occurs.
func broker(c *Connection, actionChan <-chan *action, responseChan <-chan *Response, notificationChan <-chan *Response, callChan <-chan *Response) {
	defer c.wg.Done()
	reason := "unknown"
	defer func() { c.log.Debug("broker closed", slog.String("reason", reason)) }()
	defer c.cancel()
	//var actionChan <-chan *action = c.actionChan
	enc := json.NewEncoder(c.conn)
	nextID := uint64(0)
	pendingRequests := make(map[string]*request)
	//pendingCalls := make(map[string]chan<- struct{}, 0)
	defer func() {
		for _, req := range pendingRequests {
			req.res <- &Response{ID: req.ID, Err: c.Error()}
			close(req.res)
		}
		for act := range actionChan {
			switch act.action {
			case requestAction:
				act.idChan <- "0"
				act.respChan <- &Response{Err: c.Error()}
				close(act.respChan)
			case notificationAction:
				act.respChan <- &Response{Err: c.Error()}
				close(act.respChan)
			default:
			}
		}
		//for _, ch := range pendingCalls {
		//	close(ch)
		//}
	}()
	notificationHandlers := make(map[string]NotificationHandler)
	callHandlers := make(map[string]CallHandler)

	for {
		select {
		case <-c.ctx.Done():
			reason = "context done"
			return
		// send request/notification/call_response or
		// handle internal incoming actions supposed to operate in current goroutine
		case act, ok := <-actionChan:
			if !ok {
				reason = "action channel closed"
				return
			}
			switch act.action {
			case setNotificationHandlerAction:
				// ------------------------------------------
				// handle installation of notification handler
				notificationHandlers[act.method] = act.handler
			case setCallHandlerAction:
				// ------------------------------------------
				// handle installation of call handler
				callHandlers[act.method] = act.callHandler
			case dropPendingRequestAction:
				// ------------------------------------------
				// cancel pending request
				req, ok := pendingRequests[act.hId]
				if !ok {
					continue
				}
				delete(pendingRequests, act.hId)
				req.res <- &Response{ID: req.ID, Err: fmt.Errorf("request cancelled")}
				close(req.res)
			case requestAction:
				// ------------------------------------------
				// handle outgoing request
				nextID, ok = doSendRequest(c, nextID, act, enc, pendingRequests)
				if !ok {
					reason = "failed to send request"
					return
				}
			case notificationAction:
				// ------------------------------------------
				// handle outgoing notification
				if !doSendNotification(c, act, enc) {
					reason = "failed to send notification"
					return
				}
			case responseAction:
				// ------------------------------------------
				// handle outgoing call response
				if !doSendCallResponse(c, act, enc) {
					reason = "failed to send call response"
					return
				}
			}

		// receive request result
		case res, ok := <-responseChan:
			if !ok {
				reason = "response channel closed"
				return
			}
			c.log.Debug("received response",
				slog.String("id", *res.ID),
				slog.String("response", string(res.Res)),
				slog.Any("error", res.Error()))
			req, ok := pendingRequests[*res.ID]
			if !ok {
				c.log.Debug("unknown response", slog.String("id", *res.ID))
				continue
			}
			delete(pendingRequests, *res.ID)
			req.res <- res
			close(req.res)

		// receive server notification
		case note, ok := <-notificationChan:
			if !ok {
				reason = "notification channel closed"
				return
			}
			c.log.Debug("received notification",
				slog.String("method", note.Method),
				slog.String("params", string(note.Params)))
			handler, ok := notificationHandlers[note.Method]
			if !ok {
				c.log.Debug("unknown notification", slog.String("method", note.Method))
				continue
			}
			handler(note.Params)

		// receive server call
		case call, ok := <-callChan:
			if !ok {
				reason = "call channel closed"
				return
			}
			c.log.Debug("received call",
				slog.String("id", *call.ID),
				slog.String("method", call.Method),
				slog.String("params", string(call.Params)))
			handler, ok := callHandlers[call.Method]
			if !ok {
				c.log.Debug("unknown call", slog.String("method", call.Method))
				continue
			}

			callRespChan := make(chan *Response)
			go func(method string) {
				select {
				case <-c.ctx.Done():
				case resp := <-callRespChan:
					c.actionChan <- &action{
						action:   responseAction,
						callResp: resp,
					}
				}
			}(call.Method)
			handler(call, callRespChan)
		}
	}
}

// doSendRequest sends a JSON-RPC request to the server and manages the request lifecycle.
// It creates a request with the provided ID, sets appropriate deadlines, handles context cancellation,
// and processes the response or error conditions. The function also manages the pending requests map
// for tracking in-flight requests.
// Parameters:
//   - c: The Connection instance that owns this request
//   - nextID: The ID to use for this request
//   - act: The action containing request details (method, params, context, etc.)
//   - enc: JSON encoder to use for writing the request
//   - pendingRequests: Map of pending requests indexed by ID
//
// Returns:
//   - nextID2: The next available ID for future requests
//   - ok: Boolean indicating if the connection is ok & doesn't enter a failed state
//
// If the connection enters a failed state during request sending, the function returns
// an ok value of false, signaling that the broker should terminate its event loop.
func doSendRequest(c *Connection, nextID uint64, act *action, enc *json.Encoder, pendingRequests map[string]*request) (nextID2 uint64, ok bool) {
	req := newRequest(nextID, act)
	nextID++
	deadline, ok := act.ctx.Deadline()
	if !ok {
		deadline = time.Time{} // no deadline -- use zero time to wait forever
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}
	fin := make(chan struct{})
	go func() {
		select {
		case <-act.ctx.Done():
			if act.ctx.Err() == context.Canceled {
				_ = c.conn.SetWriteDeadline(time.Now())
				c.log.Debug("request cancelled",
					slog.String("method", act.method),
					slog.String("id", *req.ID))
			}
		case <-fin:
		}
	}()
	err := enc.Encode(req)
	close(fin)
	if err != nil {
		act.idChan <- *req.ID
		close(act.idChan)
		act.respChan <- &Response{ID: req.ID, Err: err}
		close(act.respChan)
		if c.conn.failState.Load() {
			c.log.Debug("connection moved to failed state")
			return 0, false
		}
		return nextID, true
	}

	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		req, _ := json.Marshal(req)
		c.log.Debug("sent request", slog.String("request", string(req)))
	}
	pendingRequests[*req.ID] = req
	act.idChan <- *req.ID
	close(act.idChan)
	return nextID, true
}

// doSendNotification sends a JSON-RPC notification to the server.
// Parameters:
//   - c: The Connection instance that owns this notification
//   - act: The action containing notification details (method, params, context)
//   - enc: JSON encoder to use for writing the notification
//
// Returns:
//   - A boolean indicating if the connection ok & doesn't enter a failed state
//
// If the connection enters a failed state during notification sending, the function returns
// false, signaling that the broker should terminate its event loop. Otherwise, returns true.
func doSendNotification(c *Connection, act *action, enc *json.Encoder) bool {
	req := newRequest(0, act)
	deadline, ok := act.ctx.Deadline()
	if !ok {
		deadline = time.Time{} // no deadline -- use zero time to wait forever
	}
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}
	fin := make(chan struct{})
	go func() {
		select {
		case <-act.ctx.Done():
			if act.ctx.Err() == context.Canceled {
				_ = c.conn.SetWriteDeadline(time.Now())
				c.log.Debug("notification canceled",
					slog.String("method", act.method))
			}
		case <-fin:
		}
	}()
	err := enc.Encode(req)
	close(fin)
	if err != nil {
		act.respChan <- &Response{Err: err}
		close(act.respChan)
		if c.conn.failState.Load() {
			c.log.Debug("connection moved to failed state")
			return false
		}
		return true
	}
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		req, _ := json.Marshal(req)
		c.log.Debug("sent notification", slog.String("request", string(req)))
	}
	act.respChan <- &Response{}
	close(act.respChan)
	return true
}

// doSendCallResponse sends a JSON-RPC response back to the server for a received call.
// It sets a write deadline for the operation, encodes the response, and handles any errors
// that occur during sending.
//
// Parameters:
//   - c: The Connection instance that owns this call response
//   - act: The action containing call response details
//   - enc: JSON encoder to use for writing the response
//
// Returns:
//   - A boolean indicating if the connection is ok & doesn't enter a failed state
//
// If the connection enters a failed state during response sending, the function returns
// false, signaling that the broker should terminate its event loop. Otherwise, returns true.
func doSendCallResponse(c *Connection, act *action, enc *json.Encoder) bool {
	deadline := time.Now().Add(c.defaultTimeout)
	if err := c.conn.SetWriteDeadline(deadline); err != nil {
		c.log.Warn("fail to set write deadline", slog.String("error", err.Error()))
	}
	err := enc.Encode(act.callResp)
	if err != nil {
		if c.conn.failState.Load() {
			c.log.Debug("connection moved to failed state")
			return false
		}
		c.log.Debug("failed to send call response", slog.String("error", err.Error()))
		return true
	}
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		resp, _ := json.Marshal(act.callResp)
		c.log.Debug("sent call response", slog.String("response", string(resp)))
	}
	return true
}

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
//   - c: The Connection instance that owns this receiver
//   - responseChan: Channel for sending received responses to pending client requests
//   - notificationChan: Channel for sending received server notifications
//   - callChan: Channel for sending received server calls
func receiver(c *Connection, responseChan chan<- *Response, notificationChan chan<- *Response, callChan chan<- *Response) {
	defer c.wg.Done()
	defer c.log.Debug("receiver closed")
	defer close(notificationChan)
	defer close(responseChan)
	defer close(callChan)
	defer c.cancel()

	dec := json.NewDecoder(c.conn)

	for {
		var resp Response
		if err := dec.Decode(&resp); err != nil && !errors.Is(err, io.EOF) {
			//if errors.Is(err, io.EOF) ||
			//	(errors.Is(err, os.ErrDeadlineExceeded) && c.conn.failState.Load()) ||
			//	strings.Contains(err.Error(), "use of closed network connection") ||
			//	strings.Contains(err.Error(), "io: read/write on closed pipe") {
			//	c.log.Debug("broken connection", slog.String("error", err.Error()))
			//	c.conn.failState.Store(true)
			//	return
			//}
			c.conn.failState.Store(true)
			c.log.Debug("connection problem or fail to decode response", slog.String("error", err.Error()))
			return
		}
		if resp.IsNotification() {
			// notification
			notificationChan <- &resp
			continue
		}
		if resp.IsCall() {
			// call
			callChan <- &resp
			continue
		}
		responseChan <- &resp
	}
}

// Error returns an error if the connection is in a failed state.
func (c *Connection) Error() error {
	var err error = nil
	switch {
	//case c == nil:
	//	err = errors.New("nil connection")
	//case c.conn == nil:
	//	err = errors.New("closed connection")
	case c.conn.failState.Load():
		err = errors.New("failed connection")
	}
	return err
}

// Close closes the connection.
func (c *Connection) Close() error {
	//if c == nil {
	//	c.log.Errorw("close on nil connection")
	//	return errors.New("close on nil connection")
	//}
	//if c.conn == nil {
	//	c.log.Debug("close on closed connection")
	//	return nil
	//}
	c.mu.Lock()
	defer c.mu.Unlock()
	err := c.conn.Close()
	close(c.actionChan)
	c.wg.Wait()
	c.log.Debug("connection closed")
	return err
}

// validateContext check if context not nil and set default timeout if needed.
func (c *Connection) validateContext(ctx context.Context) context.Context {
	if ctx == nil {
		ctx, _ = context.WithTimeout(context.Background(), c.defaultTimeout)
	}
	return ctx
}

func (c *Connection) Notify(ctx context.Context, method string, params ...any) error {
	//if c == nil || c.conn == nil {
	//	return errors.New("closed or nil connection")
	//}
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.Error(); err != nil {
		return err
	}

	respChan := make(chan *Response, 1)
	c.actionChan <- &action{
		action:   notificationAction,
		method:   method,
		params:   params,
		ctx:      c.validateContext(ctx),
		respChan: respChan,
	}
	resp := <-respChan
	if resp.Err != nil {
		return resp.Error()
	}
	return nil
}

// Send sends a single JSON-RPC request asynchronously.
func (c *Connection) Send(ctx context.Context, method string, params ...any) (<-chan *Response, string, error) {
	//if c == nil || c.conn == nil {
	//	return nil, "", errors.New("closed or nil connection")
	//}
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.Error(); err != nil {
		return nil, "", err
	}

	respChan := make(chan *Response, 1)
	idChan := make(chan string)
	c.actionChan <- &action{
		action:   requestAction,
		method:   method,
		params:   params,
		ctx:      c.validateContext(ctx),
		idChan:   idChan,
		respChan: respChan,
	}
	return respChan, <-idChan, nil
}

func (c *Connection) DropPending(id string) {
	//if c == nil || c.conn == nil {
	//	return
	//}
	c.actionChan <- &action{
		action: dropPendingRequestAction,
		hId:    id,
	}
}

// Call sends a single JSON-RPC request synchronously.
func (c *Connection) Call(ctx context.Context, method string, params ...any) (json.RawMessage, error) {
	//if c == nil || c.conn == nil {
	//	return nil, errors.New("closed or nil connection")
	//}
	ctx = c.validateContext(ctx)
	respChan, id, err := c.Send(ctx, method, params...)
	if err != nil {
		return nil, err
	}
	// wait for response or context cancellation
	select {
	case res := <-respChan:
		return res.Res, res.Error()
	case <-ctx.Done():
		c.DropPending(id)
		return nil, ctx.Err()
	}
}

// Handle sets notification handler for incoming JSON-RPC notification.
func (c *Connection) Handle(method string, handler NotificationHandler) error {
	//if c == nil || c.conn == nil {
	//	return errors.New("closed or nil connection")
	//}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.Error(); err != nil {
		return err
	}
	c.actionChan <- &action{
		action:  setNotificationHandlerAction,
		method:  method,
		handler: handler,
	}
	return nil
}

// HandleCall sets call handler for incoming JSON-RPC call.
func (c *Connection) HandleCall(method string, handler CallHandler) error {
	//if c == nil || c.conn == nil {
	//	return errors.New("closed or nil connection")
	//}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := c.Error(); err != nil {
		return err
	}
	c.actionChan <- &action{
		action:      setCallHandlerAction,
		method:      method,
		callHandler: handler,
	}
	return nil
}
