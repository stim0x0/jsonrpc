package v1

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"
)

var netDial = net.Dial

const defaultMaxTimout = 30 * time.Second

type ConnOpt func(c *ClientConn)

func WithMaxTimeout(t time.Duration) ConnOpt {
	return func(c *ClientConn) {
		c.maxTimeout = t
	}
}
func WithDefaultTimeout(t time.Duration) ConnOpt {
	return func(c *ClientConn) {
		c.defaultTimeout = t
	}
}

// NewClient creates a new JSON-RPC connection over the specified network protocol and address.
//
// Parameters:
//   - network: The network protocol to use (e.g., "tcp", "unix").
//   - addr: The address to connect to.
//   - _log: Optional logger. If nil, the default logger will be used.
//
// Returns:
//   - A new ClientConn instance ready to send and receive JSON-RPC messages.
//   - An error if the connection could not be established.
//
// The connection establishes communication channels for different message types
// and starts background goroutines for message processing.
func NewClient(network, addr string, _log *slog.Logger, opts ...ConnOpt) (Connection, error) {
	if _log == nil {
		_log = slog.Default()
	}

	c, err := netDial(network, addr)
	if err != nil {
		_log.Debug("fail to connect", slog.String("network", network), slog.String("addr", addr))
		return nil, fmt.Errorf("fail to connect: %w", err)
	}

	_log = _log.With(slog.String("jRPC-client", network+"://"+addr))
	_log.Info("connected", slog.String("remote", c.RemoteAddr().String()))

	return NewConnection(c, _log), nil
}

// NewConnection creates a new JSON-RPC ClientConn instance from an existing network connection.
//
// It sets up all necessary communication channels, initializes the connection structure,
// and launches background goroutines for message processing.
//
// Parameters:
//   - c: An established network connection implementing the net.Conn interface
//   - _log: Logger for connection-related events. If nil, the default logger will be used.
//
// Returns:
//   - A fully initialized ClientConn ready to send and receive JSON-RPC messages.
//
// The connection starts with two background goroutines:
//   - broker: Handles message distribution and maintains state for requests and handlers
//   - receiver: Reads and decodes incoming messages from the network connection
func NewConnection(c net.Conn, _log *slog.Logger, opts ...ConnOpt) *ClientConn {
	if _log == nil {
		_log = slog.Default()
	}
	_log = _log.With("jRPC-client", c.RemoteAddr().String())

	ctx, cancel := context.WithCancel(context.Background())
	conn := &ClientConn{
		defaultTimeout:       defaultMaxTimout,
		conn:                 c,
		log:                  _log,
		ctx:                  ctx,
		cancel:               cancel,
		notificationHandlers: make(map[string]*notificationHandler),
		callHandlers:         make(map[string]*callHandler),
		pendingRequests:      make(map[string]chan<- Response),
		reqId:                make(chan string),
	}
	for _, opt := range opts {
		opt(conn)
	}

	// Create communication channels for message exchange
	//const chanBufferSize = 200
	//notificationChan := make(chan *jRequest, chanBufferSize)
	//callChan := make(chan *jRequest, chanBufferSize)
	//responseChan := make(chan *jResponse, chanBufferSize)
	//actionChan := make(chan *action, chanBufferSize)
	//conn.actionChan = actionChan

	// Start background goroutines for message processing
	conn.wg.Add(1)
	//go broker(conn, actionChan, responseChan, notificationChan, callChan)
	go conn.receiver()
	go func() {
		var id int
		for {
			id++
			idStr := strconv.Itoa(id)
			select {
			case <-conn.ctx.Done():
				close(conn.reqId)
				return
			case conn.reqId <- idStr:
			}
		}
	}()

	return conn
}

// ClientConn represents a JSON-RPC client that facilitates sending and receiving messages.
// It supports both synchronous and asynchronous calls, notifications, and
// processing incoming notifications/calls from the server.
type ClientConn struct {
	maxTimeout     time.Duration
	defaultTimeout time.Duration // defaultTimeout default timeout for requests
	conn           net.Conn      // conn basic net connection with error state
	sendLock       sync.Mutex    // sendLock locks outgoing requests
	//actionChan           chan *action       // actionChan channel for sending actions to the broker
	wg                   sync.WaitGroup     // wg wait group for goroutines
	log                  *slog.Logger       // log is a logger
	ctx                  context.Context    // ctx is a context for the connection
	cancel               context.CancelFunc // cancel is a cancel function for the context
	notificationHandlers map[string]*notificationHandler
	callHandlers         map[string]*callHandler
	handlersTablesLock   sync.RWMutex               // mu locks outgoing requests and Close function
	pendingRequests      map[string]chan<- Response // pendingRequests map of pending requests
	pendingRequestsLock  sync.RWMutex               // mu locks outgoing requests and Close function
	reqId                chan string                // reqId is a channel for generating request IDs
}

func (c *ClientConn) Done() <-chan struct{} {
	return c.ctx.Done()
}

// Close closes the connection.
func (c *ClientConn) Close() error {
	c.cancel()
	c.conn.Close()
	c.wg.Wait()
	return nil
}

func (c *ClientConn) Notify(ctx context.Context, method string, params ...any) error {
	data := bytes.NewBufferString(`{"id":null,"method":"` + method + `","params":`)
	if err := json.NewEncoder(data).Encode(params); err != nil {
		return fmt.Errorf("failed to encode params: %w", err)
	}
	data.WriteString("}")

	return c.doSendRequest(ctx, "", method, data.Bytes())
}

// Send sends a single JSON-RPC request asynchronously.
func (c *ClientConn) Send(ctx context.Context, method string, params ...any) (<-chan Response, error) {
	id := <-c.reqId
	data := bytes.NewBufferString(`{"id":"` + id + `","method":"` + method + `","params":`)
	if err := json.NewEncoder(data).Encode(params); err != nil {
		return nil, fmt.Errorf("failed to encode params: %w", err)
	}
	data.WriteString("}")

	respChan := make(chan Response, 1)
	c.pendingRequestsLock.Lock()
	c.pendingRequests[id] = respChan
	c.pendingRequestsLock.Unlock()

	err := c.doSendRequest(ctx, id, method, data.Bytes())
	if err != nil {
		c.pendingRequestsLock.Lock()
		delete(c.pendingRequests, id)
		close(respChan)
		c.pendingRequestsLock.Unlock()
		return nil, err
	}

	return respChan, nil
}

// Call sends a single JSON-RPC request synchronously.
func (c *ClientConn) Call(ctx context.Context, method string, params ...any) (Response, error) {
	respChan, err := c.Send(ctx, method, params...)
	if err != nil {
		return nil, err
	}
	res := <-respChan
	return res, nil
}

// HandleNotification sets notification handler for incoming JSON-RPC notification.
func (c *ClientConn) HandleNotification(method string, fn any) error {
	nh, err := newNotificationHandler(fn)
	if err != nil {
		return err
	}
	c.setNotificationHandler(method, nh)
	return nil
}

func (c *ClientConn) setNotificationHandler(method string, nh *notificationHandler) {
	c.handlersTablesLock.Lock()
	c.notificationHandlers[method] = nh
	c.handlersTablesLock.Unlock()
}

// HandleCall sets call handler for incoming JSON-RPC call.
func (c *ClientConn) HandleCall(method string, fn any) error {
	ch, err := newCallHandler(fn)
	if err != nil {
		return err
	}
	c.setCallHandler(method, ch)
	return nil
}

func (c *ClientConn) setCallHandler(method string, ch *callHandler) {
	c.handlersTablesLock.Lock()
	c.callHandlers[method] = ch
	c.handlersTablesLock.Unlock()
}
