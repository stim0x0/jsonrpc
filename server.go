package jsonrpc

import (
	"context"
	"log/slog"
	"net"
	"sync"
)

// ServerNotificationHandler is a function type that handles JSON-RPC notifications.
// It receives a connection and the raw notification parameters as a byte slice.
type ServerNotificationHandler func(c *Connection, params []byte)

// ServerCallHandler is a function type that handles JSON-RPC calls.
// It receives a connection, a response object, and a channel to send the response back.
type ServerCallHandler func(c *Connection, response *Response, respChan chan<- *Response)

// NewServer creates a new JSON-RPC server that listens on the specified network and address.
//
// Parameters:
//   - net: The network to listen on (e.g., "tcp", "unix")
//   - addr: The address to listen on (e.g., ":8080", "/tmp/socket")
//   - _log: Optional logger (defaults to slog.Default() if nil)
//
// Returns:
//   - *ServerConnection: An initialized server connection ready to handle requests
//   - error: Any error encountered during listener setup
func NewServer(network, addr string, _log *slog.Logger) (*ServerConnection, error) {
	if _log == nil {
		_log = slog.Default()
	}

	l, err := net.Listen(network, addr)
	if err != nil {
		return nil, err
	}

	srv := NewServerConnection(l, _log.With(slog.String("network", network), slog.String("addr", addr)))
	return srv, nil
}

// NewServerConnection creates a new JSON-RPC server connection using the provided net.Listener and logger.
// It initializes the connection, sets up notification and call handlers, and starts a goroutine to handle incoming connections.
//
// Parameters:
//   - c: The network listener that will accept incoming connections
//   - _log: Optional logger (defaults to slog.Default() if nil)
//
// Returns:
//   - *ServerConnection: A pointer to the initialized server connection
func NewServerConnection(c net.Listener, _log *slog.Logger) *ServerConnection {
	if _log == nil {
		_log = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	srv := ServerConnection{
		notificationHandlers: make(map[string]ServerNotificationHandler),
		callHandlers:         make(map[string]ServerCallHandler),
		mu:                   sync.RWMutex{},
		listener:             c,
		log:                  _log,
		ctx:                  ctx,
		cancel:               cancel,
	}

	go serve(&srv)
	return &srv
}

type ServerConnection struct {
	notificationHandlers map[string]ServerNotificationHandler // notificationHandlers map of notification handlers
	callHandlers         map[string]ServerCallHandler         // callHandlers map of call handlers
	mu                   sync.RWMutex                         // mu locks outgoing requests and Close function
	log                  *slog.Logger                         // log is a logger
	//defaultTimeout time.Duration      // defaultTimeout default timeout for requests
	//conn           *rawConnection     // conn basic net connection with error state
	//mu             sync.RWMutex       // mu locks outgoing requests and Close function
	//actionChan     chan *action       // actionChan channel for sending actions to the broker
	//wg             sync.WaitGroup     // wg wait group for goroutines
	//log            *slog.Logger       // log is a logger
	//ctx            context.Context    // ctx is a context for the connection
	//cancel         context.CancelFunc // cancel is a cancel function for the context

	listener net.Listener
	//conns    []*Connection
	ctx    context.Context
	cancel context.CancelFunc
}

func serve(srv *ServerConnection) {
	defer srv.cancel()
	defer srv.log.Info("server stopped")
LISTEN:
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			srv.log.Error("failed to accept connection", slog.String("error", err.Error()))
			return
		}
		srv.log.Info("new connection accepted", slog.Any("remote", conn.RemoteAddr()))

		c := NewConnection(conn, srv.log.With(slog.String("remote", conn.RemoteAddr().String())))
		for method, handler := range srv.notificationHandlers {
			if err := c.Handle(method, func(params []byte) { handler(c, params) }); err != nil {
				c.log.Error("failed to set notification handler", slog.String("method", method), slog.String("error", err.Error()))
				if err := c.Close(); err != nil {
					c.log.Warn("failed to close connection", slog.String("error", err.Error()))
				}
				continue LISTEN
			}
		}
		for method, handler := range srv.callHandlers {
			if err := c.HandleCall(method, func(response *Response, respChan chan<- *Response) { handler(c, response, respChan) }); err != nil {
				c.log.Error("failed to set call handler", slog.String("method", method), slog.String("error", err.Error()))
				if err := c.Close(); err != nil {
					c.log.Warn("failed to close connection", slog.String("error", err.Error()))
				}
				continue LISTEN
			}
		}

		go func() {
			select {
			case <-srv.ctx.Done():
				c.log.Info("server connection closed")
			case <-c.ctx.Done():
				c.log.Info("connection closed")
			}
			if err := c.Close(); err != nil {
				c.log.Warn("failed to close connection", slog.String("error", err.Error()))
			}
		}()
	}
}

func (s *ServerConnection) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel()

	if err := s.listener.Close(); err != nil {
		s.log.Warn("failed to close listener", slog.String("error", err.Error()))
	}

	return nil
}

func (s *ServerConnection) Handle(method string, handler ServerNotificationHandler) ServerNotificationHandler {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldHandler := s.notificationHandlers[method]
	s.notificationHandlers[method] = handler
	return oldHandler
}

func (s *ServerConnection) HandleCall(method string, handler ServerCallHandler) ServerCallHandler {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldHandler := s.callHandlers[method]
	s.callHandlers[method] = handler
	return oldHandler
}
