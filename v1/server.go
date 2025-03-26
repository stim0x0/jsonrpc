package v1

import (
	"context"
	"log/slog"
	"net"
	"sync"
)

// serverNotificationHandler is a function type that handles JSON-RPC notifications.
// It receives a connection and the raw notification parameters as a byte slice.
type serverNotificationHandler func(c *ClientConn, params ...any)

// serverCallHandler is a function type that handles JSON-RPC calls.
// It receives a connection, a response object, and a channel to send the response back.
type serverCallHandler func(c *ClientConn, id string, params jsonValueType, respChan chan<- *jResponse)

// NewServer creates a new JSON-RPC server that listens on the specified network and address.
//
// Parameters:
//   - net: The network to listen on (e.g., "tcp", "unix")
//   - addr: The address to listen on (e.g., ":8080", "/tmp/socket")
//   - _log: Optional logger (defaults to slog.Default() if nil)
//
// Returns:
//   - *ServerConn: An initialized server connection ready to handle requests
//   - error: Any error encountered during listener setup
func NewServer(network, addr string, _log *slog.Logger) (Server, error) {
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
//   - *ServerConn: A pointer to the initialized server connection
func NewServerConnection(c net.Listener, _log *slog.Logger) Server {
	if _log == nil {
		_log = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	srv := ServerConn{
		notificationHandlers: make(map[string]*notificationHandler),
		callHandlers:         make(map[string]*callHandler),
		mu:                   sync.RWMutex{},
		listener:             c,
		log:                  _log,
		ctx:                  ctx,
		cancel:               cancel,
	}

	go serve(&srv)
	return &srv
}

type ServerConn struct {
	notificationHandlers map[string]*notificationHandler // notificationHandlers map of notification handlers
	callHandlers         map[string]*callHandler         // callHandlers map of call handlers
	mu                   sync.RWMutex                    // mu locks outgoing requests and Close function
	log                  *slog.Logger                    // log is a logger
	//defaultTimeout time.Duration      // defaultTimeout default timeout for requests
	//conn           *rawConnection     // conn basic net connection with error state
	//mu             sync.RWMutex       // mu locks outgoing requests and Close function
	//actionChan     chan *action       // actionChan channel for sending actions to the broker
	//wg             sync.WaitGroup     // wg wait group for goroutines
	//log            *slog.Logger       // log is a logger
	//ctx            context.Context    // ctx is a context for the connection
	//cancel         context.CancelFunc // cancel is a cancel function for the context

	listener net.Listener
	//conns    []*ClientConn
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *ServerConn) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cancel()

	if err := s.listener.Close(); err != nil {
		s.log.Warn("failed to close listener", slog.String("error", err.Error()))
	}

	return nil
}

func (s *ServerConn) HandleNotification(method string, fn any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	noteH, err := newNotificationHandler(fn)
	if err != nil {
		return err
	}

	s.notificationHandlers[method] = noteH
	return nil
}

func (s *ServerConn) HandleCall(method string, fn any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	callH, err := newCallHandler(fn)
	if err != nil {
		return err
	}

	s.callHandlers[method] = callH
	return nil
}
