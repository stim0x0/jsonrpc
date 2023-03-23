# JSON-RPC 1.0 partial implementation for wired connections

This library implements basic features JSON-RPC protocol. It is suitable for building client application
communicating over raw network connection. Also, it has some support for RPC server implementation.

## Example

    // Create connection
    conn := NewConnection("unix", "path_to_socket", zapShugaredLogger)

    // Call method with arbitrary arguments (synchronous)
    resultRawJson, err := conn.Call(ctx, "some_method", "arg1", struct{name string, val int}{name: "arg2", val: 3})

    // Send request with arbitrary arguments (asynchronous)
    respChan, id, err := conn.Send(ctx, "some_method", "arg1", struct{name string, val int}{name: "arg2", val: 3})
    // Wait for response
    resp := <-respChan

    // Send notification with arbitrary arguments
    err := conn.Notify(ctx, "some_method", "arg1", struct{name string, val int}{name: "arg2", val: 3})

    // Handle notification from server (params are raw JSON of notification params)
    conn.Handle("echo", func(params []byte) {
        var msg string
        if err := json.Unmarshal(params, &msg); err != nil {
            return
        }
        fmt.Println(msg)
    })

    // Handle call from server (params are raw JSON of call params)
    conn.HandleCall("echo", func(req *Response, resp chan<- *Response) {
        var msg string
        if err := json.Unmarshal(params, &msg); err != nil {
            return nil, err
        }
        fmt.Println(msg)
        resp <- &Response{
            ID: req.ID,
            Res: json.RawMessage(`"echoed"`)
        }
    })

## API


    NewConnection(network, addr string, _log *zap.SugaredLogger) *Connection
Creates new connection

    Error() error
Error returns error if connection is in failed state (closed or failed due some other reason), nil otherwise

    Close() error
Close closes connection

    Notify(ctx context.Context, method string, params ...any) error
Send notification to server

    Send(ctx context.Context, method string, params ...any) (<-chan *Response, string, error)
Send request to server. Returns channel to receive response and request ID

    Call(ctx context.Context, method string, params ...any) (json.RawMessage, error)
Call method on server. Returns result part of response and error if any

    DropPending(id string)
Drop pending request with given ID

    Handle(method string, handler NotificationHandler) error
Register handler for notification, on receiving notification with given method handler will be called
with raw JSON of notification params as argument.
`NotificationHandler` is defined as: `type NotificationHandler func(params []byte)`

    HandleCall(method string, handler CallHandler) error
Register handler for call, on receiving call with given method handler will be called
with raw JSON of call params as argument.
`CallHandler` is defined as: `type CallHandler func(req *Response, resp chan<- *Response)`


