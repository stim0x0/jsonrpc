package v1

import (
	"log/slog"
)

func serve(srv *ServerConn) {
	defer srv.cancel()
	defer srv.log.Info("server stopped")

	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			srv.log.Error("failed to accept connection", slog.String("error", err.Error()))
			return
		}
		srv.log.Info("new connection accepted", slog.Any("remote", conn.RemoteAddr()))

		clog := srv.log.With(slog.String("remote", conn.RemoteAddr().String()))
		c := NewConnection(conn, clog)
		for method, handler := range srv.notificationHandlers {
			c.setNotificationHandler(method, handler)
		}
		for method, handler := range srv.callHandlers {
			c.setCallHandler(method, handler)
		}

		go func() {
			select {
			case <-srv.ctx.Done():
				clog.Info("server connection closed")
			case <-c.Done():
				clog.Info("connection closed")
			}
			if err := c.Close(); err != nil {
				clog.Warn("failed to close connection", slog.String("error", err.Error()))
			}
		}()
	}
}
