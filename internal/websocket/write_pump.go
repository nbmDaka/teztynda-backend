package websocket

import (
	"time"

	"github.com/gorilla/websocket"
)

// writePump pumps messages from the send channel to the websocket connection
// IMPORTANT: Only this goroutine writes to the WebSocket connection to guarantee concurrency safety.
func (c *Connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		_ = c.ws.Close()
		c.cancel()
		c.wg.Done()
	}()

	for {
		select {
		case <-c.ctx.Done():
			_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
			return

		case message, ok := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.ws.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(message)

			// Drain queued messages into the same buffer to optimize write throughput
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte{'\n'})
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
