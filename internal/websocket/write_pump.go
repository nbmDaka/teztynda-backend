package websocket

import (
	"time"

	"github.com/gorilla/websocket"
)

// writePump pumps messages from the send channel to the websocket connection
// IMPORTANT: Only this goroutine writes to the WebSocket connection to guarantee concurrency safety.
func (c *Connection) writePump() {
	if c.ws == nil {
		c.wg.Done()
		return
	}

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
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return

		case message, ok := <-c.send:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				_ = c.ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
				return
			}

			// Write individual JSON text frame
			if err := c.writeMessageFrame(message); err != nil {
				return
			}

			// Drain any pending queued messages, sending each as a discrete WebSocket text frame
			n := len(c.send)
			for i := 0; i < n; i++ {
				select {
				case queuedMsg := <-c.send:
					_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
					if err := c.writeMessageFrame(queuedMsg); err != nil {
						return
					}
				default:
				}
			}

		case <-ticker.C:
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (c *Connection) writeMessageFrame(data []byte) error {
	if c.ws == nil {
		return nil
	}
	w, err := c.ws.NextWriter(websocket.TextMessage)
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
