package hlld

import (
	"bufio"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	// ErrClientClosed is used if the client is closed
	ErrClientClosed = fmt.Errorf("client closed")
)

// Command is used to represent any command that can be sent to
// HLLD. It must be able to encode and decode from the wire.
type Command interface {
	Encode(*bufio.Writer) error
	Decode(*bufio.Reader) error
}

// Client is used to interact with an hlld server
type Client struct {
	config *Config

	conn net.Conn
	bufR *bufio.Reader

	bufW      *bufio.Writer
	writeLock sync.Mutex

	decodeCh chan *Future

	closed     bool
	closedCh   chan struct{}
	closedLock sync.Mutex
}

// Config is used to parameterize the client
type Config struct {
	// MaxPipeline is the maximum number of commands to pipeline
	MaxPipeline int

	// Timeout is the read or write timeout
	Timeout time.Duration
}

// Validate is used to sanity check the configuration
func (c *Config) Validate() error {
	if c.MaxPipeline <= 0 {
		return fmt.Errorf("max pipeline must be positive")
	}
	if c.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}
	return nil
}

// DefaultConfig is used as the default client configuration
func DefaultConfig() *Config {
	return &Config{
		MaxPipeline: 8192,
		Timeout:     5 * time.Second,
	}
}

// NewClient is used to create a new client by wrapping an existing connection
func NewClient(conn net.Conn, config *Config) (*Client, error) {
	// Default config if none given
	if config == nil {
		config = DefaultConfig()
	}
	if err := config.Validate(); err != nil {
		return nil, err
	}

	c := &Client{
		config:   config,
		conn:     conn,
		bufR:     bufio.NewReader(conn),
		bufW:     bufio.NewWriter(conn),
		decodeCh: make(chan *Future, config.MaxPipeline),
		closedCh: make(chan struct{}),
	}
	go c.reader()
	return c, nil
}

// Close is used to shut down the client
func (c *Client) Close() error {
	c.closedLock.Lock()
	defer c.closedLock.Lock()

	if c.closed {
		return nil
	}
	c.closed = true
	close(c.closedCh)
	c.conn.Close()
	return nil
}

// isClosed checks if the client is closed
func (c *Client) isClosed() bool {
	select {
	case <-c.closedCh:
		return true
	default:
		return false
	}
}

// reader is used to read the commands and decode them in an async manner
func (c *Client) reader() {
	for {
		select {
		case next := <-c.decodeCh:
			// Set the read deadline
			c.conn.SetReadDeadline(time.Now().Add(c.config.Timeout))

			// Decode the next command
			err := next.Command().Decode(c.bufR)
			next.respond(err)

			// Shutdown if there was an error
			if err != nil {
				c.Close()
				goto DRAIN
			}

		case <-c.closedCh:
			goto DRAIN
		}
	}

	// After the main loop, drain the decode channel
DRAIN:
	for {
		select {
		case next := <-c.decodeCh:
			next.respond(ErrClientClosed)
		default:
			return
		}
	}
}

// Execute starts command execution and returns a future
func (c *Client) Execute(cmd Command) (*Future, error) {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	// Check if the client is closed
	if c.isClosed() {
		return nil, ErrClientClosed
	}

	// Set the write deadline
	c.conn.SetWriteDeadline(time.Now().Add(c.config.Timeout))

	// Encode the command
	err := cmd.Encode(c.bufW)

	// Flush the writter
	if err == nil {
		err = c.bufW.Flush()
	}

	// Respond and do not enqueue on error, close the socket
	if err != nil {
		c.Close()
		return nil, err
	}

	// Push the future to the decode channel
	f := NewFuture(cmd)
	select {
	case c.decodeCh <- f:
	case <-c.closedCh:
		f.respond(ErrClientClosed)
	}
	return f, nil
}
