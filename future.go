package hlld

// Future is used to wrap a command and return a future
type Future struct {
	cmd    Command
	err    error
	doneCh chan struct{}
}

// NewFuture returns a new future
func NewFuture(cmd Command) *Future {
	return &Future{
		cmd:    cmd,
		doneCh: make(chan struct{}),
	}
}

// Command returns the underlying command
func (f *Future) Command() Command {
	return f.cmd
}

// Error blocks until the future is complete
func (f *Future) Error() error {
	<-f.doneCh
	return f.err
}

// respond stores the error and unblocks the future
func (f *Future) respond(err error) {
	f.err = err
	close(f.doneCh)
}
