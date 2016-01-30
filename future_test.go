package hlld

import (
	"errors"
	"testing"
	"time"
)

func TestFuture(t *testing.T) {
	cmd, _ := NewCreateCommand("foo")
	f := NewFuture(cmd)

	if f.Command() != cmd {
		t.Fatalf("bad")
	}

	expect := errors.New("hello!")
	doneCh := make(chan struct{})
	go func() {
		err := f.Error()
		close(doneCh)
		if err != expect {
			t.Fatalf("bad error")
		}

		err = f.Error()
		if err != expect {
			t.Fatalf("bad error")
		}
	}()

	// Ensure we are blocking
	time.Sleep(10 * time.Millisecond)
	select {
	case <-doneCh:
		t.Fatalf("should be blocked")
	default:
	}

	// Unblock
	f.respond(expect)

	// Ensure unblock
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	}
}
