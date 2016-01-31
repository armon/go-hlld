package hlld

import (
	"net"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	conf := DefaultConfig()
	err := conf.Validate()
	if err != nil {
		t.Fatalf("err: %v")
	}
}

func TestClient(t *testing.T) {
	list, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer list.Close()

	go func() {
		// Listen as server
		conn, err := list.Accept()
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		defer conn.Close()

		// Don't bother read, just send the response
		conn.Write([]byte("Done\nDone\nDone\n"))
	}()

	// Dial the client
	client, err := Dial(list.Addr().String())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer client.Close()

	// Create, then set, then drop
	create, err := NewCreateCommand("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	createFuture, err := client.Execute(create)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	set, err := NewSetKeysCommand("foo", []string{"bar"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	setFuture, err := client.Execute(set)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	drop, err := NewDropCommand("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	dropFuture, err := client.Execute(drop)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	futures := []*Future{createFuture, setFuture, dropFuture}
	for _, f := range futures {
		if err := f.Error(); err != nil {
			t.Fatalf("err: %v", err)
		}
	}
}
