package hlld

import (
	"bufio"
	"bytes"
	"testing"
)

func TestValidWord(t *testing.T) {
	type tcase struct {
		input string
		valid bool
	}
	cases := []tcase{
		{"foo", true},
		{"foo123", true},
		{"Foo123_123", true},
		{"Foo123 123", false},
		{"foo123-123", false},
		{"foo123:123", false},
		{"", false},
	}
	for _, tc := range cases {
		if tc.valid != validWord.MatchString(tc.input) {
			t.Fatalf("failed: %#v", tc)
		}
	}
}

func TestCreateCommand(t *testing.T) {
	// Invalid set
	_, err := NewCreateCommand("foo 123")
	if err == nil {
		t.Fatalf("expect error")
	}

	// Valid set
	cmd, err := NewCreateCommand("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Enable all settings
	cmd.Precision = 12
	cmd.ErrThreshold = 0.05
	cmd.InMemory = true

	var buf bytes.Buffer
	bufW := bufio.NewWriter(&buf)
	if err := cmd.Encode(bufW); err != nil {
		t.Fatalf("err: %v", err)
	}
	bufW.Flush()

	// Verify the encode
	out := string(buf.Bytes())
	expect := "create foo precision=12 eps=0.050000 in_memory=true\n"
	if out != expect {
		t.Fatalf("bad: %s", out)
	}
}
