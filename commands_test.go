package hlld

import (
	"bufio"
	"bytes"
	"reflect"
	"strings"
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

	// Verify the encode
	expect := "create foo precision=12 eps=0.050000 in_memory=true\n"
	verifyEncode(t, cmd, expect)

	// Verify the decode
	verifyDecode(t, cmd, "Done\n")
	ok, err := cmd.Result()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("bad")
	}

	// Verify the decode
	verifyDecode(t, cmd, "Exists\n")
	ok, err = cmd.Result()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !ok {
		t.Fatalf("bad")
	}

	// Verify the decode
	verifyDecode(t, cmd, "Delete in progress\n")
	ok, err = cmd.Result()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ok {
		t.Fatalf("bad")
	}
}

func TestListCommand(t *testing.T) {
	// Invalid prefix
	_, err := NewListCommand("foo 123")
	if err == nil {
		t.Fatalf("expect error")
	}

	// Valid prefix
	cmd, err := NewListCommand("foo")
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Verify the encode
	expect := "list foo\n"
	verifyEncode(t, cmd, expect)

	// Verify the decode
	inp := `START
foo 0.010000 14 13108 0
baz 0.005000 16 18000 50
END
`
	verifyDecode(t, cmd, inp)
	list, err := cmd.Result()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("bad: %#v", list)
	}

	expectList := []*ListEntry{
		{"foo", 0.01, 14, 13108, 0},
		{"baz", 0.005, 16, 18000, 50},
	}
	if !reflect.DeepEqual(list, expectList) {
		t.Fatalf("bad: %#v", list)
	}
}

func verifyEncode(t *testing.T, cmd Command, expect string) {
	var buf bytes.Buffer
	bufW := bufio.NewWriter(&buf)
	if err := cmd.Encode(bufW); err != nil {
		t.Fatalf("err: %v", err)
	}
	bufW.Flush()

	// Verify the encode
	out := string(buf.Bytes())
	if out != expect {
		t.Fatalf("bad: %s (expected: %s)", out, expect)
	}
}

func verifyDecode(t *testing.T, cmd Command, input string) {
	r := strings.NewReader(input)
	bufR := bufio.NewReader(r)

	if err := cmd.Decode(bufR); err != nil {
		t.Fatalf("err: %v", err)
	}
}
