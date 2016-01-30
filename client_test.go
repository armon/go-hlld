package hlld

import "testing"

func TestDefaultConfig(t *testing.T) {
	conf := DefaultConfig()
	err := conf.Validate()
	if err != nil {
		t.Fatalf("err: %v")
	}
}
