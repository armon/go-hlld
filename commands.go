package hlld

import (
	"bufio"
	"fmt"
	"regexp"
)

var (
	// validWord is used to sanity check inputs
	validWord = regexp.MustCompile("^[a-zA-Z0-9_]+$")

	// validKey is used to sanity check input keys
	validKey = regexp.MustCompile("^[^ \n]+$")
)

// CreateCommand is used to make a new set
type CreateCommand struct {
	// SetName is the name of the set to create
	SetName string

	// Precision is the number of bits used for the bucket, the higher
	// precision will reduce the errors at the cost of using more memory.A
	// By default this is unspecified and computed based on the ErrThreshold.
	Precision int

	// ErrThreshold is used to control the tolerable error. Higher thresholds
	// require less precision and less memory. It is optional and can be unspecified
	// to use the server default.
	ErrThreshold float64

	// InMemory can be set to true to prevent the set from ever being
	// paged out to disk. This is not recommended as it prevents cold
	// sets from leaving memory.
	InMemory bool

	// result is the result of the decode
	result string
}

// NewCreateCommand is used to prepare a new create command
func NewCreateCommand(name string) (*CreateCommand, error) {
	if !validWord.MatchString(name) {
		return nil, fmt.Errorf("invalid set name")
	}
	cmd := &CreateCommand{
		SetName: name,
	}
	return cmd, nil
}

func (c *CreateCommand) Encode(w *bufio.Writer) error {
	if _, err := w.WriteString("create "); err != nil {
		return err
	}
	if _, err := w.WriteString(c.SetName); err != nil {
		return err
	}
	if c.Precision != 0 {
		if _, err := fmt.Fprintf(w, " precision=%d", c.Precision); err != nil {
			return err
		}
	}
	if c.ErrThreshold != 0 {
		if _, err := fmt.Fprintf(w, " eps=%f", c.ErrThreshold); err != nil {
			return err
		}
	}
	if c.InMemory != false {
		if _, err := w.WriteString(" in_memory=true"); err != nil {
			return err
		}
	}
	return w.WriteByte('\n')
}

func (c *CreateCommand) Decode(r *bufio.Reader) error {
	resp, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	c.result = resp
	return nil
}

func (c *CreateCommand) Result() (bool, error) {
	switch c.result {
	case "":
		return false, fmt.Errorf("result not decoded yet")
	case "Done\n":
		return true, nil
	case "Exists\n":
		return true, nil
	case "Delete in progress\n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid response: %s", c.result)
	}
}

// ListCommand is used to make a new set
type ListCommand struct {
	// Prefix is the prefix to filter
	Prefix string

	// lines is each line of output
	lines []string

	// Done indicates we've ended decode
	done bool
}

// NewListCommand is used to list the sets, filtering on
// an optional prefix
func NewListCommand(prefix string) (*ListCommand, error) {
	if prefix != "" && !validWord.MatchString(prefix) {
		return nil, fmt.Errorf("invalid prefix")
	}
	cmd := &ListCommand{
		Prefix: prefix,
	}
	return cmd, nil
}

func (c *ListCommand) Encode(w *bufio.Writer) error {
	if _, err := w.WriteString("list"); err != nil {
		return err
	}
	if c.Prefix != "" {
		w.WriteByte(' ')
		if _, err := w.WriteString(c.Prefix); err != nil {
			return err
		}
	}
	return w.WriteByte('\n')
}

func (c *ListCommand) Decode(r *bufio.Reader) error {
	started := false
	for {
		resp, err := r.ReadString('\n')
		if err != nil {
			return err
		}

		// Handle the start condition
		if !started {
			if resp != "START\n" {
				return fmt.Errorf("expect list start block")
			}
			started = true
			continue
		}

		// Check for the end
		if resp == "END\n" {
			c.done = true
			return nil
		}

		// Store the line
		c.lines = append(c.lines, resp)
	}
	return nil
}

// ListEntry is used to provide the details of a set when listing
type ListEntry struct {
	Name         string
	ErrThreshold float64
	Precision    int
	Size         uint64
	Storage      uint64
}

func (c *ListCommand) Result() ([]*ListEntry, error) {
	if !c.done {
		return nil, fmt.Errorf("result not decoded yet")
	}

	out := make([]*ListEntry, len(c.lines))
	for idx, line := range c.lines {
		le := &ListEntry{}
		_, err := fmt.Sscanf(line, "%s %f %d %d %d\n", &le.Name,
			&le.ErrThreshold, &le.Precision, &le.Size, &le.Storage)
		if err != nil {
			return nil, fmt.Errorf("failed to parse '%s'", line)
		}
		out[idx] = le
	}
	return out, nil
}

// SetCommand is used to act on a set
type SetCommand struct {
	// Command is invoked on the set
	Command string

	// SetName is the name of the set to create
	SetName string

	// result is the result of the decode
	result string
}

// NewDropCommand is used to drop a set
func NewDropCommand(name string) (*SetCommand, error) {
	if !validWord.MatchString(name) {
		return nil, fmt.Errorf("invalid set name")
	}
	cmd := &SetCommand{
		Command: "drop",
		SetName: name,
	}
	return cmd, nil
}

// NewCloseCommand is used to close a set out of memory
func NewCloseCommand(name string) (*SetCommand, error) {
	if !validWord.MatchString(name) {
		return nil, fmt.Errorf("invalid set name")
	}
	cmd := &SetCommand{
		Command: "close",
		SetName: name,
	}
	return cmd, nil
}

// NewClearCommand is used to remove a set from management, but leave on disk
func NewClearCommand(name string) (*SetCommand, error) {
	if !validWord.MatchString(name) {
		return nil, fmt.Errorf("invalid set name")
	}
	cmd := &SetCommand{
		Command: "clear",
		SetName: name,
	}
	return cmd, nil
}

func (c *SetCommand) Encode(w *bufio.Writer) error {
	if _, err := w.WriteString(c.Command); err != nil {
		return err
	}
	w.WriteByte(' ')
	if _, err := w.WriteString(c.SetName); err != nil {
		return err
	}
	return w.WriteByte('\n')
}

func (c *SetCommand) Decode(r *bufio.Reader) error {
	resp, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	c.result = resp
	return nil
}

func (c *SetCommand) Result() (bool, error) {
	switch c.result {
	case "":
		return false, fmt.Errorf("result not decoded yet")
	case "Done\n":
		return true, nil
	case "Set does not exist\n":
		if c.Command == "drop" {
			return true, nil
		} else {
			return false, nil
		}
	case "Set is not proxied. Close it first.\n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid response: %s", c.result)
	}
}

// SetKeysCommand is used to set keys in a set
type SetKeysCommand struct {
	// SetName is the name of the set to create
	SetName string

	// Keys is the keys to set
	Keys []string

	// result is the result of the decode
	result string
}

// NewSetKeysCommand is used to set keys in a set
func NewSetKeysCommand(name string, keys []string) (*SetKeysCommand, error) {
	if !validWord.MatchString(name) {
		return nil, fmt.Errorf("invalid set name")
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("missing keys to set")
	}
	for _, key := range keys {
		if !validKey.MatchString(key) {
			return nil, fmt.Errorf("invalid key: %s", key)
		}
	}
	cmd := &SetKeysCommand{
		SetName: name,
		Keys:    keys,
	}
	return cmd, nil
}

func (c *SetKeysCommand) Encode(w *bufio.Writer) error {
	if _, err := w.WriteString("b "); err != nil {
		return err
	}
	if _, err := w.WriteString(c.SetName); err != nil {
		return err
	}
	for _, key := range c.Keys {
		w.WriteByte(' ')
		if _, err := w.WriteString(key); err != nil {
			return err
		}
	}
	return w.WriteByte('\n')
}

func (c *SetKeysCommand) Decode(r *bufio.Reader) error {
	resp, err := r.ReadString('\n')
	if err != nil {
		return err
	}
	c.result = resp
	return nil
}

func (c *SetKeysCommand) Result() (bool, error) {
	switch c.result {
	case "":
		return false, fmt.Errorf("result not decoded yet")
	case "Done\n":
		return true, nil
	case "Set does not exist\n":
		return false, nil
	default:
		return false, fmt.Errorf("invalid response: %s", c.result)
	}
}

//info - Gets info about a set
//flush - Flushes all sets or just a specified one<Paste>

// SetInfo contains the results of a query
type SetInfo struct {
	// InMemory is true if the set is currently in memory
	InMemory bool

	// PageIns is the number of times the set has been paged in
	PageIns uint64

	// PageOuts is the number of times the set has been paged out
	PageOuts uint64

	// ErrThreshold is the error tolerance of the set
	ErrThreshold float64

	// Precision is the number of precision bits used
	Precision int

	// Sets is the number of write operations
	Sets uint64

	// Size is the estimated cardinaality of the set
	Size uint64

	// Storage is the disk space requirements of the set
	Storage uint64
}

// QuerySet is used to return information about a set
func (c *Client) QuerySet(set string) (*SetInfo, error) {
	return nil, nil
}

// FlushSet flushes any outstanding data to disk for a set
func (c *Client) FlushSet(set string) error {
	return nil
}

// FlushAll flushes all the dirty sets to disk
func (c *Client) FlushAll() error {
	return nil
}
