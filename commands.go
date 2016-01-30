package hlld

import (
	"bufio"
	"fmt"
	"regexp"
)

var (
	// validWord is used to sanity check inputs
	validWord = regexp.MustCompile("^[a-zA-Z0-9_]+$")
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

//drop - Drop a set (Deletes from disk)
//close - Closes a set (Unmaps from memory, but still accessible)
//clear - Clears a set from the lists (Removes memory, left on disk)
//set|s - Set an item in a set
//bulk|b - Set many items in a set at once
//info - Gets info about a set
//flush - Flushes all sets or just a specified one<Paste>

// ListSets is used to return a list of sets with their information
func (c *Client) ListSets() ([]*ListEntry, error) {
	return nil, nil
}

// DropSet is used to delete a set
func (c *Client) DropSet(set string) error {
	return nil
}

// CloseSet is used to unmap a set from memory but not delete
func (c *Client) CloseSet(set string) error {
	return nil
}

// ClearSet is used to remove the set from management but remains on disk
func (c *Client) ClearSet(set string) error {
	return nil
}

// SetItems is used to set a series of keys
func (c *Client) SetItems(set string, keys []string) error {
	return nil
}

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
