# go-hlld

Provides the `hlld` package that implements a client library for the
[HyperLogLog daemon](https://github.com/armon/hlld) (HLLD). HyperLogLogs provide
an extremely efficient method of cardinality estimation. The client library
supports pipelining for extremely high throughput.


Documentation
=============

The full documentation is available on [Godoc](http://godoc.org/github.com/armon/go-hlld).

Example
=======

Below is a simple example of usage

```go
// Create a client
client, err := hlld.Dial("hlld-server:1234")
if err != nil {
    panic("could not dial")
}

// Create a new set, custom precision
createCommand, err := hlld.NewCreateCommand("foo")
if err != nil {
    panic("failed to make command")
}

// Start the command
future, err := client.Execute(createCommand)
if err != nil {
    panic("failed to make command")
}

// Wait for the command to finish
if err := future.Error(); err != nil {
    panic("command failed")
}

// Check the result
ok, err := createCommand.Result()
if !ok || err != nil {
    panic("failed to make set")
}
```
