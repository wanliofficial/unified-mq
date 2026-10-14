// S0.4 verification: NATS Go client compiles against the library.
// Run: go mod init t && go get github.com/nats-io/nats.go@v1.37.0 && go build ./...
package main

import (
	"fmt"
	"github.com/nats-io/nats.go"
)

func main() {
	// No nats-server is running, so this is expected to fail at connect time;
	// a successful BUILD proves the client API is usable against the library.
	nc, err := nats.Connect("nats://127.0.0.1:4222")
	if err != nil {
		fmt.Println("nats.Connect rejected as expected (no server):", err)
		return
	}
	defer nc.Close()
	fmt.Println("nats connected")
}
