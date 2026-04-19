package main

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"time"
)

var myName string

type PeerAPI struct{}

func (peer *PeerAPI) SayHelloToPeer(request string, response *string) error {
	*response = "Hello from " + myName + "!"
	fmt.Printf("Received: %v, response: %v\n", request, *response)
	return nil
}

func listenToConnections() {
	helloPeer := new(PeerAPI)
	rpc.Register(helloPeer)
	listener, _ := net.Listen("tcp", ":2001")
	for {
		conn, _ := listener.Accept()
		rpc.ServeConn(conn)
	}
}

func main() {

	myName = os.Args[1]
	var peerAddr string

	if myName == "worker1" {
		peerAddr = "worker2:2001"
	} else {
		peerAddr = "worker1:2001"
	}

	go listenToConnections()

	time.Sleep(time.Second * 10)

	for {
		var coordinatorResponse string

		coordinatorConnect, _ := rpc.Dial("tcp", "coordinator:3001")
		coordinatorConnect.Call("HelloAPI.SayHello", myName, &coordinatorResponse)
		fmt.Printf("coordinator response %v\n", coordinatorResponse)

		var peerResponse string

		workerConnect, _ := rpc.Dial("tcp", peerAddr)
		workerConnect.Call("PeerAPI.SayHelloToPeer", myName, &peerResponse)
		fmt.Printf("peer response %v\n", peerResponse)

		time.Sleep(time.Minute)

	}

}
