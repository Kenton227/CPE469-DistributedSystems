package main

import (
	"net/rpc"
	"fmt"
	"rpcDemo/common"
	"net"
)


type CalculatorAPI struct {}

func (calculator *CalculatorAPI) AddTwo(request common.Request, response *common.Response) error {
	response.R = request.A + request.B
	fmt.Printf("request: %v, response: %v\n", request, response)
	return nil
}

func main() {
	// create api that accepts rpc
	calculator := new(CalculatorAPI)
	rpc.Register(calculator)

	// open port to listen on
	listener, _ := net.Listen("tcp", "localhost:7777")

	fmt.Println("Server is ready and waiting for connections on port 7777")

	for {
		// wait to accept request and serve once its accepted
		conn, _ := listener.Accept()
		fmt.Println("Connection accepted...")
		// go keyword makes it threaded
		go rpc.ServeConn(conn)
	}
}
