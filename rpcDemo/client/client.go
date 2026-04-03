package main

import (
	"net/rpc"
	"fmt"
	"rpcDemo/common"
)


func main() {
	client, _ := rpc.Dial("tcp", "localhost:7777")

	request := common.Request{A: 4, B: 5}
	var response common.Response

	_ = client.Call("CalculatorAPI.AddTwo", request, &response)

	fmt.Println("Response from serveer: %v\n", response)
}
