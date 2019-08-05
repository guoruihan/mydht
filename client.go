package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

type Client struct{
	Node *Node_
	Server *rpc.Server
	L net.Listener
}

var cntz int

func Call(address string,method string,request interface{},response interface{}) error {
	client, err := rpc.Dial("tcp", address)
	cntz ++
	if err != nil {
		log.Printf("rpc.DialTCP: %v",err)
		return err
	}

	if err := client.Call(method,request,response); err != nil{
		_ = client.Close()
		log.Printf("client.Call%s  : %v",method, err)
		return err
	}
	_ = client.Close()
	return nil
}

func (np *Node_)create() {
	for i:=0;i<succNum;i++{
		np.Succ[i] = np.Address
	}
	np.Prec = nil
	go np.stabilize()
	fmt.Println("fin1")
	go np.fixFingers()
	fmt.Println("fin2")
	go np.checkPredecessor()
	time.Sleep(1000 * time.Millisecond)
}

func (np *Node_) join(tp *Node_) {
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor", tp.Address.ID, &tp.Succ[0])
	np.Prec = nil

	fmt.Println("fin1")
	var succList [succNum+1] FgType
	junk := new(int)
	_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.InheritSucc",junk,&succList)
	for i:=1;i<=succNum;i++ {
		tp.Succ[i] = succList[i-1]
	}

	fmt.Println("fin2")
	//	fmt.Println(tp.Address.IP,tp.Succ.IP)
	//	fmt.Println(tp.Address.ID,tp.Succ.ID)
	go tp.stabilize()
	go tp.fixFingers()
	go tp.checkPredecessor()
	fmt.Println("fin3")

	time.Sleep(1000 * time.Millisecond)
}

func newClient(ip IPaddress) *Client{
	var np *Client
	np = new(Client)
	np.Node = newNode_(ip)


	var err error
	np.L,err = net.Listen("tcp",ip.Address+":"+ip.Port)
	if err != nil {
		fmt.Println(err)
	}


	np.Server = rpc.NewServer()
	err = np.Server.Register(np.Node)
	if err != nil {
		fmt.Println(err)
	}
	go np.Server.Accept(np.L)

	return np
}