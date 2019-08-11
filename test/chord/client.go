package chord

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	//	"time"
)

type dhtNode struct{
	Node *Node_
	Server *rpc.Server
	L net.Listener
}

var cntz int

func Call(address string,method string,request interface{},response interface{}) error {
	dhtNode, err := rpc.Dial("tcp", address)
	cntz ++
//	fmt.Println(cntz)
	if err != nil {
		log.Printf("rpc.DialTCP: %v",err)
		return err
	}

	if err := dhtNode.Call(method,request,response); err != nil{
		_ = dhtNode.Close()
		log.Printf("dhtNode.Call%s  : %v",method, err)
		return err
	}
	_ = dhtNode.Close()
	return nil
}

func (np *Node_)create() {
	for i:=0;i<succNum;i++{
		np.Succ[i] = np.Address
	}
	np.Prec = new(FgType)
	*np.Prec = np.Address
	/*	go np.stabilize()
		go np.fixFingers()
		go np.checkPredecessor()*/
	//	time.Sleep(1000 * time.Millisecond)
}

func (np *Node_) Joinout(tp *Node_,junk *int)error{
	np.join(tp)
	return nil
	/*	_ = Call(np.Address+":"+np.Port,"Node_.FindSuccessor", tp.Address.ID, &tp.Succ[0])

		tp.Prec = nil

		tp.succlocker.Lock()
		var succList [succNum+1] FgType
		_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.InheritSucc",junk,&succList)
		for i:=1;i<=succNum;i++ {
			tp.Succ[i] = succList[i-1]
		}

		dataList:=make(map[string]string)
		_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.GetDatalist",junk,&dataList)



		tp.valuelocker.Lock()
		for k, v := range dataList {
			if between(tp.Succ[0].ID, hashString(k), tp.Address.ID, true) {
				tp.Datalist[k] = v
			}
		}

		tp.valuelocker.Unlock()
		tp.succlocker.Unlock()


		_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.RefreshDatalist",tp.Address,junk)*/
	//	fmt.Println(tp.Address.IP,tp.Succ.IP)
	//	fmt.Println(tp.Address.ID,tp.Succ.ID)
	/*	go tp.stabilize()
		go tp.fixFingers()
		go tp.checkPredecessor()
		fmt.Println("fin3")*/

	//	time.Sleep(1000 * time.Millisecond)
	//	return nil
}

func (np *Node_) join(tp *Node_) {
	//fmt.Println(np.Prec.IP.Port,np.Address.IP.Port,np.Succ[0].IP.Port,np.Address.ID)
	junk := new(int)
	var succ FgType
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor", tp.Address.ID, &succ)
	tp.Succ[0] = succ
	_ = Call(tp.Address.IP.Address+":"+tp.Address.IP.Port,"Node_.ModifySucc", succ, junk)
	var pre FgType
	_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.GetPrec",junk,&pre)
	_ = Call(tp.Address.IP.Address+":"+tp.Address.IP.Port,"Node_.ModifyPrec", pre, junk)
	*tp.Prec = pre
	//fmt.Println(tp.Prec.IP.Port,tp.Address.IP.Port,tp.Succ[0].IP.Port,tp.Address.ID)


	tp.succlocker.Lock()
	/*	tp.succlocker.Lock()
		var succList [succNum+1] FgType
		_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.InheritSucc",junk,&succList)
		for i:=1;i<=succNum;i++ {
			tp.Succ[i] = succList[i-1]
		}*/

	dataList:=make(map[string]string)
	_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.GetDatalist",junk,&dataList)

//	var pre FgType
//	_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.GetPrec",junk,&pre)

	tp.valuelocker.Lock()
	for k, v := range dataList {
		if between(pre.ID, hashString(k), tp.Address.ID, true) {
			_ = Call(tp.Address.IP.Address+":"+tp.Address.IP.Port,"Node_.NodePut",makeKV(k,v),junk)
		}
	}

	tp.valuelocker.Unlock()

	tp.succlocker.Unlock()


	_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.RefreshDatalist",tp.Address,junk)


	succ = tp.Succ[0]
	pre = *tp.Prec
	_ = Call(succ.IP.Address+":"+succ.IP.Port,"Node_.ModifyPrec",tp.Address,junk)
	_ = Call(pre.IP.Address+":"+pre.IP.Port,"Node_.ModifySucc",tp.Address,junk)
//	fmt.Println(np.Prec.IP.Port,np.Address.IP.Port,np.Succ[0].IP.Port,np.Address.ID)
//	fmt.Println(tp.Prec.IP.Port,tp.Address.IP.Port,tp.Succ[0].IP.Port,tp.Address.ID)
	//_ = Call(tp.Succ[0].IP.Address+":"+tp.Succ[0].IP.Port,"Node_.Notify",tp,junk)
	//	fmt.Println(tp.Address.IP,tp.Succ.IP)
	//	fmt.Println(tp.Address.ID,tp.Succ.ID)
	/*	go tp.stabilize()
		go tp.fixFingers()
		go tp.checkPredecessor()
		fmt.Println("fin3")*/

	//	time.Sleep(1000 * time.Millisecond)
}

func newdhtNode(ip IPaddress) *dhtNode{
	var np *dhtNode
	np = new(dhtNode)
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

func put(ip IPaddress,key string,val string){
	junk := new(int)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Put_",makeKV(key+"a",val),junk)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Put_",makeKV(key+"b",val),junk)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Put_",makeKV(key+"c",val),junk)
}

func get(ip IPaddress,key string) string{
	var err1,err2,err3 error
	var ans,ans1,ans2,ans3 string
	junk := new(int)
	err1 = Call(ip.Address+":"+ip.Port,"Node_.Get_",key+"a",&ans1)
	err2 = Call(ip.Address+":"+ip.Port,"Node_.Get_",key+"b",&ans2)
	err3 = Call(ip.Address+":"+ip.Port,"Node_.Get_",key+"c",&ans3)
	if err1 == nil || err2 == nil || err3 == nil {
		if err1 == nil{
			ans = ans1
		}
		if err2 == nil{
			ans = ans2
		}
		if err3 == nil{
			ans = ans3
		}
	}
	_ = Call(ip.Address+":"+ip.Port,"Node_.Put_",makeKV(key+"a",ans),junk)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Put_",makeKV(key+"b",ans),junk)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Put_",makeKV(key+"c",ans),junk)
	return ans
}

func deletedata(ip IPaddress,key string) {
	junk := new(int)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Delete_",key+"a",junk)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Delete_",key+"b",junk)
	_ = Call(ip.Address+":"+ip.Port,"Node_.Delete_",key+"c",junk)
}
