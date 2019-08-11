package chord


import (
	"strconv"
)

func (np *dhtNode) Help() {
	HelpPrinter()
}

func (np *dhtNode)NewNode(port int){
	address := GetLocalAddress()
	newNode_(Makeip(address,strconv.Itoa(port)))
	*np = *newdhtNode(Makeip(address, strconv.Itoa(port)))
}

func NewNode(port int)*dhtNode{
	address := GetLocalAddress()
	newNode_(Makeip(address,strconv.Itoa(port)))
	return newdhtNode(Makeip(address, strconv.Itoa(port)))
}

func (np *dhtNode) Get(k string)(bool,string){
	address := GetLocalAddress()
	var ans string
	ans = get(Makeip(address, np.Node.Address.IP.Port), k)
	return true,ans
}

func (np *dhtNode) Put(k string,v string) bool {
	put(np.Node.Address.IP,k,v)
	return true
}


func (np *dhtNode)Del(k string) bool{
	deletedata(np.Node.Address.IP,k)
	return true
}

func (np *dhtNode)Run() {
//	fmt.Println(np.Node.Address.IP,np.Node.Succ[0])
//	go np.Node.stabilize()
//	go np.Node.fixFingers()
//	go np.Node.checkPredecessor()
//ß	time.Sleep(333*time.Millisecond)
}

func (np *dhtNode) Create() {
	np.Node.create()
}

func (np *dhtNode) Join(add string) bool{
	//fmt.Println("pos1")
	junk := new(int)
	_ = Call(add,"Node_.Joinout",np.Node,junk)
//	fmt.Println(np.Node.Prec.IP.Port,np.Node.Address.IP.Port,np.Node.Succ[0].IP.Port,np.Node.Address.ID)
	//fmt.Println("pos2")
	//				fmt.Println(node0.Node.Address)
	return true
}

func (np *dhtNode) Quit() {
	junk1 := new(int)
	junk2 := new(int)
	_ = np.L.Close()
	np.Node.Quit(junk1,junk2)
}

func (np *dhtNode) ForceQuit() {
	junk1 := new(int)
	junk2 := new(int)
	_ = np.L.Close()
	np.Node.Quit(junk1,junk2)
}

func (np *dhtNode) Ping(add string) bool {
	var i int
	for i=0;add[i]!=':';i++{}
	len := len(add)
	s1:=add[0:i]
	s2 := add[i+1:len]
	return np.Node.ping(Makeip(s1,s2))
}

func (np *dhtNode) Dump (){

}

func (np *dhtNode)AppentTo() {

}
func (np *dhtNode) RemoveFrom(){

}
