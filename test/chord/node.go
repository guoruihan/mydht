package chord

import (
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"
)
type IPaddress struct{
	Address,Port string
}
type FgType struct{
	IP IPaddress
	ID *big.Int
}
type Node_ struct {
	Datalist map[string] string
	DatalistBackup map[string] string
	FgT [165] FgType
	Prec *FgType
	Address FgType
	Succ [succNum + 1]FgType
	alive bool
	succlocker sync.Mutex
	valuelocker sync.Mutex
	backuplocker sync.Mutex

	fingerlocker sync.Mutex
	stalocker sync.Mutex
	chkprelocker sync.Mutex
}

func newNode_(ip IPaddress) *Node_{
	var np *Node_
	np = new(Node_)
	np.Address.IP=ip
	np.Address.ID=hashString(ip.Port)
	np.alive = true
	np.Datalist = make(map[string]string)
	np.DatalistBackup = make(map[string]string)
	np.Succ[0] = np.Address
	np.Prec = new(FgType)
	return np
}

func (np *Node_) GetPrec (junk *int,pre *FgType) error {
	if np.Prec == nil {
		pre = nil
		return nil
	}
	*pre = *np.Prec
	return nil
}

func (np *Node_) GetSucc (pos int,succ *FgType) error {
	np.succlocker.Lock()
	*succ = np.Succ[pos]
	np.succlocker.Unlock()
	return nil
}

func (np *Node_) GetDatalist (junk *int,fin *map[string]string) error{
	np.valuelocker.Lock()
	*fin = np.Datalist
	np.valuelocker.Unlock()
	return nil
}

func (np *Node_) ModifyPrec (pre *FgType,junk *int) error{
	*np.Prec = *pre
	return nil
}

func (np *Node_) ModifySucc (succ *FgType,junk *int) error{
	np.Succ[0] = *succ
	return nil
}

func (np *Node_) CheckAlive(junk *int,tag *bool) error {
	*tag = np.alive
	return nil
}

func (np Node_)InheritSucc(junk *int,nSucc *[succNum + 1]FgType) error{
	*nSucc = np.Succ
	return nil
}

var one = big.NewInt(1)
func (np *Node_) ping (nip IPaddress) bool{
	var succ FgType
	_ = Call(nip.Address+":"+nip.Port,"Node_.FindSuccessor", new(big.Int).Sub(hashString(nip.Port),one),succ)

	if succ.IP.Port != nip.Port{
		return false
	}
	return true
	/*

	tag := new(bool)
	junk := new(int)
	if nip.Address == "" {
		return false
	}
	for i:=1;i<=pingTim; i++ {
		//		fmt.Println(nip,i)
		e := Call(nip.Address+":"+nip.Port,"Node_.CheckAlive",junk,tag)
		if e != nil {
			continue
		}
		return *tag
	}
	return false*/
}

func (np *Node_) GetWorkingSucc(junk *int,fin *FgType)error {
	*fin = np.Succ[0]
	return nil
	/*
	var i int
	np.succlocker.Lock()
	for i=0;i<succNum;i++ {
		//		fmt.Println(np.Succ[i].IP,"rua",i)
		if np.ping(np.Succ[i].IP) {
			break
		}
	}
	if i == 0 {
		*fin = np.Succ[0]
		np.succlocker.Unlock()
		return nil
	}
	if i == succNum {
		log.Println("no alive succ!")
		np.succlocker.Unlock()
		return nil
	}

	np.Succ[0] = np.Succ[i]
	var succList  [succNum+1] FgType
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.InheritSucc",junk,&succList)
	for j:=1;j<succNum;j++ {
		np.Succ[j] = succList[j-1]
	}
	*fin = np.Succ[0]
	np.succlocker.Unlock()
	return nil*/
}

func (np *Node_) getClosestdhtNode(tp *big.Int) *FgType {
/*	for i:=m;i>=1;i-- {
		if np.FgT[i].ID != nil {
			if between(np.Address.ID, np.FgT[i].ID, tp, false) {
				return &np.FgT[i]
			}
		}
	}*/
	return &np.Succ[0]
}

func (np *Node_) FindSuccessor(tp *big.Int,fin *FgType) error {

	var succ FgType

	junk := new(int)

	np.GetWorkingSucc(junk,&succ)

	if between(np.Address.ID,tp,np.Succ[0].ID,true){
		*fin = np.Succ[0]
		return nil
	}

	if np.Address.ID.Cmp(tp) == 0 && np.Address.ID.Cmp(succ.ID)==0 {
		*fin = succ
		return nil
	}

	nxtP := np.getClosestdhtNode(tp)
	_ = Call(nxtP.IP.Address+":"+nxtP.IP.Port,"Node_.FindSuccessor",tp,fin)


	/*	var aliveFin FgType
		junk1 := new(int)
		_ = Call(fin.IP.Address+":"+ fin.IP.Port,"Node_.GetWorkingSucc",junk1,&aliveFin)
		*fin = aliveFin*/
	return nil
}

func (np *Node_) RefreshDatalist(tp FgType,junk *int) error {
	var trash []string
	np.valuelocker.Lock()
	for k := range np.Datalist {
		if between(np.Prec.ID,hashString(k),tp.ID,true) {
			trash = append(trash,k)
		}
	}

	for _,k := range trash {
		delete(np.Datalist,k)
	}
	np.valuelocker.Unlock()
	return nil
}

/*func (np *Node_) Notify(tp *Node_,junk *int) error {
	if (np.Prec == nil) || (between(np.Prec.ID,tp.Address.ID,np.Address.ID,false)) {
		np.Prec = new(FgType)
		*np.Prec = tp.Address
		list := make(map[string]string)
		junk := new(int)
		err := Call(tp.Address.IP.Address+":"+tp.Address.IP.Port,"Node_.GetDatalist",junk,&list)
		np.backuplocker.Lock()
		if err == nil {
			np.DatalistBackup = list
		}
		np.backuplocker.Unlock()
		return nil
	}
	return nil
}

func (np *Node_) sta() {
	var succ FgType
	pre := new(FgType)
	junk := new(int)
	err := np.GetWorkingSucc(junk,&succ)

	if succ.ID == nil || err != nil {
		return
	}

	//	fmt.Println(np.Address.IP,np.Succ.IP)
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.GetPrec",junk,pre)
	if np.ping(pre.IP) {
		//		fmt.Println(np.Address.ID,pre.ID,np.Succ.ID)
		if pre.ID == nil {

		} else {
			np.succlocker.Lock()
			if between(np.Address.ID, pre.ID, np.Succ[0].ID, false) {
				np.Succ[0] = *pre
				var succList [succNum+1] FgType
				junk := new(int)
				_ = Call(pre.IP.Address+":"+pre.IP.Port,"Node_.InheritSucc",junk,&succList)
				for i:=1;i<succNum;i++ {
					np.Succ[i] = succList[i-1]
				}
			}
			np.succlocker.Unlock()
		}
	}
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.Notify",np,junk)


	var succList  [succNum+1] FgType
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.InheritSucc",junk,&succList)
	np.succlocker.Lock()
	for j:=1;j<succNum;j++ {
		np.Succ[j] = succList[j-1]
	}
	np.succlocker.Unlock()
}

func (np *Node_) stabilize() {
	np.stalocker.Lock()
	for np.alive{
		np.sta()
		time.Sleep(100 * time.Millisecond)
	}
	np.stalocker.Unlock()
}
*/

func (np *Node_) fixF() {
	for i:=1;i<=m;i++ {
		_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",jump(np.Address.ID,i),&np.FgT[i])
	}
}

func (np *Node_) fixFingers() {
	np.fingerlocker.Lock()
	for np.alive{
		np.fixF()
		time.Sleep(1000 * time.Millisecond)
	}
	np.fingerlocker.Unlock()
}
/*
func (np *Node_) fixPre() {
	if np.Prec == nil {
		return
	}
	tag := new(bool)
	junk := new(int)
	_ = Call(np.Prec.IP.Address+":"+np.Prec.IP.Port,"Node_.CheckAlive",junk,tag)
	if *tag == false {
		np.Prec = nil
		np.backuplocker.Lock()
		np.valuelocker.Lock()
		for k, v := range np.DatalistBackup{
			np.Datalist[k] = v
		}
		np.valuelocker.Unlock()
		var succ FgType
		_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",np.Address.ID,&succ)

		if succ.IP.Port == np.Address.IP.Port {

		} else {
			_ = Call(succ.IP.Address+":"+succ.IP.Port, "Node_.PutListBackup", np.DatalistBackup, junk)
		}
		np.DatalistBackup = make(map[string]string)
		np.backuplocker.Unlock()
	}
}

func (np *Node_) checkPredecessor(){
	np.chkprelocker.Lock()
	for np.alive{
		np.fixPre()
		time.Sleep(100 * time.Millisecond)
	}
	np.chkprelocker.Unlock()
}*/

func (np *Node_) Quit (junk1 *int,junk2 *int) error {
//	fmt.Println("quit",np.Address.IP.Port)
	np.alive = false
//	fmt.Println(np.Prec.IP.Port, np.Address.IP.Port,np.Succ[0].IP.Port)
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.PutList",np.Datalist,junk1)
	var pre,succ FgType
	succ = np.Succ[0]
	pre = *np.Prec
	if np.Prec != nil {
		_ = Call(succ.IP.Address+":"+succ.IP.Port,"Node_.ModifyPrec",pre,junk1)
	}
	_ = Call(pre.IP.Address+":"+pre.IP.Port,"Node_.ModifySucc",succ,junk1)
	time.Sleep(1000*time.Millisecond)
	return nil
}

func (np *Node_) Prt(st string,junk2 *int) error {
	fmt.Println(np.Address.IP,"rua")
	time.Sleep(2*time.Second)
	if np.Succ[0].IP.Port == st {
		return nil
	}
	fmt.Println(np.Prec.IP,np.Address.IP,np.Succ[0].IP)
	junk := new(int)
	err := Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.Prt",st,junk)
	fmt.Println(err)
	fmt.Println(cntz)
	return err
}

type KV struct {
	Key,Val string
}

func makeKV(key string,val string) KV{
	var fin KV
	fin.Key=key
	fin.Val=val
	return fin
}

func (np *Node_) NodePut(data *KV,junk *int) error{
	//	fmt.Println("input")
	np.valuelocker.Lock()
//	fmt.Println("put",data.Key,data.Val, np.Address.IP.Port)
	np.Datalist[data.Key] = data.Val
	np.valuelocker.Unlock()
	//	fmt.Println("output")
	return nil
}

func (np *Node_) NodePutBackup(data *KV,junk *int) error {
	//	fmt.Println("inbackup")
	np.backuplocker.Lock()
	np.DatalistBackup[data.Key] = data.Val
	np.backuplocker.Unlock()
	//	fmt.Println("outbackup")
	return nil
}

func (np *Node_) PutList(list map[string]string,junk *int) error {
	np.valuelocker.Lock()
	for k,v := range list{
		np.Datalist[k] = v
	}
	np.valuelocker.Unlock()
	return nil
}

func (np *Node_) PutListBackup(list map[string]string,junk *int) error {
	np.backuplocker.Lock()
	for k,v := range list{
		np.DatalistBackup[k] = v
	}
	np.backuplocker.Unlock()
	return nil
}

func (np *Node_) Put_(data KV,junk *int) error{
	var tp FgType
	nval := hashString(data.Key)
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",nval,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodePut",data,junk)
/*	var tmp FgType
	tmp = tp
	_ = Call(tmp.IP.Address+":"+tmp.IP.Port,"Node_.FindSuccessor",nval,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodePutBackup",data,junk)*/
	return nil
}

func (np *Node_) NodeGet(nkey string,fin *string)error {
	var err bool
//	fmt.Println("get",nkey,np.Address.IP.Port)
	np.valuelocker.Lock()
	*fin ,err= np.Datalist[nkey]
	np.valuelocker.Unlock()
	if err == false{
		return errors.New("not found key")
	}
	return nil
}

func (np *Node_) NodeGetBackup(nkey string,fin *string)error {
	np.backuplocker.Lock()
	*fin = np.DatalistBackup[nkey]
	np.backuplocker.Unlock()
	return nil
}

func (np *Node_) Get_(key string,val *string) error{
	var tp FgType
	nval := hashString(key)
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",nval,&tp)
	err := Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeGet",key,val)
/*	if err != nil {
		_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",tp.ID,&tp)
		err = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeGetBackup",key,val)
	}*/
	return err
}

func (np *Node_) NodeDelete(nkey string,junk *int)error {
	np.valuelocker.Lock()
	if _, ok := np.Datalist[nkey]; ok != true {
		np.valuelocker.Unlock()
		return nil
	}
	delete(np.Datalist, nkey)
	np.valuelocker.Unlock()
	return nil
}

func (np *Node_) NodeDeleteBackup(nkey string,junk *int)error {
	np.backuplocker.Lock()
	if _, ok := np.DatalistBackup[nkey]; ok != true {
		np.backuplocker.Unlock()
		return nil
	}
	delete(np.DatalistBackup, nkey)
	np.backuplocker.Unlock()
	return nil
}

func (np *Node_) Delete_(key string,junk *int) error {
	var tp FgType
	nval := hashString(key)
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",nval,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeDelete",key,junk)

/*	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",tp.ID,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeDeleteBackup",key,junk)*/
	return nil
}
