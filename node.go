package main

import (
	"errors"
	"fmt"
	"log"
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
}

func newNode_(ip IPaddress) *Node_{
	var np *Node_
	np = new(Node_)
	np.Address.IP=ip
	np.Address.ID=hashString(ip.Port)
	np.alive = true
	np.Datalist = make(map[string]string)
	np.DatalistBackup = make(map[string]string)
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
	np.Prec = pre
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

func (np *Node_) ping (nip IPaddress) bool{
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
	return false
}

func (np *Node_) GetWorkingSucc(junk *int,fin *FgType)error {
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
	return nil
}

func (np *Node_) getClosestClient(tp *big.Int) *FgType {
	for i:=m;i>=1;i-- {
		if np.FgT[i].ID != nil {
			if between(np.Address.ID, np.FgT[i].ID, tp, false) {
				return &np.FgT[i]
			}
		}
	}
	return &np.Succ[0]
}

func (np *Node_) FindSuccessor(tp *big.Int,fin *FgType) error {
	if between(np.Address.ID,tp,np.Succ[0].ID,true){
		*fin = np.Succ[0]
		return nil
	}
	nxtP := np.getClosestClient(tp)
	_ = Call(nxtP.IP.Address+":"+nxtP.IP.Port,"Node_.FindSuccessor",tp,fin)


	var aliveFin FgType
	junk1 := new(int)
	_ = Call(fin.IP.Address+":"+ fin.IP.Port,"Node_.GetWorkingSucc",junk1,&aliveFin)
	*fin = aliveFin
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

func (np *Node_) Notify(tp *Node_,junk *int) error {
	if (np.Prec == nil) || (between(np.Prec.ID,tp.Address.ID,np.Address.ID,false)) {
		np.Prec = new(FgType)
		*np.Prec = tp.Address
		list := make(map[string]string)
		junk := new(int)
		_ = Call(np.Prec.IP.Address+":"+np.Prec.IP.Port,"Node_.GetDatalist",junk,list)
		np.backuplocker.Lock()
		np.DatalistBackup = list
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
			return
		}
		np.succlocker.Lock()
		if between(np.Address.ID, pre.ID, np.Succ[0].ID, true) {
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
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.Notify",np,junk)
}

func (np *Node_) stabilize() {
	for np.alive{
		np.sta()
		time.Sleep(333 * time.Millisecond)
	}
}


func (np *Node_) fixF() {
	for i:=1;i<=m;i++ {
		_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",jump(np.Address.ID,i),&np.FgT[i])
		time.Sleep(3*time.Millisecond)
	}
/*	var inheritSucc [succNum + 1]FgType
	junk := new(int)
	_ = Call(np.Succ[0].IP.Address+":"+np.Succ[0].IP.Port,"Node_.InheritSucc",junk,&inheritSucc)*/
}

func (np *Node_) fixFingers() {
	for np.alive{
		np.fixF()
		time.Sleep(333 * time.Millisecond)
	}
}

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
		for k, v := range np.DatalistBackup{
			np.Datalist[k] = v
		}
		np.backuplocker.Unlock()
		var succ FgType
		_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",np.Address.ID,&succ)
		_ = Call(succ.IP.Address+":"+succ.IP.Port,"Node_.PutListBackup",&np.DatalistBackup,junk)


		np.DatalistBackup = make(map[string]string)
	}
}

func (np *Node_) checkPredecessor(){
	for np.alive{
		np.fixPre()
		time.Sleep(333 * time.Millisecond)
	}
}

func (np *Node_) Quit (junk1 *int,junk2 *int) error {
	np.alive = false
	time.Sleep(333*time.Millisecond)
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
	np.valuelocker.Lock()
	np.Datalist[data.Key] = data.Val
	np.valuelocker.Unlock()
	return nil
}

func (np *Node_) NodePutBackup(data *KV,junk *int) error {
	np.backuplocker.Lock()
	np.DatalistBackup[data.Key] = data.Val
	np.backuplocker.Unlock()
	return nil
}

func (np *Node_) PutList(list *map[string]string,junk *int) error {
	np.valuelocker.Lock()
	for k,v := range *list{
		np.Datalist[k] = v
	}
	np.valuelocker.Unlock()
	return nil
}

func (np *Node_) PutListBackup(list *map[string]string,junk *int) error {
	np.backuplocker.Lock()
	for k,v := range *list{
		np.DatalistBackup[k] = v
	}
	np.backuplocker.Unlock()
	return nil
}

func (np *Node_) Put(data KV,junk *int) error{
	var tp FgType
	nval := hashString(data.Key)
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",nval,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodePut",data,junk)
	return nil
}

func (np *Node_) NodeGet(nkey string,fin *string)error {
	var err bool
	*fin ,err= np.Datalist[nkey]
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

func (np *Node_) Get(key string,val *string) error{
	var tp FgType
	nval := hashString(key)
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",nval,&tp)
	err := Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeGet",key,val)
	if err != nil {
		_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",tp.ID,&tp)
		err = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeGetBackup",key,val)
	}
	return err
}

func (np *Node_) NodeDelete(nkey string,junk *int)error {
	if _, ok := np.Datalist[nkey]; ok != true {
		return nil
	}
	np.valuelocker.Lock()
	delete(np.Datalist, nkey)
	np.valuelocker.Unlock()
	return nil
}

func (np *Node_) NodeDeleteBackup(nkey string,junk *int)error {
	if _, ok := np.DatalistBackup[nkey]; ok != true {
		return nil
	}
	np.backuplocker.Lock()
	delete(np.DatalistBackup, nkey)
	np.backuplocker.Unlock()
	return nil
}

func (np *Node_) Delete(key string,junk *int) error {
	var tp FgType
	nval := hashString(key)
	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",nval,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeDelete",key,junk)

	_ = Call(np.Address.IP.Address+":"+np.Address.IP.Port,"Node_.FindSuccessor",tp.ID,&tp)
	_ = Call(tp.IP.Address+":"+tp.IP.Port,"Node_.NodeDeleteBackup",key,junk)
	return nil
}