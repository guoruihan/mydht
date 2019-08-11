package chord

import (
	"fmt"
	"time"
)

var ins = false

const (
	m = 160
	succNum = 5
	pingTim = 3
	maxSteps = 160 * 1.5

)

func main() {
	var n int
	var s1, s2, s3 string
	fmt.Println("rua")
	_, _ = fmt.Scanf("%d", &n)
	address := GetLocalAddress()
	var node0 *dhtNode
	var port string
	//junk := new(int)
	for ; n > 0; n-- {
		_, _ = fmt.Scanln(&s1, &s2, &s3)
		fmt.Println(s1, "|", s2, "|", s3)
		switch s1 {
		case "help":
			HelpPrinter()
		case "port":
			if !ins {
				if s2 == "" {
					port = "3410"
				} else {
					port = s2
				}
				fmt.Println("port selected at " + port)
			} else {
				fmt.Println("you have already inserted")
			}
		case "create":
			if !ins {
				node0 = newdhtNode(Makeip(address, port))
				node0.Node.create()
				ins = true
			} else {
				fmt.Println("you have already inserted")
			}
		case "join":
			if !ins {
				fmt.Println("you haven't inserted")
			} else {
				var ndhtNode *dhtNode
				ndhtNode = newdhtNode(Makeip(address, s2))
				//				fmt.Println(node0.Node.Address)
				node0.Node.join(ndhtNode.Node)
			}
		case "prt":
			junk := new(int)
			_ = Call(address+":"+port, "Node_.Prt", port, junk)
		case "quit":
			junk := new(int)
			if s2 == port {
				//				_=Call(address+":"+s2,"Node_.Deleteall",port,junk)
				_ = Call(address+":"+s2, "Node_.Quit", junk, junk)
				return
			} else {
				_ = Call(address+":"+s2, "Node_.Quit", junk, junk)
			}
		case "put":
			put(Makeip(address, port), s2, s3)
			time.Sleep(2*time.Second)
		case "get":
			var ans string
			ans = get(Makeip(address, port), s2)
			fmt.Println(ans)
		case "delete":
			deletedata(Makeip(address, port), s2)
		}
	}
}
