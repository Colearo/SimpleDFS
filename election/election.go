package election

import (
	"fmt"
	"net"
	"simpledfs/membership"
	"simpledfs/utils"
	"time"
)

const (
	ElecTimeoutPeriod = 8000 * time.Millisecond
)

var ElectionPort string
var elecTimer *time.Timer
var msch chan uint32

type Elector struct {
	NodeID     utils.NodeID
	MemberList *membership.MemberList
}

func NewElector(nodeid utils.NodeID, memberlist *membership.MemberList) *Elector {
	elector := Elector{
		NodeID:     nodeid,
		MemberList: memberlist,
	}
	return &elector
}

func sendUDP(addr string, packet []byte) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer conn.Close()

	conn.Write(packet)
}

func (e *Elector) listener() {
	udpAddr, _ := net.ResolveUDPAddr("udp4", ":"+ElectionPort)
	uconn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer uconn.Close()

	for {
		packet := make([]byte, 256)
		n, addr, err := uconn.ReadFromUDP(packet)
		if err != nil {
			fmt.Println(err.Error())
		}

		e.handler(packet[:n], addr)
	}
}

func (e *Elector) handler(packet []byte, addr *net.UDPAddr) {
	if string(packet[:]) == "election" {
		if e.NodeID.IP > utils.BinaryIP(addr.IP.String()) {
			sendUDP(addr.IP.String()+":"+ElectionPort, []byte("ok"))
		}

	} else if string(packet[:]) == "ok" {
		stop := elecTimer.Stop()
		if stop {
			fmt.Printf("%s has higher IP, %s's election stops\n", addr.IP.String(), utils.StringIP(e.NodeID.IP))
		}

	} else if string(packet[:]) == "coordinator" {
		fmt.Printf("%s becomes new master\n", addr.IP.String())
		// Code to notify other rotine the new master
		msch <- utils.BinaryIP(addr.IP.String())

	} else {
		fmt.Println("[electon] unknown packet")
	}
}

// Initiate an election when the node detects the master failed
func (e *Elector) Election() {
	for i := 0; i < e.MemberList.Size(); i++ {
		member := e.MemberList.Members[i]
		if e.NodeID.IP < member.IP {
			fmt.Printf("send election to %s\n", utils.StringIP(member.IP))
			sendUDP(utils.StringIP(member.IP)+":"+ElectionPort, []byte("election"))
		}
	}

	// Set a timer, if not OK response within timeout, the election sender becomes master
	elecTimer = time.NewTimer(ElecTimeoutPeriod)
	go func() {
		<-elecTimer.C
		fmt.Printf("%s elected as the master\n", utils.StringIP(e.NodeID.IP))
		e.Coordination()
	}()
}

func (e *Elector) Coordination() {
	for i := 0; i < e.MemberList.Size(); i++ {
		member := e.MemberList.Members[i]
		sendUDP(utils.StringIP(member.IP)+":"+ElectionPort, []byte("coordinator"))
	}
}

func (e *Elector) Start(port string, mch chan uint32) {
	ElectionPort = port
	msch = mch
	fmt.Println("election service start")
	e.listener()
}
