package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"simpledfs/datanode"
	"simpledfs/election"
	"simpledfs/master"
	"simpledfs/membership"
	"simpledfs/utils"
)

var masterIP string

func detector(elector *election.Elector, tch chan uint64, ich chan uint32, mch chan uint32, dch chan string, dfc chan uint32) {
	for {
		select {

		case <-tch:
			// node failed
			ip := <-ich
			fmt.Printf("node %s failed\n", utils.StringIP(ip))

			if utils.StringIP(ip) == masterIP {
				fmt.Printf("master failed. election start\n")
				elector.Election()
			} else {
				dfc <- ip // notify master the failure node's ip
			}

		case mip := <-mch:
			// new master elected
			fmt.Printf("daemon has new master ip %s\n", utils.StringIP(mip))
			masterIP = utils.StringIP(mip)
			dch <- masterIP

		default:

		}
	}
}

func main() {
	masterIpPtr := flag.String("master", "127.0.0.1", "Master's IP")
	masternodePortPtr := flag.Int("mn-port", 5000, "MasterNode serving port")
	membershipPortPtr := flag.Int("mem-port", 5001, "Membership serving port")
	datanodePortPtr := flag.Int("dn-port", 5002, "DataNode serving port")
	flag.Parse()
	masterIP = *masterIpPtr
	masternodePort := *masternodePortPtr
	membershipPort := *membershipPortPtr
	datanodePort := *datanodePortPtr
	masterIP = utils.LookupIP(masterIP)
	localIP := utils.GetLocalIP().String()

	tsch := make(chan uint64) // channel for membership to notify deamon the failure node's timestamp
	ipch := make(chan uint32) // channel for membership to notify deamon the failure node's ip
	msch := make(chan uint32) // channel for elector to  notify daemon the new master's binary ip
	dnch := make(chan string) // channel for daemon to notify data node the new master's string ip
	dfch := make(chan uint32) // channel for daemon to notify master the failure node's ip

	// remove all replica files when rejoin
	files, err := filepath.Glob("./*:*")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}

	if membership.Initilize() == true {
		fmt.Printf("[INFO]: distibuted file system service start\n")
	} else {
		fmt.Printf("[ERROR]: distibuted file system service start failed\n")
		return
	}

	if masterIP == localIP {
		masterNode := master.NewMasterNode(fmt.Sprintf("%d", masternodePort), uint16(datanodePort), membership.MyList)
		go masterNode.Start(dfch)
	}

	nodeID := utils.NodeID{Timestamp: membership.MyMember.Timestamp, IP: membership.MyMember.IP}
	node := datanode.NewDataNode(fmt.Sprintf("%d", datanodePort), membership.MyList, nodeID)
	go node.Start(masterIP, dnch)

	elector := election.NewElector(nodeID, membership.MyList)
	go elector.Start("5003", msch) // 5003 is elector's port

	go detector(elector, tsch, ipch, msch, dnch, dfch)

	membership.Start(masterIP, fmt.Sprintf("%d", membershipPort), tsch, ipch)
}
