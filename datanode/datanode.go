package datanode

import (
	"fmt"
	"net"
	"simpledfs/membership"
	"simpledfs/utils"
	// "time"
	// "sync"
	// "bytes"
	"errors"
	"io"
	"os"
)

var meta utils.Meta

const (
	BufferSize = 4096
)

type dataNode struct {
	NodeID     utils.NodeID
	NodePort   string
	MemberList *membership.MemberList
}

func NewDataNode(port string, memberList *membership.MemberList, nodeID utils.NodeID) *dataNode {
	dn := dataNode{NodePort: port, MemberList: memberList, NodeID: nodeID}
	return &dn
}

func (dn *dataNode) Listener() {
	ln, err := net.Listen("tcp", ":"+dn.NodePort)
	if err != nil {
		fmt.Println(err.Error())
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err.Error())
		}
		go dn.Handler(conn)
	}
}

func (dn *dataNode) Handler(conn net.Conn) {
	defer conn.Close()

	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		fmt.Println(err.Error())
	}

	if buf[0] == utils.WriteRequestMsg {
		// Receive write request from client
		msg := utils.WriteRequest{}
		utils.Deserialize(buf[:n], &msg)
		dn.fileReader(conn, msg)

	} else if buf[0] == utils.ReadRequestMsg {
		// Receive read request from client
		msg := utils.ReadRequest{}
		utils.Deserialize(buf[:n], &msg)
		dn.fileWriter(conn, msg)

	} else if buf[0] == utils.CopyRequestMsg {
		// Receive copy request from master node
		msg := utils.CopyRequest{}
		utils.Deserialize(buf[:n], &msg)
		dn.fileCopyer(conn, msg)
	}
}


func (dn *dataNode) fileCopyer(conn net.Conn, cr utils.CopyRequest) {
	defer conn.Close()

	hashFilename := utils.Hash2Text(cr.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", cr.Timestamp)
	filename := hashFilename + ":" + timestamp // Local filename

	// Check file existence
	hasFile := false
	infos, exist := meta[hashFilename]
	if exist {
		for _, info := range infos {
			if cr.Timestamp == info.Timestamp {
				hasFile = true
				break
			}
		}
	}

	if hasFile {
		// send copy to next hop 
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer file.Close()

		nodeID, err := dn.getNextNodeID(cr.DataNodeList[:])
		if err != nil {
			fmt.Println(err.Error())
		}
		nextNodeConn, err := net.Dial("tcp", utils.StringIP(nodeID.IP)+":"+dn.NodePort)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer nextNodeConn.Close()

		nextNodeConn.Write(utils.Serialize(cr)) // Send copy request

		// Wait for next hop's reply
		buf := make([]byte, BufferSize)
		n, err := nextNodeConn.Read(buf)
		for string(buf[:n]) != "OK" {}
		fmt.Printf("node %v ready to receive file", nodeID)

		buf = make([]byte, BufferSize)
		for {
			n, err := file.Read(buf)
			conn.Write(buf[:n])
			if err == io.EOF {
				fmt.Printf("send file %s to %s finish\n", filename, utils.StringIP(nodeID.IP))
				break
			}
		}


	} else {
		// receive copy from copy sender and send copy to the next hop if exists
		filesize := cr.Filesize

		file, err := os.Create(filename)
		if err != nil {
			fmt.Println(err.Error())
		}
		defer file.Close()

		hasNextNode := true
		var nextNodeConn net.Conn
		nodeID, err := dn.getNextNodeID(cr.DataNodeList[:])
		if err != nil {
			fmt.Println(err.Error())
			hasNextNode = false		
		} else {
			nextNodeConn, err = net.Dial("tcp", utils.StringIP(nodeID.IP)+":"+dn.NodePort)
			if err != nil {
				fmt.Println(err.Error())
			}
			defer nextNodeConn.Close()
			nextNodeConn.Write(utils.Serialize(cr))

			// Wait for next hop's reply
			buf := make([]byte, BufferSize)
			n, _ := nextNodeConn.Read(buf)
			for string(buf[:n]) != "OK" {}
			fmt.Printf("node %v ready to receive file", nodeID)
		}

		conn.Write([]byte("OK"))

		buf := make([]byte, BufferSize)
		var receivedBytes uint64
		for {
			n, err := conn.Read(buf)
			file.Write(buf[:n])
			receivedBytes += uint64(n)

			if hasNextNode {
				nextNodeConn.Write(buf[:n])
			}

			if err == io.EOF {
				fmt.Printf("receive file %s finish", filename)
				break
			}
		}

		if receivedBytes != filesize {
			fmt.Println("file size unmatch")
		} else {
			info := utils.Info{
				Timestamp: cr.Timestamp,
				Filesize: cr.Filesize,
				DataNodes: cr.DataNodeList[:],
			}
			meta.PutFileInfo(hashFilename, info)
			meta.StoreMeta("meta.json")
			fmt.Printf("put %s with ts %d into meta list\n", hashFilename, cr.Timestamp)

			// Tell master node it receives a new file
		}
	}
}


// Receive remote file from cleint, store it in local and send it to next hop if possible
func (dn *dataNode) fileReader(conn net.Conn, wr utils.WriteRequest) {
	filesize := wr.Filesize
	// Create local filename from write request
	hashFilename := utils.Hash2Text(wr.FilenameHash[:])
	timestamp := fmt.Sprintf("%d", wr.Timestamp)
	filename := hashFilename + ":" + timestamp

	fmt.Println("filename: ", filename)

	// Create file descriptor
	file, err := os.Create(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	// Check whether next node exists
	hasNextNode := true
	nextNodeConn, err := dn.dialDataNode(wr)
	if err != nil {
		fmt.Println(err.Error())
		hasNextNode = false
	} else {
		fmt.Println("next node addr: ", (*nextNodeConn).RemoteAddr().String())
		defer (*nextNodeConn).Close()
	}

	conn.Write([]byte("OK")) // Send OK to notify file sender I am ready to accept

	// Read file data from connection and write to local
	buf := make([]byte, BufferSize)
	var receivedBytes uint64
	for {
		n, err := conn.Read(buf)
		file.Write(buf[:n])
		receivedBytes += uint64(n)

		// Send file data to next node
		if hasNextNode {
			(*nextNodeConn).Write(buf[:n])
		}

		if err == io.EOF {
			fmt.Printf("receive file %s finish\n", filename)
			break
		}
	}

	// File size check
	if receivedBytes != filesize {
		fmt.Println("file size unmatch")
	} else {
		info := utils.Info{
			Timestamp: wr.Timestamp,
			Filesize:  wr.Filesize,
			DataNodes: wr.DataNodeList[:],
		}
		meta.PutFileInfo(hashFilename, info)
		meta.StoreMeta("meta.json")
		fmt.Printf("put %s with ts %d into meta list\n", hashFilename, wr.Timestamp)

		// Tell master it receives a file
		// dialMasterNode()
	}
}

// Send local file to client
func (dn *dataNode) fileWriter(conn net.Conn, rr utils.ReadRequest) {
	defer conn.Close()

	// Retrieve local filename from read request and meta data
	filename := utils.Hash2Text(rr.FilenameHash[:])
	info, ok := meta.FileInfo(filename)
	if ok == false {
		conn.Write([]byte(" "))
		fmt.Println("local file requested not found")
		return
	}
	timestamp := fmt.Sprintf("%d", info.Timestamp)
	filename = filename + ":" + timestamp

	// Send file to client
	file, err := os.OpenFile(filename, os.O_RDONLY, 0755)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	buf := make([]byte, BufferSize)
	for {
		n, err := file.Read(buf)
		conn.Write(buf[:n])
		if err == io.EOF {
			fmt.Printf("send file %s finish\n", filename)
			break
		}
	}
}

func (dn *dataNode) dialMasterNode(masterID uint8, filenameHash [32]byte, filesize uint64, timestamp uint64) {
	conn, err := net.Dial("tcp", ":8000")
	if err != nil {
		fmt.Println(err.Error())
	}
	defer conn.Close()

	wc := utils.WriteConfirm{
		MsgType:      utils.WriteConfirmMsg,
		FilenameHash: filenameHash,
		Filesize:     filesize,
		Timestamp:    timestamp,
		DataNode:     dn.NodeID,
	}

	conn.Write(utils.Serialize(wc))
}

// Dial DataNode with WriteRequest
func (dn *dataNode) dialDataNode(wr utils.WriteRequest) (*net.Conn, error) {
	nodeID, err := dn.getNextNodeID(wr.DataNodeList[:])
	if err != nil {
		return nil, err
	}

	conn, err := net.Dial("tcp", utils.StringIP(nodeID.IP)+":"+dn.NodePort)
	if err != nil {
		return nil, err
	}

	// Send write request to the next hop
	conn.Write(utils.Serialize(wr))

	// Wait for next hop's reply
	buf := make([]byte, BufferSize)
	n, err := conn.Read(buf)
	for string(buf[:n]) != "OK" {}
	fmt.Printf("node %v ready to receive file", nodeID)

	return &conn, nil
}

func (dn *dataNode) getNextNodeID(nodeList []utils.NodeID) (utils.NodeID, error) {
	for k, v := range nodeList {
		if v == dn.NodeID && k < len(nodeList)-1 &&
			nodeList[k+1].IP != 0 && nodeList[k+1].Timestamp != 0 {
			return nodeList[k+1], nil
		}
	}
	return utils.NodeID{}, errors.New("Nexthop doesn't exists")
}

func (dn *dataNode) Start() {
	meta = utils.NewMeta("meta.json")

	dn.Listener()
}
