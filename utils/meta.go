package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Meta map[string]Infos

type Infos []Info

type Info struct {
	Timestamp uint64
	Filesize  uint64
	DataNodes []uint8
}

func NewMeta(filename string) Meta {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err.Error())
	}
	defer file.Close()

	meta := Meta{}
	b, _ := ioutil.ReadAll(file)
	json.Unmarshal(b, &meta)

	return meta
}

func (meta Meta) FileInfo(filename string) Info {
	return meta[filename][0]
}

func (meta Meta) PutFileInfo(filename string, info Info) {
	meta[filename] = append(meta[filename], info)
}

func (meta Meta) SortFileInfo(filename string) {
	
}



// Test client
func main() {
	meta := NewMeta("meta3.json")

	info := Info{
		Timestamp: 20,
		Filesize: 32,
		DataNodes: []uint8{1,2,3,4},
	}

	meta.PutFileInfo("file1", info)

	fmt.Println(meta["file1"][3])
}




