package main

import (
	"os"
	"sort"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"bufio"
	"encoding/binary"
	"bytes"
	"strconv"
	"strings"
	"go/constant"
	"errors"
)

const (
	APP_BASE_PATH = "./"
	APP_INITIAL_FILENAME  = "data.*.log"
	CONN_HOST = "localhost"
	CONN_PORT = "3280"
	CONN_TYPE = "tcp"
)
type (
	conn_count int
)

var (
	conn_counter conn_count
	incoming_buf = make([]byte, 1024)
)

func main(){

}

func startTCP(){
	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + CONN_HOST + ":" + CONN_PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn)
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn)() {
	// Close the connection when you're done with it.
	defer conn.Close()
	defer func(){
		recover()
	}()
	if conn_counter == 6{
		conn.Close()
		fmt.Println("Error: To many connections")
		return
	}

	conn_counter = conn_counter + 1
	// Read the incoming connection into the buffer.

	if buflen, err := conn.Read(incoming_buf); err != nil {
		fmt.Println("Error reading:", err.Error())
	}else{
		if buflen > 0 {
			cleanBufIndex, err := checkLeadingZeros(incoming_buf)
			if err != nil {
				panic(err)
				return
			}
			// Send a response back to person contacting us.
			conn.Write(incoming_buf[cleanBufIndex:])
		}else{
			fmt.Println("Failed as input is empty")
		}
	}
}

func checkLeadingZeros (inputByteArray []byte)(indexZeroCounter int,err error){
	valueLen := len(inputByteArray)
	for i:=0; i < valueLen;i++ {
		incrementSliceValueBuf := i + 1
		if incrementSliceValueBuf > valueLen {
			incrementSliceValueBuf = incrementSliceValueBuf - 1
		}
		leadingZeroCheck := string(inputByteArray[i:incrementSliceValueBuf])
		leadingZeroCheckInt, err := strconv.Atoi(leadingZeroCheck)
		if err != nil {
			return 0,fmt.Errorf("Value is not an int %s\n",leadingZeroCheck)
			break
		}
		if leadingZeroCheckInt == 0 {
			indexZeroCounter  = indexZeroCounter+1
		} else if leadingZeroCheckInt > 0 {
			fmt.Printf("%d leading zeros\n",indexZeroCounter)
			break
		}
		fmt.Printf("check: %s\nisZero: %d\n",leadingZeroCheck,indexZeroCounter)
	}
	fmt.Printf("final value: %s\n",string(inputByteArray[indexZeroCounter:]))
	return indexZeroCounter,nil
}

func readDirNames(dirname string) ([]string, error) {
	f, err := os.Open(dirname)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	names, err := f.Readdirnames(-1)

	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

func RetrieveContents(name string) ([]byte, error){
	fmt.Printf("retrieve file data %s \n",name)
	f,err := os.OpenFile(name,os.O_RDONLY,0)
	defer f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr,"%v, Can't open %s: error %\n",os.Args[0],name,err)
		os.Exit(1)
		return nil,err
	}
	contents,err := ioutil.ReadAll(f)
	if err != nil{
		log.Fatal(err)
		return nil,err
	}
	return contents,nil
}




