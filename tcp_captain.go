package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	APP_BASE_PATH        = "./"
	APP_INITIAL_FILENAME = "data.*.log"
	APP_SHUTDOWN         = "shutdown"
	CONN_HOST            = "localhost"
	CONN_PORT            = "3280"
	CONN_TYPE            = "tcp"
)

type (
	 count int
	 Duration int
)

var (
	connCounter         count
	durationVal         Duration
	incomingBuf   =     make([]byte, 1024)
	gCachInputMap       bytes.Buffer
	counter             struct{
						   total count
						   totalSince count
					    }
	timer			    struct{
		 				   fiveSecInterval time.Timer
		 				   tenSecInterval time.Timer
		                }
	fileData            os.File
	err 				error
)

func init(){

	cleanLogs()
	fileData, err = createFile()
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	startTCP()
}

func interval5(){
	//do read out for uniq numbers in this interval of 10 seconds
	// do read out for uniq numbers total for server duration
	durationVal = 5e3
	time.sleep(durationVal)
	// do the work
	interval5()
}

func interval10(){
	durationVal = 10e3
	time.sleep(durationVal)
     // do the work
	interval10()
}

func startTCP() {
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
func handleRequest(conn net.Conn) {
	// Close the connection when you're done with it.
	defer func() {
		recover()
	}()


	if connCounter == 6 {
		conn.Close()
		fmt.Println("Error: To many connections")
		conn.Write([]byte("Error: To many connections"))
		return
	}
	connCounter = connCounter + 1
	// Read the incoming connection into the buffer.
	buflen, err := conn.Read(incomingBuf)
	if err != nil {
		fmt.Println("Error reading:", err.Error())
		conn.Close()
	} else {
		if buflen > 0 {
			 s_Value := string(incomingBuf[:buflen])
			if strings.ToLower(s_Value) == APP_SHUTDOWN {
				conn.Write([]byte("Bye Bye monte\n"))
				conn.Close()
				return
			}else if strings.IndexRune(s_Value, '\n') > -1 || strings.IndexRune(s_Value, '\r') > -1 {
				conn.Write([]byte("Bye Bye monte\n"))
				conn.Close()
				return
			}
			cleanBufIndex, err := checkLeadingZeros(incomingBuf)
			if err != nil {
				conn.Write([]byte(err.Error()))
				panic(err)
				return
			}
			// Send a response back to person contacting us.
			updated_incomingBuf := incomingBuf[cleanBufIndex:]
			gCachInputMap.Write(updated_incomingBuf)
			conn.Write(updated_incomingBuf)
		} else {
			conn.Write([]byte("Failed as input is empty"))
			conn.Close()
		}
	}
}

func checkLeadingZeros(inputByteArray []byte) (indexZeroCounter int, err error) {
	valueLen := len(inputByteArray)
	for i := 0; i < valueLen; i++ {
		incrementSliceValueBuf := i + 1
		if incrementSliceValueBuf > valueLen {
			incrementSliceValueBuf = incrementSliceValueBuf - 1
		}
		leadingZeroCheck := string(inputByteArray[i:incrementSliceValueBuf])
		leadingZeroCheckInt, err := strconv.Atoi(leadingZeroCheck)
		if err != nil {
			return 0, fmt.Errorf("Value is not an int %s\n", leadingZeroCheck)
			break
		}
		if leadingZeroCheckInt == 0 {
			indexZeroCounter = indexZeroCounter + 1
		} else if leadingZeroCheckInt > 0 {
			//fmt.Printf("%d leading zeros\n", indexZeroCounter)
			break
		}
		//fmt.Printf("check: %s\nisZero: %d\n", leadingZeroCheck, indexZeroCounter)
	}
	//fmt.Printf("final value: %s\n", string(inputByteArray[indexZeroCounter:]))
	return indexZeroCounter, nil
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

func RetrieveContents(name string) ([]byte, error) {
	fmt.Printf("retrieve file data %s \n", name)
	f, err := os.OpenFile(name, os.O_RDONLY, 0)
	defer f.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v, Can't open %s: error %\n", os.Args[0], name, err)
		os.Exit(1)
		return nil, err
	}
	contents, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return contents, nil
}

func createFile() (*os.File,error){
	f, err := os.Create("data.0.log")//os.OpenFile("data.0.log", os.O_CREATE, 0755)
	if err != nil {
		log.Fatal(err)
	}
	return f,err
}

func cleanLogs() (){
	files, err := readDirNames(APP_BASE_PATH)
	if err != nil {
		log.Fatal(err)
		return
	}
	for _,value := range files {
	    if strings.Contains(value,"data.") {
			// remove file
			if err = os.Remove(value); err != nil {
				fmt.Println(err)
			}
		}
	}
	// remove any data logs in this working directory
}