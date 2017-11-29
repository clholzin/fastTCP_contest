package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"time"
	"io/ioutil"
	"sync"
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
	count    int
	Duration int
	File     *os.File
	incomingBufData []byte
	/*counter  struct {
		total      count
		totalSince count
	}*/
	logSt struct {
		fileCount count
	}
)

var (
	connCounter   count
	inputRecievedCounter   int
	//durationVal   time.Duration
	keptValuesBuf bytes.Buffer
	gCachInputMap bytes.Buffer
	fileData      = new(logSt)
	err           error
)

func init() {

	fileData.cleanLogs()
	err = fileData.createLog()
	if err != nil {
		fmt.Println(err)
	}
}

func main() {
	startTCP()
}

func startTCP() {
	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+":"+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	interval5()
	interval10()
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

func interval5() {
	//do read out for uniq numbers in this interval of 10 seconds
	// do read out for uniq numbers total for server duration
	 go func(){
		//if gCachInputMap.Len() > 0 {
			fmt.Printf("Total unique numbers this session: %d | Total unique numbers received: %d | Total Recieved %d\n",bytes.Count(gCachInputMap.Bytes(), []byte("\n")),fileData.countAllLogs(),inputRecievedCounter)
		//}else{
		//	fmt.Printf("Total unique numbers this session: %d | Total unique numbers received: %d | Total Recieved %d\n",0,fileData.countAllLogs(),inputRecievedCounter)
		//}
		time.Sleep(5 * time.Second)
		// do the work
		interval5()
	 }()
}

func interval10() {
	//Every 10 seconds, the log should rotate and increment the number in the name,
	//all while only writing unique numbers. Example: data.0.log -> data.1.log -> data.2.log.
	 go func(){
		fileData.fileCount++//count and create new file
		gCachInputMap.Reset()// drop cache for this session
		fileData.createLog()
		if fileData.fileCount > 10 {
			return
		}
		time.Sleep(10 * time.Second)
		interval10()
	 }()

}

// Handles incoming requests.
func handleRequest(conn net.Conn) {

	incomingBuf := make(incomingBufData, 1024)
	lineBreakByteBuf := incomingBufData("\n")
	var keptValuesBuf  bytes.Buffer
	var wg sync.WaitGroup
	wg.Add(1)
	// Close the connection when you're done with it.
    defer func(){
		conn.Close()
		recover()
	}()

	if connCounter == 6 {
		//conn.Write([]byte("Error: To many connections\n"))
		return
	}
	connCounter++
	// Read the incoming connection into the buffer.
	buflen, err := conn.Read(incomingBuf)//read connection data to incomingBuf
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error reading:\n %s \n", err.Error())))
	} else {
		if buflen > 0 {
			sValue := strings.Split(string(incomingBuf[:]),"\n")
			if len(sValue) > 0 {
				inputRecievedCounter = inputRecievedCounter + len(sValue)// all values received
				for i := 0;i< len(sValue);i++ {
					if strings.ToLower(sValue[i]) == APP_SHUTDOWN || len(sValue[i]) == 0 {
						//conn.Write([]byte("Bye Bye\n"))
						break
					}
					singleIncomingBuf := incomingBufData(sValue[i])
					cleanBufIndex, err := checkLeadingZeros(singleIncomingBuf)
					if err != nil {
						//conn.Write([]byte(err.Error()))
						//panic(err)
						break
					}
					cleanedInputVal := singleIncomingBuf[cleanBufIndex:]
					if !strings.Contains(gCachInputMap.String(), sValue[i]) {
						singleIncomingBuf = append(cleanedInputVal,lineBreakByteBuf[0])
						keptValuesBuf.Write(singleIncomingBuf)
					}

				}
				/** cache all results for quick search while server is alive **/
				if keptValuesBuf.Len() > 0 {
					/** say to the user we got it **/
					//conn.Write([]byte("got it\n"))
					/** open file and save **/
					var fileName = fmt.Sprintf("data.%d.log",fileData.fileCount)
					f,err := os.Create(fileName)// this will either create or open RDWR
					if err != nil {
						conn.Write([]byte(fmt.Sprintf("Error failed opening file %s:\n %s \n",fileName, err.Error())))
						panic(err)
					}else{
						gCachInputMap.Write(keptValuesBuf.Bytes())
						f.Write(keptValuesBuf.Bytes())
					}

				}
			}


		} else {
			conn.Write([]byte("Failed as input is empty\n"))
		}
	}
	if connCounter > 0 {
		connCounter--
	}
	return
}

func checkLeadingZeros(inputByteArray []byte) (indexZeroCounter int, err error) {
	valueLen := len(inputByteArray)
	zeroByte := []byte("0")
	for i := 0; i < valueLen; i++ {
		incrementSliceValueBuf := i + 1
		if incrementSliceValueBuf > valueLen {
			incrementSliceValueBuf = incrementSliceValueBuf - 1
			break
		}
		/*leadingZeroCheck := inputByteArray[i:incrementSliceValueBuf]
			eadingZeroCheckInt, err := strconv.Atoi(leadingZeroCheck)
			if err != nil {
				return 0, fmt.Errorf("Value is not an int %s\n", leadingZeroCheck)
				break
			}*/
		if bytes.Contains(inputByteArray[i:incrementSliceValueBuf],zeroByte[:]){
			indexZeroCounter++
		} else {//if inputByteArray[i:incrementSliceValueBuf] !== zeroByte[:]
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

func (f *logSt)createLog() error {
	// create initial data log
	fileName  := fmt.Sprintf("data.%d.log",f.fileCount)
	_, err = os.Create(fileName)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func (f *logSt)cleanLogs() {
	// remove any data logs in this working directory
	files, err := readDirNames(APP_BASE_PATH)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, value := range files {
		if strings.Contains(value, "data.") {
			// remove file
			if err = os.Remove(value); err != nil {
				fmt.Println(err)
			}
		}
	}

}

func (f *logSt)countAllLogs() (n int) {
	// remove any data logs in this working directory
	files, err := readDirNames(APP_BASE_PATH)
	if err != nil {
		log.Fatal(err)
	}
	for _, value := range files {
		if strings.Contains(value, "data.") {
			logFile,err := os.Open(value)
			if err != nil {
				fmt.Println(err)
				return
			}
			//read
			fileGuts,err := ioutil.ReadAll(logFile)
			if err != nil {
				fmt.Println(err)
				return
			}
			//close
			n = n + bytes.Count(fileGuts, []byte("\n"))//count\
			logFile.Close()
		}
	}
	return n
}