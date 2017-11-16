package main

import (
	"bytes"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"io/ioutil"
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
	counter  struct {
		total      count
		totalSince count
	}
	logSt struct {
		fileCount count
	}
)

var (
	connCounter   count
	durationVal   time.Duration
	incomingBuf   = make([]byte, 1024)
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
	durationVal = 5 * time.Second
	go func(){
		time.Sleep(durationVal)
		if gCachInputMap.Len() > 0 {
			fmt.Printf("\r Total unique numbers this session: %d | Total unique numbers received: %d",bytes.Count(gCachInputMap.Bytes(), []byte("\n")),fileData.countAllLogs())
		}else{
			fmt.Printf("\r Total unique numbers this session: %d | Total unique numbers received: %d",0,fileData.countAllLogs())
		}

		// do the work
		interval5()
	}()
}

func interval10() {
	//Every 10 seconds, the log should rotate and increment the number in the name,
	//all while only writing unique numbers. Example: data.0.log -> data.1.log -> data.2.log.
	durationVal = 10 * time.Second
	go func(){
		time.Sleep(durationVal)
		fileData.fileCount++//count and create new file
		gCachInputMap.Reset()// drop cache for this session
		fileData.createLog()
		if fileData.fileCount > 10 {
			return
		}
		interval10()
	}()

}

// Handles incoming requests.
func handleRequest(conn net.Conn) {
	// Close the connection when you're done with it.
	defer conn.Close()
	defer func() {
		recover()
	}()

	if connCounter == 6 {
		conn.Write([]byte("Error: To many connections"))
		return
	}
	connCounter++
	// Read the incoming connection into the buffer.
	buflen, err := conn.Read(incomingBuf)//read connection data to incomingBuf
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Error reading:\n %s \n", err.Error())))
	} else {
		if buflen > 0 {
			s_Value := string(incomingBuf[:buflen])
			if strings.ToLower(s_Value) == APP_SHUTDOWN {
				conn.Write([]byte("Bye Bye monte\n"))
				return
			} else if strings.IndexRune(s_Value, '\n') > -1 || strings.IndexRune(s_Value, '\r') > -1 {
				conn.Write([]byte("Bye Bye monte\n"))
				return
			}
			cleanBufIndex, err := checkLeadingZeros(incomingBuf)
			if err != nil {
				conn.Write([]byte(err.Error()))
				panic(err)
				return
			}
			/** Send a response back to person contacting us.**/
			updated_incomingBuf := incomingBuf[cleanBufIndex:]// add new line

			/** search cache for previous value, kill conn if found */
			if bytes.Contains(gCachInputMap.Bytes(), updated_incomingBuf) {
				conn.Write([]byte(fmt.Sprintf("Already received this value %s\n",string(updated_incomingBuf))))
				return
			}

			updated_incomingBuf = append(updated_incomingBuf,'\n')
			/** Todo
			   Collect previous files numbers and compare all numbers always
			   will need another buffer
			 */
			/** cache all results for quick search while server is alive **/
			gCachInputMap.Write(updated_incomingBuf)

			/** say to the user we got it **/
			conn.Write([]byte("got it\n"))

			/** open file and save **/
			var fileName = fmt.Sprintf("data.%d.log",fileData.fileCount)
			f,err := os.Create(fmt.Sprintf(fileName))// this will either create or open RDWR
			if err != nil {
				conn.Write([]byte(fmt.Sprintf("Error failed opening file %s:\n %s \n",fileName, err.Error())))
				panic(err)
				return
			}
			f.Write(updated_incomingBuf)
		} else {
			conn.Write([]byte("Failed as input is empty\n"))

		}
	}
	if connCounter > 0 {
		connCounter--
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
			n += bytes.Count(fileGuts, []byte("\n"))//count\
			logFile.Close()
		}
	}
	return n
}