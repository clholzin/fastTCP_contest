package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	APP_BASE_PATH        = "./"
	APP_INITIAL_FILENAME = "data.*.log"
	APP_SHUTDOWN         = "shutdown"
	CONN_HOST            = "127.0.0.1:"
	CONN_PORT            = "3280"
	CONN_TYPE            = "tcp"
)

type (
	count           int
	Duration        int
	incomingBufData []byte
	counter         struct {
		mu         sync.Mutex
		total      int
		totalSince int
		totalUniq  int
		duplicates int
		fileCount  int
		currentMap map[string]int
	}
	bundleData struct {
		countData *counter
		readData  []byte
	}
)

var (
	connCounter    count
	backendChannel = make(chan *bundleData, 6)
	countChan      = make(chan count, 1)
)

func init() {
	runtime.GOMAXPROCS(2)
	cleanLogs()
}

func main() {
	// Listen for incoming connections.

	//------------------------TCP Listener----------------------
	l, err := net.Listen(CONN_TYPE, CONN_HOST+CONN_PORT)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	fmt.Println("Listening on " + CONN_HOST + CONN_PORT)
	//----------------------------------------------------------

	mainCounter := new(counter)
	mainCounter.currentMap = make(map[string]int)
	killChan := make(chan os.Signal, 1)

	mainCounter.interval10()
	mainCounter.interval5()

	// Close the listener when the application closes.
	defer l.Close()

	go func() {
		for {

			if connCounter >= 6 {
				continue
			}
			// Listen for an incoming connection.
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting: ", err.Error())
				os.Exit(1)
			}

			countChan <- 1

			go pump(conn, killChan, backendChannel, countChan)

		}
	}()

	go func() {
		for {
			select {
			case valInt := <-countChan:
				connCounter += valInt
			}
		}
	}()

	go mainCounter.consume(backendChannel)

	select {
	case <-killChan:
		os.Exit(1)
	}
}

func pump(connection net.Conn, c chan os.Signal, backendChannel chan *bundleData, countChan chan count) {
	defer func() {
		connection.Close()
		countChan <- -1 // sync with chan
	}()

	sValue := ""
	currentCounter := new(counter)
	currentCounter.currentMap = make(map[string]int)
	lineBreakByteBuf := incomingBufData("\n")
	killLoopChan := make(chan int, 1)
	drainBuf := make(incomingBufData, 1024)
	keptValuesBuf := bytes.NewBuffer(drainBuf)
	bu := make([]byte, 1024)
	bufConn := bytes.NewBuffer(bu)

	for {
		_, err := connection.Read(bu) //bufio.NewReader(connection)
		//bytesLeft := bufReader.Buffered()
		//readString := string(bu[:])
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		readString := string(bu[:])
		sValueArray := strings.Split(readString, "\n")
		for i := 0; i < len(sValueArray); i++ {
			sValue = sValueArray[i]
			if len(sValue) == 0 {
				continue
			}
			/*sValue,err = bufConn.ReadString(lineBreakByteBuf[0])
			if err != nil || sValue == ""{
				if sValue == "ooopps"{
					fmt.Println(sValue)
					return
				}
				sValue = "ooopps"
				break
			}*/
			//if bytesLeft > 0 {
			//bufReader.ReadBytes(lineBreakByteBuf[0])
			/*if err != nil || len(sValue) == 0{//|| byteValue[0] == lineBreakByteBuf[0]
				if err != io.EOF {
					fmt.Println(err.Error())
					return
				}
				killLoopChan <- 1
			}*/
			/*if len(sValue) < 5 {
				continue
			}*/
			//sValue := string(byteValue[:])
			currentCounter.total++
			switch {
			case strings.ToLower(sValue) == APP_SHUTDOWN:
				signal.Notify(c, os.Interrupt)
				return
			default: //len(sValue) > 0 && sValue != "\n" && sValue != "\r"
				//sValue = strings.Replace(sValue, "\n", "", 1)
				checkVal := sValue //checkLeadingZeros(sValue)
				/*if len(checkVal) > 5 {
					if currentCounter.currentMap[checkVal] == 0 {
						keptValuesBuf.Write(append(incomingBufData(checkVal), lineBreakByteBuf[0]))
						currentCounter.currentMap[checkVal] = 1
						currentCounter.totalUniq++
					} else {
						currentCounter.currentMap[checkVal]++
						currentCounter.duplicates++
					}
				}*/
				keptValuesBuf.Write(append(incomingBufData(checkVal), lineBreakByteBuf[0]))
				if currentCounter.currentMap[checkVal] == 0 {
					currentCounter.totalUniq++
				}
				currentCounter.currentMap[checkVal]++
			}

			//}
			select {
			case <-killLoopChan:
				break
			default:
			}
		}
		bufConn.Reset()
	}
	b := &bundleData{currentCounter, keptValuesBuf.Bytes()}
	backendChannel <- b

	/*select {
	case <-c:
		os.Exit(1)
	default:
	}*/

}

func (c *counter) consume(backendChannel chan *bundleData) {

	for {
		select {
		case d := <-backendChannel:
			fileLog, err := createLog(c.fileCount)
			if err != nil {
				fmt.Println(err)
			}

			if _, err = fileLog.Write(d.readData); err != nil {
				fmt.Println(err.Error())
			}
			fileLog.Close()
			c.addUp(d.countData)
		}
	}
}

func (c *counter) dumpCounts() {

	c.mu.Lock()
	c.total = 0
	c.totalSince = 0
	c.totalUniq = 0
	//c.duplicates = 0
	/*file := c.memoizeFile()
	file.Close()
	fileLog, err := createLog(c.fileCount)
	if err != nil {
		fmt.Println(err)
		return
	}
	c.memoizeFile = func() (*os.File) {
		return fileLog
	}*/
	c.currentMap = make(map[string]int)
	c.mu.Unlock()
}

func (c *counter) addUp(currentCounter *counter) {
	c.mu.Lock()
	c.total += currentCounter.total
	c.duplicates += currentCounter.duplicates
	for k, _ := range currentCounter.currentMap {
		if c.currentMap[k] == 0 {
			c.currentMap[k] = currentCounter.currentMap[k]
			c.totalUniq++
		} else {
			c.currentMap[k] += currentCounter.currentMap[k]
			c.duplicates++
		}
	}
	c.mu.Unlock()
}

func (c *counter) interval5() {
	//do read out for uniq numbers in this interval of 10 seconds
	// do read out for uniq numbers total for server duration
	go func() {
		c.countAllLogs()
		fmt.Printf("Total unique numbers this session: %d"+
			"\nTotal unique numbers: %d"+
			"\nTotal duplicates received: %d"+
			"\nTotal Recieved %d\n\n",
			c.totalSince, c.totalUniq, c.duplicates, c.total)
		c.dumpCounts()
		time.Sleep(5 * time.Second)
		c.interval5()
	}()
}

func (c *counter) interval10() {
	//Every 10 seconds, the log should rotate and increment the number in the name,
	//all while only writing unique numbers. Example: data.0.log -> data.1.log -> data.2.log.
	go func() {
		c.mu.Lock()
		c.fileCount++ //count and create new file
		/*file := c.memoizeFile()
		file.Close()
		fileLog, err := createLog(c.fileCount)
		if err != nil || c.fileCount > 10 {
			return
		}
		c.memoizeFile = func() (*os.File) {
			return fileLog
		}*/
		_, err := createLog(c.fileCount)
		if err != nil || c.fileCount > 10 {
			return
		}
		c.mu.Unlock()

		time.Sleep(10 * time.Second)
		c.interval10()
	}()

}

func (c *counter) countAllLogs() {
	files, err := readDirNames(APP_BASE_PATH)
	if err != nil {
		log.Fatal(err)
	}
	c.mu.Lock()
	for _, value := range files {
		if strings.Contains(value, "data.") {
			logFile, err := os.Open(value)
			if err != nil {
				fmt.Println(err)
				continue
			}
			data, err := ioutil.ReadAll(logFile)
			if err != nil {
				fmt.Println(err)
				continue
			}
			if strings.Contains(value, string(c.fileCount)) {
				c.totalSince += bytes.Count(data, []byte("\n"))
			} else {
				c.totalUniq += bytes.Count(data, []byte("\n"))
			}

			logFile.Close()
		}
	}
	//c.totalSince = bytes.Count(gCachInputMap.Bytes(),[]byte("\n"))
	c.totalUniq = c.totalSince + c.totalUniq
	c.mu.Unlock()
}

func checkLeadingZeros(s string) (t string) {
	valueLen := len(s)
	var zeroByte = []byte("0")
	i := 0
	for ; i < valueLen; i++ {
		if s[i] == zeroByte[0] {
			if i+1 >= len(s) {
				break
			}
		} else {
			break
		}
	}
	t = s[i:]
	return t
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

func createLog(fileCount int) (f *os.File, err error) {
	// create initial data log
	fileName := fmt.Sprintf("data.%d.log", fileCount)
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Failed to open file: ", err.Error())
		return nil, err
	}
	return f, err
}

func cleanLogs() {
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
