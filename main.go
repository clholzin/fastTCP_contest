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
	triggerChan chan int
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
	triggerInterval5Chan := make(triggerChan, 6)
	go mainCounter.interval10()
	go mainCounter.interval5(&connCounter,triggerInterval5Chan)

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

			go pumpData(conn, killChan, backendChannel, countChan,triggerInterval5Chan)

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

func pumpData(connection net.Conn,c chan os.Signal,backendChannel chan *bundleData,
	countChan chan count,triggerInterval5Chan triggerChan) {

	defer func() {
		//connection.Close()
		countChan <- -1 // sync with chan
	}()

	sValue := ""
	currentCounter := new(counter)
	currentCounter.currentMap = make(map[string]int)
	//lineBreakByteBuf := incomingBufData("\n")
	killLoopChan := make(chan int, 1)
	drainBuf := make(incomingBufData, 1024)
	keptValuesBuf := bytes.NewBuffer(drainBuf)
	bu := make([]byte, 1024)
	for {
		_, err := connection.Read(bu)
		if err != nil {
			fmt.Println(err.Error())
			break
		}
		bufConn := bytes.NewBuffer(bu)
		readString := bufConn.String()
		if len(readString) == 0 {
			break
		}
		currentCounter.mu.Lock()
		currentCounter.currentMap = make(map[string]int)
		currentCounter.mu.Unlock()

		sValueArray := strings.Split(readString, "\n")
		for i := 0; i < len(sValueArray); i++ {
			sValue = sValueArray[i]
			if len(sValue) == 0 {
				continue
			}
			currentCounter.total++
			currentCounter.totalSince++
			switch {
			case strings.ToLower(sValue) == APP_SHUTDOWN:
				signal.Notify(c, os.Interrupt)// receive shutdown string and kill app
				break
			default:
				//checkVal := checkLeadingZeros(sValue)
				if _,ok := currentCounter.currentMap[sValue]; !ok {
					currentCounter.totalUniq++
					keptValuesBuf.Write(incomingBufData(sValue+"\n"))//append(incomingBufData(sValue), lineBreakByteBuf[0])
				}else{
					currentCounter.duplicates++
				}
				currentCounter.currentMap[sValue]++
			}
			select {
			case <-killLoopChan:
				break
			case <-triggerInterval5Chan:
				u := keptValuesBuf.Bytes()
				/* clone map */
				nmap := make(map[string]int)
				for g,m := range currentCounter.currentMap {
					nmap[g] = m
				}
				newCountCurrent := new(counter)
				newCountCurrent.total = currentCounter.total
				newCountCurrent.duplicates = currentCounter.duplicates
				newCountCurrent.totalUniq = currentCounter.totalUniq
				newCountCurrent.totalSince = currentCounter.totalSince
				newCountCurrent.currentMap = nmap
				b := &bundleData{newCountCurrent, append([]byte(""),u...)}
				backendChannel <- b
				keptValuesBuf.Reset()
			default:

			}
		}

		 bufConn.Reset()
		 bufConn.Grow(1024)
	}


}

func (c *counter) consume(backendChannel chan *bundleData) {

	for {
		select {
		case d,ok := <-backendChannel:
			if ok {
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
}

func (c *counter) dumpCounts() {

	c.mu.Lock()
	c.total = 0
	c.totalSince = 0
	c.totalUniq = 0
	c.duplicates = 0
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
		c.totalSince += currentCounter.totalSince
		for k, n := range currentCounter.currentMap {
			if _,ok := c.currentMap[k];!ok {
				c.totalUniq++
			}else{
				c.duplicates++
			}
			c.currentMap[k] += n
		}
	c.mu.Unlock()
}

func (c *counter) interval5(connCount *count,triggerInterval5Chan triggerChan) {
	//do read out for uniq numbers in this interval of 5 seconds
	// do read out for uniq numbers total for server duration
	for {
		select{
		case <- time.Tick(5 * time.Second):
			go c.countAllLogs()
			var i count = 0
			valueCont := *connCount
			for ;i < valueCont;i++{
					triggerInterval5Chan <- 1
			}
			fmt.Printf("Total unique numbers this session: %d"+
				"\nTotal unique numbers: %d"+
				"\nTotal duplicates received: %d"+
				"\nTotal Recieved %d\n\n",
				c.totalSince, c.totalUniq, c.duplicates, c.total)
			 c.dumpCounts()
		}
	}
}

func (c *counter) interval10() {
	//Every 10 seconds, the log should rotate and increment the number in the name,
	//all while only writing unique numbers. Example: data.0.log -> data.1.log -> data.2.log.
	for {
		select{
		case <- time.Tick(10 * time.Second):
			c.mu.Lock()
			c.fileCount++ //count and create new file
			_, err := createLog(c.fileCount)
			if err != nil || c.fileCount > 10 {
				return
			}
			c.mu.Unlock()
		}
	}
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
			totalInFile := bytes.Count(data, []byte("\n"))
			c.total += totalInFile
			if strings.Contains(value, string(c.fileCount)) {
				c.totalSince += totalInFile
			} else {
				c.totalUniq += totalInFile
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
