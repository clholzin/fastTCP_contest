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
	"os/signal"
	"sync"
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
	count    int
	Duration int
	File     *os.File
	incomingBufData []byte
	counter  struct {
		sync.Mutex
		total      int
		totalSince int
		totalUniq  int
		duplicates int
		fileCount  int
		currentMap map[string]int
	}
)

var (
	connCounter   count
	//inputRecievedCounter int
	keptValuesBuf bytes.Buffer
	//gCachInputMap bytes.Buffer
	err           error
)



func (c *counter) dumpCounts () {

	c.total = 0
	c.totalSince = 0
	c.totalUniq = 0
	//c.duplicates = 0
	c.currentMap = make(map[string]int)
}

func init(){
	cleanLogs()
}

func main() {
	// Listen for incoming connections.
	l, err := net.Listen(CONN_TYPE, CONN_HOST+CONN_PORT)
	mainCounter := new(counter)
	mainCounter.currentMap = make(map[string]int)
	c := make(chan os.Signal, 1)

	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	mainCounter.interval5()
	//mainCounter.interval10()
	// Close the listener when the application closes.
	defer l.Close()
	fmt.Println("Listening on " + CONN_HOST+CONN_PORT)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		go func(){


			if connCounter == 6 {
				conn.Close()
				return
			}
			connCounter++

			f,err := createLog(mainCounter.fileCount)
			if err != nil {
				fmt.Println(fmt.Sprintf("Error failed opening data.%d.log:\n %s \n",mainCounter.fileCount, err.Error()))
				conn.Close()
				return
			}

			lineBreakByteBuf   := incomingBufData("\n")
			var drainBuf       =  make(incomingBufData,10)
			var keptValuesBuf  =  bytes.NewBuffer(drainBuf)
			var data 		   =  make(incomingBufData,10)
			var incomingBuf    =  bytes.NewBuffer(data)

				for {
					if _, err = conn.Read(data[:]); err != nil {
						break
					}
					for {
						sValue,err := incomingBuf.ReadString(lineBreakByteBuf[0])
						if err != nil {
							break
						}else if len(sValue) > 0 {
							mainCounter.total++
							if  len(sValue) == 0 || sValue == "\n" || sValue == "\r" {
								return
							}
							sValue = strings.Replace(sValue,"\n","",1)

							if strings.ToLower(sValue) == APP_SHUTDOWN {
								signal.Notify(c, os.Interrupt)
								break
							}

							checkVal := checkLeadingZeros(sValue)
							if len(checkVal) < 6 {
								return
							}
							mainCounter.Lock()
							if mainCounter.currentMap[checkVal] == 0 {
								keptValuesBuf.Write(append(incomingBufData(checkVal),lineBreakByteBuf[0]))
								mainCounter.currentMap[checkVal] = 1
							}else{
								mainCounter.currentMap[checkVal]++
								mainCounter.duplicates++
							}
							mainCounter.Unlock()
						}
					}
				}
				if keptValuesBuf.Len() > 0 {
					if _,err = f.Write(keptValuesBuf.Bytes()); err != nil{
						fmt.Println(err.Error())
					}
				}

			if connCounter > 0 {
				connCounter--
				conn.Close()
			}
			select {
			case <-c:
				os.Exit(1)
			}
		}()

	}
}


func (c *counter) interval5() {
	//do read out for uniq numbers in this interval of 10 seconds
	// do read out for uniq numbers total for server duration
	 go func(){
	    c.Lock()
		//if gCachInputMap.Len() > 0 {
		c.countAllLogs()
	    fmt.Printf("Total unique numbers this session: %d" +
	    	"\nTotal unique numbers: %d" +
			"\nTotal duplicates received: %d" +
	    	"\nTotal Recieved %d\n\n",
	    		c.totalSince,c.totalUniq,c.duplicates,c.total)
		 c.dumpCounts()
	    c.Unlock()
		time.Sleep(5 * time.Second)
		// do the work
		 c.interval5()
	 }()
}

func (c *counter) interval10() {
	//Every 10 seconds, the log should rotate and increment the number in the name,
	//all while only writing unique numbers. Example: data.0.log -> data.1.log -> data.2.log.
	 go func(){
		c.fileCount++//count and create new file
		_,err = createLog(c.fileCount)
		if err != nil || c.fileCount > 10 {
			return
		}
		time.Sleep(10 * time.Second)
		c.interval10()
	 }()

}

func checkLeadingZeros(s string) (t string) {
	valueLen := len(s)
	var zeroByte = []byte("0")
	for i := 0; i<valueLen;i++ {
		if s[i] == zeroByte[0] {
			if i+1 < len(s) {
				s = s[i+1:]
			} else {
				s = s[i:]
				break
			}
		}else{
			break
		}
	}
	t = s
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

func createLog(fileCount int) (f *os.File,err error){
	// create initial data log
	fileName  := fmt.Sprintf("data.%d.log",fileCount)
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Failed to open file: ",err.Error())
		return nil,err
	}
	return f,err
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

func (c *counter)countAllLogs() {
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
			data,err := ioutil.ReadAll(logFile)
			if err != nil {
				fmt.Println(err)
				return
			}
			if strings.Contains(value,string(c.fileCount)) {
				c.totalSince =  bytes.Count(data, []byte("\n"))
			}else{
				c.totalUniq += bytes.Count(data, []byte("\n"))
			}

			logFile.Close()
		}
	}
	//c.totalSince = bytes.Count(gCachInputMap.Bytes(),[]byte("\n"))
	c.totalUniq = c.totalSince + c.totalUniq
}