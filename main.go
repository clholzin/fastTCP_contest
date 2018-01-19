package main

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strings"
	. "github.com/clholzin/fastTCP_contest/utilCounter"
)

const (
	AppShutdown         = "shutdown"
	ConnHost            = "127.0.0.1:"
	ConnPort            = "3280"
	ConnType            = "tcp"
	APP_INITIAL_FILENAME = "data.*.log"
)

type (
	Duration        int
	incomingBufData []byte
)

var (
	connCounter    Count
	backendChannel = make(chan *BundleData, 6)
	countChan      = make(chan Count, 1)
)

func init() {
	runtime.GOMAXPROCS(2)
	CleanLogs()
}

func main() {
	// Listen for incoming connections.

	//------------------------TCP Listener----------------------
	l, err := net.Listen(ConnType, ConnHost+ConnPort)
	if err != nil {
		fmt.Println("Error listening:", err.Error())
		os.Exit(1)
	}
	fmt.Println("Listening on " + ConnHost + ConnPort)
	//----------------------------------------------------------

	mainCounter := new(Counter)
	mainCounter.CurrentMap = make(map[string]int)
	killChan := make(chan os.Signal, 1)
	triggerInterval5Chan := make(TriggerChan, 6)
	go mainCounter.Interval10()
	go mainCounter.Interval5(&connCounter,triggerInterval5Chan)

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

	go mainCounter.Consume(backendChannel)

	select {
	case <-killChan:
		os.Exit(1)
	}
}

func pumpData(connection net.Conn,c chan os.Signal,backendChannel chan *BundleData,
	countChan chan Count,triggerInterval5Chan TriggerChan) {

	defer func() {
		//connection.Close()
		countChan <- -1 // sync with chan
	}()

	sValue := ""
	currentCounter := new(Counter)
	currentCounter.CurrentMap = make(map[string]int)
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
		currentCounter.Mu.Lock()
		currentCounter.CurrentMap = make(map[string]int)
		currentCounter.Mu.Unlock()

		sValueArray := strings.Split(readString, "\n")
		for i := 0; i < len(sValueArray); i++ {
			sValue = sValueArray[i]
			if len(sValue) == 0 {
				continue
			}
			currentCounter.Total++
			currentCounter.TotalSince++
			switch {
			case strings.ToLower(sValue) == AppShutdown:
				signal.Notify(c, os.Interrupt)// receive shutdown string and kill app
				break
			default:
				//checkVal := checkLeadingZeros(sValue)
				if _,ok := currentCounter.CurrentMap[sValue]; !ok {
					currentCounter.TotalUniq++
					keptValuesBuf.Write(incomingBufData(sValue+"\n"))//append(incomingBufData(sValue), lineBreakByteBuf[0])
				}else{
					currentCounter.Duplicates++
				}
				currentCounter.CurrentMap[sValue]++
			}
			select {
			case <-killLoopChan:
				break
			case <-triggerInterval5Chan:
				u := keptValuesBuf.Bytes()
				/* clone map */
				nmap := make(map[string]int)
				for g,m := range currentCounter.CurrentMap {
					nmap[g] = m
				}
				newCountCurrent := new(Counter)
				newCountCurrent.Total = currentCounter.Total
				newCountCurrent.Duplicates = currentCounter.Duplicates
				newCountCurrent.TotalUniq = currentCounter.TotalUniq
				newCountCurrent.TotalSince = currentCounter.TotalSince
				newCountCurrent.CurrentMap = nmap
				b := &BundleData{newCountCurrent, append([]byte(""),u...)}
				backendChannel <- b
				keptValuesBuf.Reset()
			default:

			}
		}

		 bufConn.Reset()
		 bufConn.Grow(1024)
	}


}


