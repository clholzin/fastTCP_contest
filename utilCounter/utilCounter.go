package utilCounter

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	APP_BASE_PATH = "./"
)

type (
	Count   int
	Counter struct {
		Mu         sync.Mutex
		Total      int
		TotalSince int
		TotalUniq  int
		Duplicates int
		FileCount  int
		CurrentMap map[string]int
	}
	BundleData struct {
		CountData *Counter
		ReadData  []byte
	}
	TriggerChan chan int
)

func (c *Counter) Consume(backendChannel chan *BundleData) {

	for {
		select {
		case d, ok := <-backendChannel:
			if ok {
				fileLog, err := CreateLog(c.FileCount)
				if err != nil {
					fmt.Println(err)
				}

				if _, err = fileLog.Write(d.ReadData); err != nil {
					fmt.Println(err.Error())
				}
				fileLog.Close()
				c.AddUp(d.CountData)
			}
		}
	}
}

func (c *Counter) DumpCounts() {
	c.Mu.Lock()
	c.Total = 0
	c.TotalSince = 0
	c.TotalUniq = 0
	c.Duplicates = 0
	c.CurrentMap = make(map[string]int)
	c.Mu.Unlock()
}

func (c *Counter) AddUp(currentCounter *Counter) {
	c.Mu.Lock()
	c.Total += currentCounter.Total
	c.Duplicates += currentCounter.Duplicates
	c.TotalSince += currentCounter.TotalSince
	for k, n := range currentCounter.CurrentMap {
		if _, ok := c.CurrentMap[k]; !ok {
			c.TotalUniq++
		} else {
			c.Duplicates++
		}
		c.CurrentMap[k] += n
	}
	c.Mu.Unlock()
}

func (c *Counter) Interval5(connCount *Count, triggerInterval5Chan TriggerChan) {
	//do read out for uniq numbers in this interval of 5 seconds
	// do read out for uniq numbers total for server duration
	for {
		select {
		case <-time.Tick(5 * time.Second):
			go c.CountAllLogs()
			var i Count = 0
			valueCont := *connCount
			for ; i < valueCont; i++ {
				triggerInterval5Chan <- 1
			}
			fmt.Printf("Total unique numbers this session: %d"+
				"\nTotal unique numbers: %d"+
				"\nTotal duplicates received: %d"+
				"\nTotal Recieved %d\n\n",
				c.TotalSince, c.TotalUniq, c.Duplicates, c.Total)
			c.DumpCounts()
		}
	}
}

func (c *Counter) Interval10() {
	//Every 10 seconds, the log should rotate and increment the number in the name,
	//all while only writing unique numbers. Example: data.0.log -> data.1.log -> data.2.log.
	for {
		select {
		case <-time.Tick(10 * time.Second):
			c.Mu.Lock()
			c.FileCount++ //count and create new file
			_, err := CreateLog(c.FileCount)
			if err != nil || c.FileCount > 10 {
				return
			}
			c.Mu.Unlock()
		}
	}
}

func (c *Counter) CountAllLogs() {
	files, err := ReadDirNames(APP_BASE_PATH)
	if err != nil {
		log.Fatal(err)
	}
	c.Mu.Lock()
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
			c.Total += totalInFile
			if strings.Contains(value, string(c.FileCount)) {
				c.TotalSince += totalInFile
			} else {
				c.TotalUniq += totalInFile
			}

			logFile.Close()
		}
	}
	//c.totalSince = bytes.Count(gCachInputMap.Bytes(),[]byte("\n"))
	c.TotalUniq = c.TotalSince + c.TotalUniq
	c.Mu.Unlock()
}

func CheckLeadingZeros(s string) (t string) {
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

func ReadDirNames(dirname string) ([]string, error) {
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

func CreateLog(fileCount int) (f *os.File, err error) {
	// create initial data log
	fileName := fmt.Sprintf("data.%d.log", fileCount)
	f, err = os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("Failed to open file: ", err.Error())
		return nil, err
	}
	return f, err
}

func CleanLogs() {
	// remove any data logs in this working directory
	files, err := ReadDirNames(APP_BASE_PATH)
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
