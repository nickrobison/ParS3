package main

import (
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"fmt"
	"os"
//	"os/user"
	"strings"
	"runtime"
	"io/ioutil"
//	"code.google.com/p/gcfg"
//	"github.com/zyxar/hdfs"
	"encoding/json"
	"flag"
	"time"
)

type continueOn  struct {
	nextMarker string
	truncated bool
	finished bool
}







type AWSCredentialsType struct {
	AccessKey string
	SecretKey string
}

//var HDFSServer string = "localhost"
//var HDFSPort uint16 = 8020


func getandsave(b s3.Bucket, prefix string, left, right chan continueOn, done chan<- struct{}, threadNum int, directory string, maxMarker int) {
	grab := <-right

	if grab.finished == true{
		left <- grab
		done <- struct{}{}
		return
	}
//	if grab.truncated == false {
//		left <- continueOn{truncated: false, nextMarker: ""}
//		done <- struct{}{}
//		return
//	}
	resp, err := b.List(prefix, "", grab.nextMarker, maxMarker)
	if err != nil {
		fmt.Println(err)
		for e := 0; e <=5 && err != nil; e++ {
			time.Sleep(3 * time.Second)
			resp, err = b.List(prefix, "", grab.nextMarker, maxMarker)
			if e == 5{
				panic("Directory List Failed")
			}
		}
	}


	onward := continueOn{resp.Contents[len(resp.Contents) - 1].Key, resp.IsTruncated, false}

	switch onward.truncated {
	case true: left <- onward
	case false: left <- continueOn{nextMarker: "", truncated: false, finished: true}
	}

//	if len(resp.Contents) == 1 {
//		fmt.Println("Not Downloading")
//		done <- struct{}{}
//		return
//	
	startIndex := 0
	if grab.nextMarker == "" {
		startIndex = 1
	}

	writepath := ""
	fmt.Println("Running here")
	//Connect to HDFS
//	fs, err := hdfs.ConnectAsUser(HDFSServer, HDFSPort, "nick")
	
	for j := startIndex; j <= len(resp.Contents) - 1; j++ {

		FileGet := resp.Contents[j].Key

		f, err := b.Get(FileGet)
		if err != nil {
			fmt.Println(err)
			for e := 0; e <=5 && err != nil; e++ {
				time.Sleep(3 * time.Second)
				f, err = b.Get(FileGet)
				if e == 5{
					panic("File Download Failed")
				}
		}
	}

		Splits := strings.SplitAfter(FileGet, "/")

		writepath = directory + Splits[len(Splits) - 1]
		//file, err := os.Create(Splits[len(Splits) - 1])
		file, err := os.Create(writepath)
		if err != nil {
			panic(err)
		}
		fmt.Println("Writing File: ", Splits[len(Splits) - 1])
		
		// Hadoop Write
		//file, err := fs.OpenFile(writepath, 01|0100, 0, 0, 0)
		//if err != nil {
		//	panic(err)
		//}
		file.Write(f)
		file.Close()
		
		
}
	done <- struct{}{}
	//fs.Disconnect()


}

func awaitCompletion(done <-chan struct{}) {
	for f := 0; f < workers; f++ {
		<-done
	}
//	close(leftmost)
}

var workers = runtime.NumCPU() * 16




func main() {

	// Define flags
	srcbucket := flag.String("bucket", "bmi-weather-test", "Bucket from which to retrieve files")
	prefix := flag.String("prefix", "TestCSV/", "Prefix from which to retrieve files")
	directory := flag.String("dir", "/Users/nrobison/Developer/git/ParS3/", "Directory to store files")
	maxMarker := flag.Int("max", 1000, "Max Markers to Retrieve per Worker")
	flag.Parse()

	// Setup User Environment
//	usr, err := user.()
//	if err != nil {
//		panic(err)
//	}
	//HomeDir := usr.HomeDir + "/.ParS3"
//	fmt.Println(HomeDir)

	// Read in Config File
//	file, err := ioutil.ReadFile(usr.HomeDir + "/.ParS3")
	file, err := ioutil.ReadFile(".ParS3")
	if err != nil {
		panic(err)
	}

	var cfgFile AWSCredentialsType
	json.Unmarshal(file, &cfgFile)

//	hadoopfs, err := hdfs.Connect("localhost", 8020)
//	if err != nil {
//		panic(err)
//	}

	//auth, err := aws.EnvAuth()
	//if err != nil {
	//	panic(err)
	//}

	auth := aws.Auth {
		AccessKey: cfgFile.AccessKey,
		SecretKey: cfgFile.SecretKey,
	}

	e := s3.New(auth, aws.USEast)

	b := s3.Bucket {
		S3: e,
		Name: *srcbucket,
	}

	i := s3.Bucket(b)
	//fmt.Println("Number of workers: ", workers)
	nextLoop := continueOn{truncated: true, nextMarker: "", finished: false}
	done := make(chan struct{}, workers)
	leftmost := make(chan continueOn)
	right := leftmost
	left := leftmost

	for nextLoop.truncated == true && nextLoop.finished == false {
		leftmost = make(chan continueOn)
		right = leftmost
		left = leftmost
	for z := 0; z < workers; z++ {
//		nextLoop = getandsave(i, "Singles/", passit.nextMarker, passit)
		//nextLoop = <-passit
//		fmt.Println("mainLoop")
		right = make(chan continueOn)
		go getandsave(i, *prefix, left, right, done, z, *directory, *maxMarker)
		//nextLoop = <-right
		left = right
//		fmt.Println("Running next on: ", nextLoop.nextMarker)
		//fmt.Println(nextLoop.markersReturned)
//		left = make(chan continueOn)
	}
	go func(c chan continueOn) { c <-nextLoop}(right)
	nextLoop = <-leftmost
	awaitCompletion(done)
}



}
