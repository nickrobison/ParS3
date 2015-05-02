// HDFS Config Details: https://github.com/zyxar/hdfs

package main

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	//"github.com/zyxar/hdfs"
	"io/ioutil"
	"launchpad.net/goamz/aws"
	"launchpad.net/goamz/s3"
	"os"
	"os/user"
	"path"
	"runtime"
	"strings"
	"time"
)

type continueOn struct {
	nextMarker string
	truncated  bool
	finished   bool
}

type ConfigSettingsType struct {
	AWSCredentials AWSCredentialsType
	System         SystemType
}

type AWSCredentialsType struct {
	AccessKey string
	SecretKey string
}

type SystemType struct {
	javaHome     string
	ldPath       string
	hadoopServer string
	hadoopPort   uint16
}

type fileInfo struct {
	FileName      string
	WriteLocation string
	MD5Sum        string
	File          []byte
	Size          int
}

type Job struct {
	filename string
	results  chan<- Result
}

type Result struct {
	filename string
}

type s3Directory struct {
	bucket string
	prefix string
	files  []s3File
}

type s3File struct {
	Name         string
	Size         int64
	Owner        string
	StorageClass string
}

// Global Constants

var workers int = runtime.NumCPU()

const hadoopServer string = "default"
const hadoopPort int = 0

func getandsave(b s3.Bucket, prefix string, left, right chan continueOn, done chan<- struct{}, threadNum int, directory string, maxMarker int, hadoop bool, hashCheck bool) {
	grab := <-right

	if grab.finished == true {
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
		for e := 0; e <= 5 && err != nil; e++ {
			time.Sleep(3 * time.Second)
			resp, err = b.List(prefix, "", grab.nextMarker, maxMarker)
			if e == 5 {
				panic("Directory List Failed")
			}
		}
	}

	onward := continueOn{resp.Contents[len(resp.Contents)-1].Key, resp.IsTruncated, false}

	switch onward.truncated {
	case true:
		left <- onward
	case false:
		left <- continueOn{nextMarker: "", truncated: false, finished: true}
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
	var finished string = ""
	if hadoop == true {
		fmt.Println("Remove me")
		//finished = hadoopWrite(*resp, b, directory, startIndex)
	} else {
		finished = standardWrite(*resp, b, directory, startIndex, hashCheck)
	}

	//	for j := startIndex; j <= len(resp.Contents)-1; j++ {
	//
	//		fileData := s3Get(j, *resp, b, directory)
	//
	//
	//
	//
	//
	//
	//	}

	fmt.Println(finished)

	done <- struct{}{}

}

func awaitCompletion(done <-chan struct{}) {
	for f := 0; f < workers; f++ {
		<-done
	}
	//	close(leftmost)
}

func s3Get(j int, resp s3.ListResp, b s3.Bucket, directory string) fileInfo {

	FileGet := resp.Contents[j].Key

	f, err := b.Get(FileGet)
	if err != nil {
		fmt.Println(err)
		for e := 0; e <= 5 && err != nil; e++ {
			time.Sleep(3 * time.Second)
			f, err = b.Get(FileGet)
			if e == 5 {
				panic("File Download Failed")
			}
		}
	}

	dir, filename := path.Split(FileGet)

	return fileInfo{
		FileName:      filename,
		WriteLocation: path.Join(directory, dir),
		MD5Sum:        resp.Contents[j].ETag,
		File:          f,
		Size:          len(f),
	}

}

//func hadoopWrite(resp s3.ListResp, b s3.Bucket, directory string, startIndex int) string {
//
//	currentUser, err := user.Current()
//	if err != nil {
//		panic(err)
//	}
//
//	fs, err := hdfs.ConnectAsUser(hadoopServer, 0, currentUser.Name)
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Println("Running files: ", len(resp.Contents)-1)
//
//	for j := startIndex; j <= len(resp.Contents)-1; j++ {
//		//
//		//		FileGet := resp.Contents[j].Key
//		//
//		//		f, err := b.Get(FileGet)
//		//		if err != nil {
//		//			fmt.Println(err)
//		//			for e := 0; e <= 5 && err != nil; e++ {
//		//				time.Sleep(3 * time.Second)
//		//				f, err = b.Get(FileGet)
//		//				if e == 5 {
//		//					panic("File Download Failed")
//		//				}
//		//			}
//		//		}
//
//		fileData := s3Get(j, resp, b, directory)
//
//		fmt.Println("Writing File: ", fileData.FileName)
//		fmt.Println("To Directory: ", fileData.WriteLocation)
//
//		file, err := fs.OpenFile(fileData.WriteLocation, 01|0100, 0, 0, 0)
//		if err != nil {
//			panic(err)
//		}
//		fs.Write(file, fileData.File, fileData.Size)
//		fs.Flush(file)
//		fs.CloseFile(file)
//
//	}
//
//	fs.Disconnect()
//
//	return "done"
//}

func standardWrite(resp s3.ListResp, b s3.Bucket, directory string, startIndex int, hashCheck bool) string {

	fmt.Println("Running files: ", len(resp.Contents)-1)

	for j := startIndex; j <= len(resp.Contents)-1; j++ {

		fileData := s3Get(j, resp, b, directory)

		// Check for Directory
		err := os.MkdirAll(fileData.WriteLocation, 0777)
		if err != nil {
			panic(err)
		}

		fmt.Println("Writing File: ", path.Join(fileData.WriteLocation, fileData.FileName))

		file, _ := os.Create(path.Join(fileData.WriteLocation, fileData.FileName))
		writer := bufio.NewWriter(file)
		writer.Write(fileData.File)
		writer.Flush()
		file.Close()

		if hashCheck == true {

			file, _ := ioutil.ReadFile(fileData.WriteLocation)

			s3Hash := strings.Replace(fileData.MD5Sum, "\"", "", -1)
			hash := md5.New()
			hash.Write(file)
			hashString := hex.EncodeToString(hash.Sum([]byte{}))

			if hashString != s3Hash {
				fmt.Println("File MD5 Hash Unmatched")
				j--
			}
		}

	}

	return "done"
}

//func configureHadoop(javaHome, ldPath string) {
//
//	//Set Java Home
//	os.Setenv("JAVA_HOME", javaHome)
//	os.Setenv("LD_LIBRARY_PATH", ldPath)
//
//	//Build CLASSPATH
//
//	//classpath := "/home/hadoop/conf:" + javaHome + "lib/tools.jar"
//	classpath := "/etc/hadoop/conf:" + javaHome + "lib/tools.jar"
//
//	//Grab JAR Files from Hadoop Root Dir
//	//rootList, err := ioutil.ReadDir("/home/hadoop/")
//	rootList, err := ioutil.ReadDir("/usr/lib/hadoop/")
//	if err != nil {
//		panic(err)
//	}
//
//	for _, fi := range rootList {
//		//classpath = classpath + ":/home/hadoop/" + fi.Name()
//		classpath = classpath + ":/usr/lib/hadoop/" + fi.Name()
//	}
//
//	// Grab JAR Files from Hadoop Lib Dir
//	//libList, err := ioutil.ReadDir("/home/hadoop/lib/")
//	libList, err := ioutil.ReadDir("/usr/lib/hadoop/lib/")
//	if err != nil {
//		panic(err)
//	}
//
//	for _, fi := range libList {
//		//classpath = classpath + ":/home/hadoop/lib/" + fi.Name()
//		classpath = classpath + ":/usr/lib/hadoop/lib/" + fi.Name()
//	}
//
//	os.Setenv("CLASSPATH", classpath)
//
//	// Connect to Hadoop
//
//}

func addJobs(jobs chan<- Job, files []os.FileInfo, results chan<- Result) {
	for _, file := range files {
		jobs <- Job{file.Name(), results}
	}
	close(jobs)
}

func doJobs(done chan<- struct{}, b s3.Bucket, dir string, prefix string, jobs <-chan Job) {
	for job := range jobs {
		s3Put(b, job.filename, dir, prefix)
	}
	done <- struct{}{}

}

func s3Put(b s3.Bucket, filename string, dir string, prefix string) {
	readLocation := path.Join(dir, filename)
	s3Path := path.Join(prefix, filename)
	fmt.Println("Reading ", readLocation)
	file, err := ioutil.ReadFile(readLocation)
	if err != nil {
		panic(err)
	}

	fmt.Println("Putting ", s3Path)
	b.Put(s3Path, file, "binary/octet-stream", s3.Private)
}

func s3Ls(b s3.Bucket, prefix string, nextMarker string, maxMarker int) (isTruncated bool, next string) {

	resp, err := b.List(prefix, "", nextMarker, maxMarker)
	if err != nil {
		panic(err)
	}
	fmt.Println(resp)
	return resp.IsTruncated, resp.Contents[len(resp.Contents)-1].Key
}

func main() {

	// Define flags
	//srcbucket := flag.String("bucket", "bmi-weather-test", "Bucket from which to retrieve files")
	//prefix := flag.String("prefix", "Temp/", "Prefix from which to retrieve files")
	//directory := flag.String("dir", "/Users/nrobison/Developer/git/ParS3/", "Directory to store files")
	maxMarker := flag.Int("max", 1000, "Max Markers to Retrieve per Worker")
	hadoopWrite := flag.Bool("hadoop", false, "Write Files to Hadoop Destination")
	hashCheck := flag.Bool("hash", false, "Check MD5 Hashes of Downloaded Files")
	//gets := flag.Bool("gets", false, "Get Files from S3")
	flag.Parse()

	verb := strings.ToLower(os.Args[1])
	var srcdir string = os.Args[2]
	var destdir string = os.Args[3]
	//var destdir string = ""

	hadoop := *hadoopWrite
	//
	//	hadoopfill := hdfs.FileInfo {
	//		Name: "test",
	//		}
	//	fmt.Println(hadoopfill.Name)

	// Get User HomeDir
	currentUser, err := user.Current()
	if err != nil {
		panic(err)
	}

	// Read in Config File
	//TODO Add Error Checking for Config File
	configDir := currentUser.HomeDir + "/.ParS3"

	file, err := ioutil.ReadFile(configDir)
	if err != nil {
		panic(err)
	}

	//var cfgFile AWSCredentialsType
	var cfgFile ConfigSettingsType
	json.Unmarshal(file, &cfgFile)

	auth := aws.Auth{
		AccessKey: cfgFile.AWSCredentials.AccessKey,
		SecretKey: cfgFile.AWSCredentials.SecretKey,
	}

	//	if hadoop == true {
	//		configureHadoop(cfgFile.System.javaHome, cfgFile.System.ldPath)
	//	}

	e := s3.New(auth, aws.USEast)

	//fmt.Println("Number of workers: ", workers)

	if verb == "get" {
		// Check for Valid Src Bucket
		if strings.HasPrefix(srcdir, "s3://") != true {
			fmt.Println("Not an S3 bucket")
			os.Exit(1)
		}

		// Check for Directory
		errdir := os.MkdirAll(destdir, 0777)
		if errdir != nil {
			panic(errdir)
		}

		// Format source string
		srcdir = strings.Replace(srcdir, "s3://", "", 1)
		srcdir2 := strings.Split(srcdir, "/")
		srcbucket := srcdir2[0]
		prefix := strings.Join(srcdir2[1:], "/")

		// Bucket

		b := s3.Bucket{
			S3:   e,
			Name: srcbucket,
		}

		i := s3.Bucket(b)

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
				go getandsave(i, prefix, left, right, done, z, destdir, *maxMarker, hadoop, *hashCheck)
				//nextLoop = <-right
				left = right
				//		fmt.Println("Running next on: ", nextLoop.nextMarker)
				//fmt.Println(nextLoop.markersReturned)
				//		left = make(chan continueOn)
			}
			go func(c chan continueOn) { c <- nextLoop }(right)
			nextLoop = <-leftmost
			awaitCompletion(done)
		}
	} else if verb == "put" {

		// Check for Valid Dest Bucket
		if strings.HasPrefix(destdir, "s3://") != true {
			fmt.Println("Not an S3 bucket")
			os.Exit(1)
		}

		// Format Dest string
		destdir := strings.Replace(destdir, "s3://", "", 1)
		destdir2 := strings.Split(destdir, "/")
		destbucket := destdir2[0]
		prefix := strings.Join(destdir2[1:], "/")

		// Bucket

		b := s3.Bucket{
			S3:   e,
			Name: destbucket,
		}

		i := s3.Bucket(b)

		// Get Directory List
		files, _ := ioutil.ReadDir(srcdir)
		fmt.Printf("Uploading %d files\n", len(files))

		jobs := make(chan Job, workers)
		results := make(chan Result, len(files))
		done := make(chan struct{}, workers)

		go addJobs(jobs, files, results)
		for j := 0; j < workers; j++ {
			go doJobs(done, i, srcdir, prefix, jobs)
		}
		awaitCompletion(done)
		close(results)

	} else if verb == "ls" {

		// Check for Valid Bucket
		if strings.HasPrefix(srcdir, "s3://") != true {
			fmt.Println("Not an S3 bucket")
			os.Exit(1)
		}

		// Format string
		srcdir = strings.Replace(srcdir, "s3://", "", 1)
		srcdir2 := strings.Split(srcdir, "/")
		srcbucket := srcdir2[0]
		prefix := strings.Join(srcdir2[1:], "/")

		// Bucket

		b := s3.Bucket{
			S3:   e,
			Name: srcbucket,
		}

		//isTruncated := true
		nextMarker := ""

		//for  isTruncated == true {

		//isTruncated, nextMarker =
		s3Ls(b, prefix, nextMarker, *maxMarker)
		//}

	} else {

		fmt.Println("Invalid Action")
		os.Exit(1)
	}

}
