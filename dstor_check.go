package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"os"
	"s3client"
//	"strings"
	"sync"
//	"sync/atomic"
//	"container/list"
	"time"
)


type SessInfo struct{
	S3 *s3.S3
	ObjMu sync.Mutex
	DirMu sync.Mutex
	ObjFile *os.File
	DirFile *os.File
}
type object struct{
	size uint64
	mtime time.Time
	cptime time.Time
}
var OBJECT1FILE = "S_Object"
var OBJECT2FILE = "D_Object"
var DIR1FILE = "S_Dir"
var DIR2FILE = "D_Dir"
var BUCKET1FILE = "S_Bucket"
var BUCKET2FILE = "D_Bucket"

var TimeFormat = "2017-11-23T14:18:00"

func ListObject(sess *s3.S3, bucket string, Marker string, prefix string, omap map[string]object, dmap map[string]bool){
	marker := aws.String(Marker)
	now := time.Now()
	for{
		params := &s3.ListObjectsInput{
			Bucket:	aws.String(bucket),
			Delimiter: aws.String("/"),
			Marker:	marker,
			MaxKeys: aws.Int64(1000),
			Prefix: aws.String(prefix),
		}
		resp, err := sess.ListObjects(params)
		if err != nil{
			panic(err)
			return
		}
		for _,dir := range resp.CommonPrefixes{
			//strip trailing/
			dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
			//strip previous prefix
			dirName = dirName[len(*params.Prefix):]
			if len(dirName) == 0{
				continue
			}
			//fmt.Println(*dir.Prefix)
			dmap[*dir.Prefix] = true
		}
		for _,obj := range resp.Contents{
			baseName := (*obj.Key)[len(prefix):]
			if len(baseName) == 0{
				continue
			}
			//fmt.Println(*obj.Size)
			var ob object
			ob.size = uint64(*obj.Size)
			ob.mtime = *obj.LastModified
			ob.cptime = now
			omap[*obj.Key] = ob
		}

		if *resp.IsTruncated{
			marker = resp.NextMarker
		}else{
		//	marker = nil
			return
		}
	}
}

func CompareObj(map1 map[string]object, map2 map[string]object){
	for k,v1 := range map1{
		if v2,ok := map2[k];ok{
			if v1.size == v2.size{
				delete(map1, k)
				delete(map2, k)
			}
		}
	}
}

func CurrentDir(Sess1 *SessInfo, Sess2 *SessInfo, DirNum *sync.WaitGroup, Prefix string, PrefixCh chan string, Sess1Bucket string, Sess2Bucket string){
	
	var Object1Map = make(map[string]object)
	var Object2Map = make(map[string]object)
	var Dir1Map = make(map[string]bool)
	var Dir2Map = make(map[string]bool)
	ListObject(Sess1.S3, Sess1Bucket, "", Prefix, Object1Map, Dir1Map)
	ListObject(Sess2.S3, Sess2Bucket, "", Prefix, Object2Map, Dir2Map)
	
	CompareObj(Object1Map, Object2Map)
	
	for name,v := range Object1Map{
		//fmt.Println(name)
		Sess1.ObjMu.Lock()
		_,err := Sess1.ObjFile.WriteString(Sess1Bucket+" "+name+" "+v.mtime.UTC().Format(time.RFC3339)+" "+v.cptime.UTC().Format(time.RFC3339)+"\n")
		if nil != err{
			panic(err)
		}
		Sess1.ObjMu.Unlock()
	}
	
	for name,v := range Object2Map{
		//fmt.Println(name)
		Sess2.ObjMu.Lock()
		_,err := Sess2.ObjFile.WriteString(Sess2Bucket+" "+name+" "+v.mtime.UTC().Format(time.RFC3339)+" "+v.cptime.UTC().Format(time.RFC3339)+" "+"\n")
		if nil != err{
			panic(err)
		}
		Sess2.ObjMu.Unlock()
	}
	
	for k,_ := range Dir1Map{
		if _,ok := Dir2Map[k];ok{
			PrefixCh <-k
			DirNum.Add(1)
			delete(Dir1Map, k)
			delete(Dir2Map, k)
		}
	}
	
	for name,_ := range Dir1Map{
		//fmt.Println(name)
		Sess1.DirMu.Lock()
		_,err := Sess1.DirFile.WriteString(Sess1Bucket+" "+name+"\n")
		if nil != err{
			panic(err)
		}
		Sess1.DirMu.Unlock()
	}
	
	for name,_ := range Dir2Map{
		//fmt.Println(name)
		Sess2.DirMu.Lock()
		_,err := Sess2.DirFile.WriteString(Sess2Bucket+" "+name+"\n")
		if nil != err{
			panic(err)
		}
		Sess2.DirMu.Unlock()
	}
}
func CheckStart(Sess1 *SessInfo, Sess2 *SessInfo, DirNum *sync.WaitGroup, PrefixCh chan string, DoneCh chan string, Num int, Sess1Bucket string, Sess2Bucket string){
	for x := 0; x < Num; x++{
		go func() {
			for {
				select {
					case Prefix, ok := <-PrefixCh:
						if !ok {
							return
						}
						CurrentDir(Sess1, Sess2, DirNum, Prefix, PrefixCh, Sess1Bucket, Sess2Bucket)
						DirNum.Done()
					case <-DoneCh:
						return
				}
			}
		}()
	}
}

func CheckBucketObject(Sess1 *SessInfo, Sess2 *SessInfo, Num int, Sess1Bucket string, Sess2Bucket string){
	
	var DirNum sync.WaitGroup
	var PrefixCh = make(chan string, 5120000)
	var DoneCh = make(chan string)
	PrefixCh <-""
	DirNum.Add(1)
	for{
		CheckStart(Sess1, Sess2, &DirNum, PrefixCh, DoneCh, Num, Sess1Bucket, Sess2Bucket)
		DirNum.Wait()
		if(0 == len(PrefixCh)){
			break
		}
	}
}

func ListBuckets(sess *s3.S3, BucketMap map[string]bool){
	params := &s3.ListBucketsInput{
	}
	resp, err := sess.ListBuckets(params)
	if err != nil{
		panic(err)
		return
	}

	for _, b := range resp.Buckets {
		bucket := aws.StringValue(b.Name)
		BucketMap[bucket] = true
		//fmt.Println(bucket)
	}
}

func CheckBuckets(Sess1 *SessInfo, Sess2 *SessInfo, BucketMap map[string]bool){

	var Bucket1Map = make(map[string]bool)
	var Bucket2Map = make(map[string]bool)
	
	ListBuckets(Sess1.S3, Bucket1Map)
	ListBuckets(Sess2.S3, Bucket2Map)
	for k,_ := range Bucket1Map{
		if _,ok := Bucket2Map[k];ok{
			BucketMap[k] = true
			delete(Bucket1Map, k)
			delete(Bucket2Map, k)
		}
	}
	
	Buk1File,err := os.OpenFile(BUCKET1FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	Buk2File,err := os.OpenFile(BUCKET2FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	for name,_ := range Bucket1Map{
		//fmt.Println(name)
		Buk1File.WriteString(name+"\n")
	}	
	for name,_ := range Bucket2Map{
		//fmt.Println(name)
		Buk2File.WriteString(name+"\n")
	}

	if err := Buk1File.Close(); err != nil{
		panic(err)
	}
	if err := Buk2File.Close(); err != nil{
		panic(err)
	}
}

func main() {	
	var Sh string
	var Sid string
	var Skey string
	var Sb string
	var Dh string
	var Did string
	var Dkey string
	var Db string
	var GoNum int
	var H bool
	
	flag.StringVar(&Sh, "sh", "", "src host.")
	flag.StringVar(&Sid, "sid", "", "src AccessKeyId.")
	flag.StringVar(&Skey, "skey", "", "src SecretKey.")
	flag.StringVar(&Sb, "sb", "", "src Bucket name.")
	flag.StringVar(&Dh, "dh", "", "dest host.")
	flag.StringVar(&Did, "did", "", "dest AccessKeyId.")
	flag.StringVar(&Dkey, "dkey", "", "dest SecretKey.")
	flag.StringVar(&Db, "db", "", "dest Bucket name.")
	flag.IntVar(&GoNum, "n", 256, "Goroutines num.")
	flag.BoolVar(&H, "h", false, "this help")
	flag.Parse()
	
	if H || 
	   "" == Sh ||
	   "" == Sid ||
	   "" == Skey ||
	   "" == Dh ||
	   "" == Did ||
	   "" == Dkey{
		flag.Usage()
		fmt.Println("Examples:\n dstor_check -sh 127.0.0.1:6081  -sid KD18D4O4SVHFS40TMA5D  -skey SPuJ2JRyYlg6KfIEX03MkZ46hHbRmqqEKn2IEJZR  -dh 192.168.122.142:6081 -did KD18D4O4SVHFS40TMA5D -dkey SPuJ2JRyYlg6KfIEX03MkZ46hHbRmqqEKn2IEJZR")
		return
	}
	
	var Sess1 SessInfo
	var Sess2 SessInfo
	var err error
	Sess1.S3 = s3client.NewS3Client(Sid , Skey, Sh) 
	Sess2.S3 = s3client.NewS3Client(Did , Dkey, Dh)
	Sess1.ObjFile,err = os.OpenFile(OBJECT1FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	Sess1.DirFile,err = os.OpenFile(DIR1FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	Sess2.ObjFile,err = os.OpenFile(OBJECT2FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	Sess2.DirFile,err = os.OpenFile(DIR2FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	
	if "" == Sb ||
	   "" == Db{
		var BucketMap = make(map[string]bool)
		CheckBuckets(&Sess1, &Sess2, BucketMap)
		for Bucket,_ := range BucketMap{
			CheckBucketObject(&Sess1 ,&Sess2, GoNum, Bucket, Bucket)
		}
	}else{
		CheckBucketObject(&Sess1 ,&Sess2, GoNum, Sb, Db)
	}
	
	if err := Sess1.ObjFile.Close(); err != nil{
		panic(err)
	}
	if err := Sess1.DirFile.Close(); err != nil{
		panic(err)
	}
	if err := Sess2.ObjFile.Close(); err != nil{
		panic(err)
	}
	if err := Sess2.DirFile.Close(); err != nil{
		panic(err)
	}

	var Obj1Info os.FileInfo
	var Obj2Info os.FileInfo
	var Dir1Info os.FileInfo
	var Dir2Info os.FileInfo
	
	if Obj1Info, err = os.Stat(OBJECT1FILE); err != nil{
		panic(err)
	}
	if Obj2Info, err = os.Stat(OBJECT2FILE); err != nil{
		panic(err)
	}
	if Dir1Info, err = os.Stat(DIR1FILE); err != nil{
		panic(err)
	}
	if Dir2Info, err = os.Stat(DIR2FILE); err != nil{
		panic(err)
	}
	
	var Buf string
	if "" == Sb ||
	   "" == Db{
		var Buk1Info os.FileInfo
		var Buk2Info os.FileInfo

		if Buk1Info, err = os.Stat(BUCKET1FILE); err != nil{
			panic(err)
		}
		if Buk2Info, err = os.Stat(BUCKET2FILE); err != nil{
			panic(err)
		}
		if 0 != Buk1Info.Size(){
			Buf = Buf + BUCKET1FILE + " \n"
		}else{
			if err = os.Remove(BUCKET1FILE); err != nil{
				panic(err)
			}
		}
		if 0 != Buk2Info.Size(){
			Buf = Buf + BUCKET2FILE + " \n"
		}else{
			if err = os.Remove(BUCKET2FILE); err != nil{
				panic(err)
			}
		}
	}

	if 0 != Obj1Info.Size(){
		Buf = Buf + OBJECT1FILE + " \n"
	}else{
		if err = os.Remove(OBJECT1FILE); err != nil{
			panic(err)
		}
	}
	if 0 != Obj2Info.Size(){
		Buf = Buf + OBJECT2FILE + " \n"
	}else{
		if err = os.Remove(OBJECT2FILE); err != nil{
			panic(err)
		}
	}
	if 0 != Dir1Info.Size(){
		Buf = Buf + DIR1FILE + " \n"
	}else{
		if err = os.Remove(DIR1FILE); err != nil{
			panic(err)
		}
	}
	if 0 != Dir2Info.Size(){
		Buf = Buf + DIR2FILE + " \n"
	}else{
		if err = os.Remove(DIR2FILE); err != nil{
			panic(err)
		}
	}
	
	if 0 == len(Buf){
		fmt.Printf("\033[;32mPASS\033[5m")
		fmt.Println("\033[;\033[0m")
	}else{
		fmt.Printf("\033[;31mNOT PASS\033[5m")
		fmt.Println("\033[;\033[0m")
		fmt.Printf(Buf)
	}
}
