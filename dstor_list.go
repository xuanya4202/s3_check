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
			MaxKeys: aws.Int64(2000),
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
			//fmt.Println("list object resp.NextMarker:", resp.NextMarker)
			marker = resp.NextMarker
		}else{
			//marker = nil
			//fmt.Println("list object *resp.IsTruncated:", *resp.IsTruncated)
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

func CurrentDir(Sess1 *SessInfo, DirNum *sync.WaitGroup, Prefix string, PrefixCh chan string, Sess1Bucket string){
	var Object1Map = make(map[string]object)
	var Dir1Map = make(map[string]bool)
	ListObject(Sess1.S3, Sess1Bucket, "", Prefix, Object1Map, Dir1Map)
	
	for name,_ := range Object1Map{
		//fmt.Println(name)
		Sess1.ObjMu.Lock()
		_,err := Sess1.ObjFile.WriteString(name+"\n")
		if nil != err{
			panic(err)
		}
		Sess1.ObjMu.Unlock()
	}
	
	for k,_ := range Dir1Map{
		PrefixCh <-k
		DirNum.Add(1)
	}
	
//	for name,_ := range Dir1Map{
//		Sess1.DirFile.WriteString(Sess1Bucket+" "+name+"\n")
//	}
}
func CheckStart(Sess1 *SessInfo, DirNum *sync.WaitGroup, PrefixCh chan string, DoneCh chan string, Num int, Sess1Bucket string){
	for x := 0; x < Num; x++{
		go func() {
			for {
				
				select {
					case Prefix, ok := <-PrefixCh:
						if !ok {
							return
						}
						CurrentDir(Sess1, DirNum, Prefix, PrefixCh, Sess1Bucket)
						DirNum.Done()
					case <-DoneCh:
						return
				}
			}
		}()
	}
}

func CheckBucketObject(Sess1 *SessInfo, Num int, Sess1Bucket string){
	
	var DirNum sync.WaitGroup
	var PrefixCh = make(chan string, 5120000)
	var DoneCh = make(chan string)
	PrefixCh <-""
	DirNum.Add(1)
	for{
		CheckStart(Sess1, &DirNum, PrefixCh, DoneCh, Num, Sess1Bucket)
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

func CheckBuckets(Sess1 *SessInfo, BucketMap map[string]bool){

	var Bucket1Map = make(map[string]bool)
	
	ListBuckets(Sess1.S3, Bucket1Map)
	for k,_ := range Bucket1Map{
		BucketMap[k] = true
	}
}

func main() {	
	var Sh string
	var Sid string
	var Skey string
	var Sb string
	var GoNum int
	var H bool
	
	flag.StringVar(&Sh, "sh", "", "src host.")
	flag.StringVar(&Sid, "sid", "", "src AccessKeyId.")
	flag.StringVar(&Skey, "skey", "", "src SecretKey.")
	flag.StringVar(&Sb, "sb", "", "src Bucket name.")
	flag.IntVar(&GoNum, "n", 16, "Goroutines num.")
	flag.BoolVar(&H, "h", false, "this help")
	flag.Parse()
	
	if H || 
	   "" == Sh ||
	   "" == Sid ||
	   "" == Skey{
		flag.Usage()
		fmt.Println("Examples:\n dstor_list -sh 127.0.0.1:6081  -sid KD18D4O4SVHFS40TMA5D  -skey SPuJ2JRyYlg6KfIEX03MkZ46hHbRmqqEKn2IEJZR")
		return
	}
	
	var Sess1 SessInfo
	var err error
	Sess1.S3 = s3client.NewS3Client(Sid , Skey, Sh) 
	Sess1.ObjFile,err = os.OpenFile(OBJECT1FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil{
		panic(err)
	}
	//Sess1.DirFile,err = os.OpenFile(DIR1FILE, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	//if err != nil{
	//	panic(err)
	//}
	
	if "" == Sb{
		var BucketMap = make(map[string]bool)
		CheckBuckets(&Sess1, BucketMap)
		for Bucket,_ := range BucketMap{
			CheckBucketObject(&Sess1, GoNum, Bucket)
		}
	}else{
		CheckBucketObject(&Sess1, GoNum, Sb)
	}
	
	if err := Sess1.ObjFile.Close(); err != nil{
		panic(err)
	}
	//if err := Sess1.DirFile.Close(); err != nil{
	//	panic(err)
	//}

}
