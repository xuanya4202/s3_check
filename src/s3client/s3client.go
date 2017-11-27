package s3client

import (
//	"bytes"
//	"context"
//	"io"
//	"log"
	//"os"
	//"sync"
//	"time"

	"github.com/aws/aws-sdk-go/aws"
//	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
//	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
//	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

var default_region string = "us-east-1"

func NewS3Client(accessid string, accessKey string, ipport string) *s3.S3 {
	endpoint := "http://" + ipport
	creds := credentials.NewStaticCredentials(accessid, accessKey, "")
	awsConfig := &aws.Config{
		Region:           &default_region,
		Endpoint:         &endpoint,
		S3ForcePathStyle: aws.Bool(true),
		Credentials:	  creds,
	}

	sess := session.Must(session.NewSession(awsConfig))
	svc := s3.New(sess)

	svc.Handlers.Sign.Clear()
	svc.Handlers.Sign.PushBack(SignV2)
	svc.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)

	return svc
}

