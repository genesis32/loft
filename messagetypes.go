package main

const bucketNameLength = 6

const (
	bucketGenerateMessageType = 1000
	bucketPutBytesMessageType = 1001
	bucketGetBytesMessageType = 1002
)

type Header struct {
	MessageType int32
	Version     int32
}

// BucketGenerateRequest Generate the bucket with a 0 sized file
type BucketGenerateRequest struct {
	Header
	NumBytesInBucket int64
}

// BucketGenerateResponse The response to bucket geneation
type BucketGenerateResponse struct {
	Header
	ErrorCode        int32
	UniqueIdentifier string
}

// BucketPutBytesRequest Put the users bytes in the bucket
type BucketPutBytesRequest struct {
	Header
	UniqueIdentifier [bucketNameLength]byte
}

// BucketPutBytesResponse  lbha
type BucketPutBytesResponse struct {
	Header
	ErrorCode        int32
	UniqueIdentifier string
}

type BucketGetBytesRequest struct {
	Header
	UniqueIdentifier [bucketNameLength]byte
}

type BucketGetBytesResponse struct {
	Header
	ErrorCode int32
	Size      int64
}
