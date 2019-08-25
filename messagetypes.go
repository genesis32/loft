package main

const bucketNameLength = 6

const (
	bucketGenerateMessageType         = 1000
	bucketGenerateResponseMessageType = 1003
	bucketPutBytesMessageType         = 1001
	bucketPutBytesResponseMessageType = 1004
	bucketGetBytesMessageType         = 1002
	bucketGetBytesResponseMessageType = 1005
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
	ErrorCode                int32
	UniqueIdentifierNumBytes int64
	UniqueIdentifier         [bucketNameLength]byte
}

// BucketPutBytesRequest Put the users bytes in the bucket
type BucketPutBytesRequest struct {
	Header
	UniqueIdentifier [bucketNameLength]byte
	NumBytes         int64
}

// BucketPutBytesResponse
type BucketPutBytesResponse struct {
	Header
	ErrorCode int32
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
