package util

const BucketNameLength = 6

const (
	BucketGenerateMessageType         = 1000
	BucketGenerateResponseMessageType = 1003
	BucketPutBytesMessageType         = 1001
	BucketPutBytesResponseMessageType = 1004
	BucketGetBytesMessageType         = 1002
	BucketGetBytesResponseMessageType = 1005
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
	UniqueIdentifier         [BucketNameLength]byte
}

// BucketPutBytesRequest Put the users bytes in the bucket
type BucketPutBytesRequest struct {
	Header
	UniqueIdentifier [BucketNameLength]byte
	NumBytes         int64
}

// BucketPutBytesResponse
type BucketPutBytesResponse struct {
	Header
	ErrorCode int32
}

type BucketGetBytesRequest struct {
	Header
	UniqueIdentifier [BucketNameLength]byte
}

type BucketGetBytesResponse struct {
	Header
	ErrorCode int32
	Size      int64
}
