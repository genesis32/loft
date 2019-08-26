package util

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/pkg/errors"
)

var Verbose bool

func VPrintfOut(format string, v ...interface{}) {
	if Verbose {
		if len(v) == 0 {
			fmt.Fprintf(os.Stdout, format)
		} else {
			fmt.Fprintf(os.Stdout, format, v)
		}
	}
}

func VPrintfErr(format string, v ...interface{}) {
	if Verbose {
		if len(v) == 0 {
			fmt.Fprintf(os.Stderr, format)
		} else {
			fmt.Fprintf(os.Stderr, format, v)
		}
	}
}

func DeserializeMessage2(messageBuffer *bytes.Buffer) (interface{}, error) {
	var err error

	header := Header{}
	err = binary.Read(messageBuffer, binary.BigEndian, &header.MessageType)
	if err != nil {
		return nil, err
	}
	log.Printf("message type: %d", header.MessageType)
	err = binary.Read(messageBuffer, binary.BigEndian, &header.Version)
	if err != nil {
		return nil, err
	}

	switch header.MessageType {
	case BucketGenerateMessageType:
		ret := BucketGenerateRequest{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.NumBytesInBucket)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case BucketPutBytesMessageType:
		ret := BucketPutBytesRequest{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.NumBytes)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case BucketGetBytesMessageType:
		ret := BucketGetBytesRequest{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case BucketGenerateResponseMessageType:
		ret := BucketGenerateResponse{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.ErrorCode)
		if err != nil {
			return nil, err
		}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.UniqueIdentifierNumBytes)
		if err != nil {
			return nil, err
		}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case BucketPutBytesResponseMessageType:
		ret := BucketPutBytesResponse{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.ErrorCode)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case BucketGetBytesResponseMessageType:
		ret := BucketGetBytesResponse{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.ErrorCode)
		if err != nil {
			return nil, err
		}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.Size)
		if err != nil {
			return nil, err
		}
		return ret, nil
	}
	return nil, errors.New("unmapped message type")
}

func WriteMessageToWriter(w *bufio.Writer, message interface{}) error {
	var err error
	serializedMessage, err := SerializeMessage2(message)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize message")
	}
	_, err = w.Write([]byte{byte(serializedMessage.Len())})
	if err != nil {
		return errors.Wrapf(err, "failed to write size of message to connection")
	}
	_, err = w.Write(serializedMessage.Next(serializedMessage.Len()))
	if err != nil {
		return errors.Wrapf(err, "failed to write message to connection")
	}
	w.Flush()
	return nil
}

func SerializeMessage2(message interface{}) (*bytes.Buffer, error) {
	var err error
	byteBuffer := new(bytes.Buffer)
	switch v := message.(type) {
	case BucketGenerateRequest:
		if err = binary.Write(byteBuffer, binary.BigEndian, v.MessageType); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Version); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.NumBytesInBucket); err != nil {
			return nil, err
		}
		return byteBuffer, nil
	case BucketPutBytesRequest:
		if err = binary.Write(byteBuffer, binary.BigEndian, v.MessageType); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Version); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.UniqueIdentifier); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.NumBytes); err != nil {
			return nil, err
		}
		return byteBuffer, nil
	case BucketGetBytesRequest:
		if err = binary.Write(byteBuffer, binary.BigEndian, v.MessageType); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Version); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.UniqueIdentifier); err != nil {
			return nil, err
		}
		return byteBuffer, nil
	case BucketGenerateResponse:
		if err = binary.Write(byteBuffer, binary.BigEndian, v.MessageType); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Version); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.ErrorCode); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.UniqueIdentifierNumBytes); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.UniqueIdentifier); err != nil {
			return nil, err
		}
		return byteBuffer, nil
	case BucketPutBytesResponse:
		if err = binary.Write(byteBuffer, binary.BigEndian, v.MessageType); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Version); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.ErrorCode); err != nil {
			return nil, err
		}
		return byteBuffer, nil
	case BucketGetBytesResponse:
		if err = binary.Write(byteBuffer, binary.BigEndian, v.MessageType); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Version); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.ErrorCode); err != nil {
			return nil, err
		}
		if err = binary.Write(byteBuffer, binary.BigEndian, v.Size); err != nil {
			return nil, err
		}
		return byteBuffer, nil
	}
	return nil, errors.New("unmapped type to serialize")
}
