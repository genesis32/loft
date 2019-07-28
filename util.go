package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"math/rand"
	"os"
	"path"

	"github.com/pkg/errors"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generateBucketName() [bucketNameLength]byte {
	var b [bucketNameLength]byte
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func bucketNameToString(bucketName [bucketNameLength]byte) string {
	return string(bucketName[:])
}

func bucketGenerate2(request BucketGenerateRequest) (BucketGenerateResponse, error) {
	bucketName := generateBucketName()
	bucketPath := path.Join(runtimeConfig.BucketPath, bucketNameToString(bucketName))
	bucketGenerateResponse := BucketGenerateResponse{
		Header:                   Header{MessageType: bucketGenerateResponseMessageType, Version: 1},
		UniqueIdentifier:         bucketName,
		UniqueIdentifierNumBytes: bucketNameLength,
		ErrorCode:                0,
	}
	f, err := os.Create(bucketPath)
	if err != nil {
		bucketGenerateResponse.ErrorCode = 1
		return bucketGenerateResponse, err
	}

	if err := f.Truncate(request.NumBytesInBucket); err != nil {
		bucketGenerateResponse.ErrorCode = 2
		return bucketGenerateResponse, err
	}
	return bucketGenerateResponse, nil
}

func bucketGetBytes2(w *bufio.Writer, request BucketGetBytesRequest) error {
	uniqueIdentifier := string(request.UniqueIdentifier[:])
	bucketGetBytesResponse := BucketGetBytesResponse{
		Header:    Header{MessageType: bucketGetBytesResponseMessageType, Version: 1},
		ErrorCode: 0,
		Size:      -1,
	}

	bucketPath := path.Join(runtimeConfig.BucketPath, uniqueIdentifier)
	fileInfo, err := os.Stat(bucketPath)
	if err != nil {
		log.Printf("cannot find bucket '%s' err: %+v", bucketPath, err)
		bucketGetBytesResponse.ErrorCode = 1
		writeMessageToWriter(w, bucketGetBytesResponse)
		return errors.Wrapf(err, "error reading bucket")
	}

	bucketGetBytesResponse.Size = fileInfo.Size()

	writeMessageToWriter(w, bucketGetBytesResponse)
	log.Printf("Writing size: %d bytes", bucketGetBytesResponse.Size)

	fp, _ := os.Open(bucketPath)
	defer fp.Close()

	buff := make([]byte, 32*1024)
	for true {
		var err error
		bytesRead, err := fp.Read(buff)
		if bytesRead == 0 && err == io.EOF {
			break
		}
		bytesWrote, err := w.Write(buff[:bytesRead])
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Wrote %d bytes", bytesWrote)
	}

	return nil
}

func bucketPutBytes2(r io.Reader, request BucketPutBytesRequest) (BucketPutBytesResponse, error) {
	uniqueIdentifier := string(request.UniqueIdentifier[:])
	bucketPutBytesResponse := BucketPutBytesResponse{
		Header:    Header{MessageType: bucketPutBytesResponseMessageType, Version: 1},
		ErrorCode: 0,
	}

	bucketPath := path.Join(runtimeConfig.BucketPath, uniqueIdentifier)
	fileInfo, err := os.Stat(bucketPath)
	if err != nil {
		log.Printf("error putting bytes: %+v", err)
		bucketPutBytesResponse.ErrorCode = 1
		return bucketPutBytesResponse, err
	}

	numBytesToRead := fileInfo.Size()
	log.Printf("bucketName:%s bucketSize: %d", uniqueIdentifier, numBytesToRead)

	f, err := os.Create(bucketPath)
	if err != nil {
		log.Fatal(err)
	}
	buff := make([]byte, 32*1024)
	for numBytesToRead > int64(0) {
		bytesRead, err := r.Read(buff)
		if err != nil {
			f.Close()
			if err == io.EOF {
				return bucketPutBytesResponse, err
			}
			os.Remove(bucketPath)
			bucketPutBytesResponse.ErrorCode = 2
			return bucketPutBytesResponse, err
		}
		nb, err := f.Write(buff[:bytesRead])
		log.Printf("wrote %d bytes to file", nb)
		if err != nil {
			f.Close()
			os.Remove(bucketPath)
			bucketPutBytesResponse.ErrorCode = 3
			return bucketPutBytesResponse, err
		}
		numBytesToRead -= int64(bytesRead)
		log.Printf("number of bytes left to read: %d", numBytesToRead)
	}

	return bucketPutBytesResponse, nil
}

func deserializeMessage2(messageBuffer *bytes.Buffer) (interface{}, error) {
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
	case bucketGenerateMessageType:
		ret := BucketGenerateRequest{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.NumBytesInBucket)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case bucketPutBytesMessageType:
		ret := BucketPutBytesRequest{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case bucketGetBytesMessageType:
		ret := BucketGetBytesRequest{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case bucketGenerateResponseMessageType:
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
	case bucketPutBytesResponseMessageType:
		ret := BucketPutBytesResponse{Header: header}
		err = binary.Read(messageBuffer, binary.BigEndian, &ret.ErrorCode)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case bucketGetBytesResponseMessageType:
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

func writeMessageToWriter(w *bufio.Writer, message interface{}) error {
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
