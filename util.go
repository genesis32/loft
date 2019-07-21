package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math/rand"
	"os"
	"path"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generateBucketName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func bucketGenerate2(request BucketGenerateRequest) (BucketGenerateResponse, error) {
	bucketName := generateBucketName(bucketNameLength)
	bucketPath := path.Join(runtimeConfig.BucketPath, bucketName)
	bucketGenerateResponse := BucketGenerateResponse{UniqueIdentifier: bucketName, ErrorCode: 0}
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

func bucketGetBytes2(w io.Writer, request BucketGetBytesRequest) (BucketGetBytesResponse, error) {
	uniqueIdentifier := string(request.UniqueIdentifier[:])
	bucketGetBytesResponse := BucketGetBytesResponse{ErrorCode: 0}

	bucketPath := path.Join(runtimeConfig.BucketPath, uniqueIdentifier)
	fileInfo, err := os.Stat(bucketPath)
	if err != nil {
		log.Printf("cannot find bucket '%s' err: %+v", bucketPath, err)
		bucketGetBytesResponse.ErrorCode = 1
		binary.Write(w, binary.BigEndian, int64(8))
		binary.Write(w, binary.BigEndian, int64(-1))
		return bucketGetBytesResponse, err
	}

	bucketGetBytesResponse.Size = fileInfo.Size()

	binary.Write(w, binary.BigEndian, int64(8))
	binary.Write(w, binary.BigEndian, bucketGetBytesResponse.Size)
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

	return bucketGetBytesResponse, nil
}

func bucketPutBytes2(r io.Reader, request BucketPutBytesRequest) (BucketPutBytesResponse, error) {
	uniqueIdentifier := string(request.UniqueIdentifier[:])
	bucketPutBytesResponse := BucketPutBytesResponse{UniqueIdentifier: uniqueIdentifier, ErrorCode: 0}

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

	// open file
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
	}
	return nil, errors.New("unmapped message type")
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
	}
	return nil, errors.New("unmapped type to serialize")
}
