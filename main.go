package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

var runtimeConfig configuration

/*
openssl ecparam -genkey -name prime256v1 -out server.key
openssl req -new -x509 -key server.key -out server.pem -days 3650
*/

/*
operations: bucketget,bucketcombine
*/

type configuration struct {
	SslEnabled     bool
	ServerKeyPath  string
	ServerCertPath string
	BucketPath     string
	ListenPort     string
}

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
	ErrorCode int32
}

type BucketGetBytesRequest struct {
	Header
	UniqueIdentifier [bucketNameLength]byte
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generateBucketName(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// TODO: wrap these functions in the binary protocol parser
func bucketGenerate(request BucketGenerateRequest) (BucketGenerateResponse, error) {
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

func bucketPutBytes(request BucketPutBytesRequest) (*BucketPutBytesResponse, error) {
	return nil, nil
}

func serializeMessage(message interface{}) (*bytes.Buffer, error) {
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

func deserializeMessage(messageBytes *bytes.Buffer) (interface{}, error) {
	var err error

	header := Header{}
	err = binary.Read(messageBytes, binary.BigEndian, &header.MessageType)
	if err != nil {
		return nil, err
	}
	log.Printf("message type: %d", header.MessageType)
	err = binary.Read(messageBytes, binary.BigEndian, &header.Version)
	if err != nil {
		return nil, err
	}

	switch header.MessageType {
	case bucketGenerateMessageType:
		ret := BucketGenerateRequest{Header: header}
		err = binary.Read(messageBytes, binary.BigEndian, &ret.NumBytesInBucket)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case bucketPutBytesMessageType:
		ret := BucketPutBytesRequest{Header: header}
		err = binary.Read(messageBytes, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		return ret, nil
	case bucketGetBytesMessageType:
		ret := BucketGetBytesRequest{Header: header}
		err = binary.Read(messageBytes, binary.BigEndian, &ret.UniqueIdentifier)
		if err != nil {
			return nil, err
		}
		return ret, nil
	}
	return nil, errors.New("unmapped message type")
}

func startClient(destinationIPAndPort string) {
	var err error
	var conn net.Conn
	fmt.Println("foo", destinationIPAndPort)
	if runtimeConfig.SslEnabled {
		rootCert, err := ioutil.ReadFile("../loftserver/server.pem")
		if err != nil {
			log.Fatal(err)
		}
		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM([]byte(rootCert))
		if !ok {
			log.Fatal("failed to parse root certificate")
		}
		config := &tls.Config{RootCAs: roots}

		conn, err = tls.Dial("tcp", destinationIPAndPort, config)
	} else {
		conn, err = net.Dial("tcp", destinationIPAndPort)
	}
	if err != nil {
		log.Fatal(err)
	}

	bufferReader := bufio.NewReader(conn)
	for {
		var uniqIdentifier [bucketNameLength]byte
		{
			bucketGenerateRequest := BucketGenerateRequest{Header: Header{MessageType: bucketGenerateMessageType, Version: 1}, NumBytesInBucket: 24}
			connectionByteBuffer, err := serializeMessage(bucketGenerateRequest)
			if err != nil {
				log.Fatal(err)
			}
			conn.Write([]byte{byte(connectionByteBuffer.Len())})
			_, err = conn.Write(connectionByteBuffer.Next(connectionByteBuffer.Len()))

			buffMessageLength := make([]byte, 8)
			_, err = io.ReadFull(bufferReader, buffMessageLength)
			if err != nil {
				log.Fatal(err)
			}
			var size int64
			binary.Read(bytes.NewBuffer(buffMessageLength), binary.BigEndian, &size)
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("size: %+v", size)
			buffMessage := make([]byte, 256)
			io.ReadFull(bufferReader, buffMessage[:size])
			copy(uniqIdentifier[:], buffMessage[:size])
			log.Printf("bucket id: %s", string(uniqIdentifier[:]))
			time.Sleep(100 * time.Millisecond)
		}

		{
			bucketPutRequest := BucketPutBytesRequest{Header: Header{MessageType: bucketPutBytesMessageType, Version: 1}, UniqueIdentifier: uniqIdentifier}
			connectionByteBuffer, err := serializeMessage(bucketPutRequest)
			if err != nil {
				log.Fatal(err)
			}
			conn.Write([]byte{byte(connectionByteBuffer.Len())})
			_, err = conn.Write(connectionByteBuffer.Next(connectionByteBuffer.Len()))

			buffMessageLength := make([]byte, 8)
			_, err = io.ReadFull(bufferReader, buffMessageLength)
			var size int64
			binary.Read(bytes.NewBuffer(buffMessageLength), binary.BigEndian, &size)
			if err != nil {
				log.Fatal(err)
			}

			buffMessage := make([]byte, 256)
			io.ReadFull(bufferReader, buffMessage[:size])
			log.Printf("put result: %+v", string(buffMessage[:size]))
			time.Sleep(100 * time.Millisecond)
		}

		{
			bucketGetRequest := BucketGetBytesRequest{Header: Header{MessageType: bucketGetBytesMessageType, Version: 1}, UniqueIdentifier: uniqIdentifier}
			connectionByteBuffer, err := serializeMessage(bucketGetRequest)
			if err != nil {
				log.Fatal(err)
			}
			conn.Write([]byte{byte(connectionByteBuffer.Len())})
			_, err = conn.Write(connectionByteBuffer.Next(connectionByteBuffer.Len()))

			buffMessageLength := make([]byte, 8)
			_, err = io.ReadFull(bufferReader, buffMessageLength)
			var size int64
			binary.Read(bytes.NewBuffer(buffMessageLength), binary.BigEndian, &size)
			if err != nil {
				log.Fatal(err)
			}

			buffMessage := make([]byte, 256)
			io.ReadFull(bufferReader, buffMessage[:size])
			log.Printf("get result: %+v", string(buffMessage[:size]))
			time.Sleep(100 * time.Millisecond)
		}
	}
	conn.Close()
}

const (
	requestPending = iota
	messagePending
	dataPending
	dataComplete
)

func handleServerRequest(c net.Conn) {
	for {
		var err error
		log.Print("incoming connection")
		buff := make([]byte, 256)
		bufferReader := bufio.NewReader(c)
		// TODO: Switch to 64bit
		size, err := bufferReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		buf := new(bytes.Buffer)
		_, err = io.ReadFull(bufferReader, buff[:size])
		if err != nil {
			log.Fatal(err)
		}
		messageBuffer := bytes.NewBuffer(buff)
		message, err := deserializeMessage(messageBuffer)
		if err != nil {
			log.Fatal(err)
		}
		switch v := message.(type) {
		case BucketGenerateRequest:
			log.Printf("BucketGenerateRequest: %+v", message)
			bucketGenerateResponse, err := bucketGenerate(v)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf(string(bucketGenerateResponse.UniqueIdentifier))
			binary.Write(buf, binary.BigEndian, int64(len(bucketGenerateResponse.UniqueIdentifier)))
			binary.Write(buf, binary.BigEndian, []byte(bucketGenerateResponse.UniqueIdentifier))
			c.Write(buf.Bytes())
		case BucketPutBytesRequest:
			log.Printf("BucketPutBytesRequest: %+v", message)
			payload := "OK"
			binary.Write(buf, binary.BigEndian, int64(len(payload)))
			binary.Write(buf, binary.BigEndian, []byte(payload))
			c.Write(buf.Bytes())
		case BucketGetBytesRequest:
			log.Printf("BucketGetBytesRequest: %+v", message)
			payload := "[[FILE CONTENTS]]"
			binary.Write(buf, binary.BigEndian, int64(len(payload)))
			binary.Write(buf, binary.BigEndian, []byte(payload))
			c.Write(buf.Bytes())
		}
	}
	c.Close()
}

func startServer() {
	var err error
	var l net.Listener
	log.Println("Starting Server bucket path:", runtimeConfig.BucketPath)
	if runtimeConfig.SslEnabled {
		serverKey, err := ioutil.ReadFile(runtimeConfig.ServerKeyPath)
		if err != nil {
			log.Fatal(err)
		}

		serverCert, err := ioutil.ReadFile(runtimeConfig.ServerCertPath)
		if err != nil {
			log.Fatal(err)
		}

		cer, err := tls.X509KeyPair([]byte(serverCert), []byte(serverKey))
		if err != nil {
			log.Fatal(err)
		}

		tlsConfig := &tls.Config{Certificates: []tls.Certificate{cer}}

		l, err = tls.Listen("tcp", runtimeConfig.ListenPort, tlsConfig)
	} else {
		l, err = net.Listen("tcp", runtimeConfig.ListenPort)
	}

	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	log.Printf("Listening for connection on %s", runtimeConfig.ListenPort)
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// TODO: Set read timeout
		go handleServerRequest(conn)
	}
}

func main() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	if err := viper.Unmarshal(&runtimeConfig); err != nil {
		log.Fatalf("Unable to decode struct, %s", err)
	}

	flag.Bool("server", false, "start the server")
	flag.String("dst", "localhost:8089", "[client mode] destination ip and port")

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	isServer := viper.GetBool("server")
	if isServer {
		startServer()
	} else {
		dst := viper.GetString("dst")
		startClient(dst)
	}
}
