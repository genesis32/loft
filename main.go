package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"

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
	ListenPort     string
}

const (
	bucketGenerateRequestType = 1000
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
	UniqueIdentifier string
}

// BucketPutBytesResponse  lbha
type BucketPutBytesResponse struct {
	Header
	ErrorCode int32
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
func bucketGenerate(request BucketGenerateRequest, connectionByteBuffer *bytes.Buffer) (BucketGenerateResponse, error) {
	bucketName := generateBucketName(6)
	bucketGenerateResponse := BucketGenerateResponse{UniqueIdentifier: bucketName, ErrorCode: 0}
	return bucketGenerateResponse, nil
}

func bucketPutBytes(request BucketPutBytesRequest) (*BucketPutBytesResponse, error) {
	return nil, nil
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

	connectionByteBuffer := new(bytes.Buffer)
	header := Header{MessageType: bucketGenerateRequestType, Version: 1}
	err = binary.Write(connectionByteBuffer, binary.BigEndian, header.MessageType)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(connectionByteBuffer, binary.BigEndian, header.Version)
	if err != nil {
		log.Fatal(err)
	}
	_, err = conn.Write(connectionByteBuffer.Next(8))

	readBuffer := make([]byte, 1024)
	_, err = conn.Read(readBuffer)
	if err != nil {
		log.Fatal(err)
	}
	println("result: " + string(readBuffer))
	conn.Close()
}

const (
	requestPending = iota
	dataPending
	dataComplete
)

func handleServerRequest(c net.Conn) {
	connectionByteBuffer := bytes.Buffer{}
	for {
		log.Print("incoming connection")
		readBuffer := make([]byte, 1024)
		bytesRead, err := c.Read(readBuffer)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("bytes read: %d", bytesRead)
		_, err = connectionByteBuffer.Write(readBuffer)
		if err != nil {
			log.Fatal(err)
		}
		if connectionByteBuffer.Len() >= 8 {
			header := Header{}
			err = binary.Read(&connectionByteBuffer, binary.BigEndian, &header.MessageType)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("message type: %d", header.MessageType)
			err = binary.Read(&connectionByteBuffer, binary.BigEndian, &header.Version)
			if err != nil {
				log.Fatal(err)
			}
			if header.MessageType == bucketGenerateRequestType {
				bucketGenerateRequest := BucketGenerateRequest{Header: header}
				err = binary.Read(&connectionByteBuffer, binary.BigEndian, &bucketGenerateRequest.NumBytesInBucket)
				if err != nil {
					log.Fatal(err)
				}
				bucketGenerateResponse, err := bucketGenerate(bucketGenerateRequest, &connectionByteBuffer)
				if err != nil {
					log.Fatal(err)
				}
				r := []byte(bucketGenerateResponse.UniqueIdentifier)
				c.Write(r)
				break
			}
		}
	}
	c.Close()
}

func startServer() {
	var err error
	var l net.Listener
	log.Println("Starting Server")
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
