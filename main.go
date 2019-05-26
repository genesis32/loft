package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"

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

// BucketGenerateRequest Generate the bucket with a 0 sized file
type BucketGenerateRequest struct {
	Version          int32
	NumBytesInBucket int64
}

// BucketGenerateResponse The response to bucket geneation
type BucketGenerateResponse struct {
	Version          int32
	ErrorCode        int32
	UniqueIdentifier string
}

// BucketPutBytesRequest Put the users bytes in the bucket
type BucketPutBytesRequest struct {
	Version          int32
	UniqueIdentifier string
}

// BucketPutBytesResponse  lbha
type BucketPutBytesResponse struct {
	Version   int32
	ErrorCode int32
}

// TODO: wrap these functions in the binary protocol parser
func bucketGenerate(request BucketGenerateRequest) (*BucketPutBytesRequest, error) {
	// generate a 0 byte file on the filesystem of the size of request.NumBytesInBucket
	// generate a 6 character long unique identifier [A-Za-z0-9]
	return nil, nil
}

func bucketPutBytes(request BucketPutBytesRequest) (*BucketPutBytesResponse, error) {
	return nil, nil
}

func startClient() {
	var err error
	var conn net.Conn
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

		conn, err = tls.Dial("tcp", "localhost:8089", config)
	} else {
		conn, err = net.Dial("tcp", "localhost:8089")
	}
	if err != nil {
		log.Fatal(err)
	}

	io.WriteString(conn, "Hello simple secure Server")
	conn.Close()
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
		go func(c net.Conn) {
			io.Copy(os.Stdout, c)
			fmt.Println()
			c.Close()
		}(conn)
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

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)

	isServer := viper.GetBool("server")
	if isServer {
		startServer()
	}
}
