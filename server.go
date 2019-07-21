package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"

	"github.com/pkg/errors"
)

type ServerConfiguration struct {
	ListenAddrAndPort     string
	SslEnabled            bool
	SslClientCertFilePath string
	SslClientKeyFilePath  string
}

type ServerConnection struct {
	bufferedReader *bufio.Reader
	bufferedWriter *bufio.Writer
	theConn        net.Conn
}

type Server struct {
	config         ServerConfiguration
	bufferedReader *bufio.Reader
	bufferedWriter *bufio.Writer
	theListener    net.Listener
}

type LoftServer interface {
	StartAndServe() error
}

func newServerConnection(conn net.Conn) *ServerConnection {
	newConnection := &ServerConnection{theConn: conn}
	newConnection.bufferedReader = bufio.NewReader(newConnection.theConn)
	newConnection.bufferedWriter = bufio.NewWriter(newConnection.theConn)
	return newConnection
}

func newServer(config ServerConfiguration) LoftServer {
	newServer := &Server{config: config}
	return newServer
}

func handleServerRequest2(clientConn *ServerConnection) {
	for {
		var err error
		log.Print("waiting for message")
		size, err := clientConn.bufferedReader.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}

		messageBytes := make([]byte, 256)
		_, err = io.ReadFull(clientConn.bufferedReader, messageBytes[:size])
		if err != nil {
			log.Fatal(err)
		}

		messageBuffer := bytes.NewBuffer(messageBytes)
		theMessage, err := deserializeMessage2(messageBuffer)
		if err != nil {
			log.Fatal(err)
		}

		switch v := theMessage.(type) {
		case BucketGenerateRequest:
			log.Printf("BucketGenerateRequest: %+v", theMessage)
			bucketGenerateResponse, err := bucketGenerate2(v)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf(string(bucketGenerateResponse.UniqueIdentifier))
			binary.Write(clientConn.bufferedWriter, binary.BigEndian, int64(len(bucketGenerateResponse.UniqueIdentifier)))
			binary.Write(clientConn.bufferedWriter, binary.BigEndian, []byte(bucketGenerateResponse.UniqueIdentifier))
		case BucketPutBytesRequest:
			log.Printf("BucketPutBytesRequest: %+v", theMessage)
			bucketPutBytesResponse, err := bucketPutBytes2(clientConn.bufferedReader, v)
			if err != nil {
				log.Fatal(err)
			}
			var l string
			if bucketPutBytesResponse.ErrorCode == 0 {
				l = "OK"
			} else {
				l = fmt.Sprintf("ERROR_CODE=%d", bucketPutBytesResponse.ErrorCode)
			}
			log.Printf("put result code: %s", l)
			binary.Write(clientConn.bufferedWriter, binary.BigEndian, int64(bucketPutBytesResponse.ErrorCode))
		case BucketGetBytesRequest:
			log.Printf("BucketGetBytesRequest: %+v", theMessage)
			_, err := bucketGetBytes2(clientConn.bufferedWriter, v)
			if err != nil {
				log.Fatal(err)
			}
		}
		clientConn.bufferedWriter.Flush()
	}
}

func (s *Server) StartAndServe() error {
	var err error
	s.theListener, err = net.Listen("tcp", s.config.ListenAddrAndPort)
	if err != nil {
		return errors.Wrapf(err, "failed to start listener on %s", s.config.ListenAddrAndPort)
	}
	defer s.theListener.Close()

	log.Printf("Listening for connection on %s", s.config.ListenAddrAndPort)
	for {
		conn, err := s.theListener.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// TODO: Set read timeout
		clientConnection := newServerConnection(conn)
		go handleServerRequest2(clientConnection)
	}
	return nil
}
