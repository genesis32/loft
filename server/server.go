package server

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path"

	"github.com/genesis32/loft/util"
	"github.com/pkg/errors"
)

type ServerConfiguration struct {
	ListenAddrAndPort     string
	SslEnabled            bool
	SslClientCertFilePath string
	SslClientKeyFilePath  string
	BucketPath            string
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
	bucketGenerate2(request util.BucketGenerateRequest) (util.BucketGenerateResponse, error)
	bucketGetBytes2(w *bufio.Writer, request util.BucketGetBytesRequest) error
	bucketPutBytes2(r io.Reader, w *bufio.Writer, request util.BucketPutBytesRequest) (util.BucketPutBytesResponse, error)
}

func newServerConnection(conn net.Conn) *ServerConnection {
	newConnection := &ServerConnection{theConn: conn}
	newConnection.bufferedReader = bufio.NewReader(newConnection.theConn)
	newConnection.bufferedWriter = bufio.NewWriter(newConnection.theConn)
	return newConnection
}

func NewServer(config ServerConfiguration) LoftServer {
	newServer := &Server{config: config}
	return newServer
}

func handleServerRequest2(server *Server, clientConn *ServerConnection) {
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
		theMessage, err := util.DeserializeMessage2(messageBuffer)
		if err != nil {
			log.Fatal(err)
		}

		switch v := theMessage.(type) {
		case util.BucketGenerateRequest:
			log.Printf("BucketGenerateRequest: %+v", theMessage)
			bucketGenerateResponse, err := server.bucketGenerate2(v)
			if err != nil {
				log.Fatal(err)
			}
			log.Printf(string(bucketGenerateResponse.UniqueIdentifier[:]))
			util.WriteMessageToWriter(clientConn.bufferedWriter, bucketGenerateResponse)
		case util.BucketPutBytesRequest:
			log.Printf("BucketPutBytesRequest: %+v", theMessage)
			bucketPutBytesResponse, err := server.bucketPutBytes2(clientConn.bufferedReader, clientConn.bufferedWriter, v)
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
			util.WriteMessageToWriter(clientConn.bufferedWriter, bucketPutBytesResponse)
		case util.BucketGetBytesRequest:
			log.Printf("BucketGetBytesRequest: %+v", theMessage)
			err := server.bucketGetBytes2(clientConn.bufferedWriter, v)
			if err != nil {
				log.Fatal(err)
			}
		}
		clientConn.bufferedWriter.Flush()
	}
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func generateBucketName() [util.BucketNameLength]byte {
	var b [util.BucketNameLength]byte
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

func bucketNameToString(bucketName [util.BucketNameLength]byte) string {
	return string(bucketName[:])
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
		go handleServerRequest2(s, clientConnection)
	}
	return nil
}

func (s *Server) bucketGenerate2(request util.BucketGenerateRequest) (util.BucketGenerateResponse, error) {
	bucketName := generateBucketName()
	bucketPath := path.Join(s.config.BucketPath, bucketNameToString(bucketName))
	bucketGenerateResponse := util.BucketGenerateResponse{
		Header:                   util.Header{MessageType: util.BucketGenerateResponseMessageType, Version: 1},
		UniqueIdentifier:         bucketName,
		UniqueIdentifierNumBytes: util.BucketNameLength,
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

func (s *Server) bucketGetBytes2(w *bufio.Writer, request util.BucketGetBytesRequest) error {
	uniqueIdentifier := string(request.UniqueIdentifier[:])
	bucketGetBytesResponse := util.BucketGetBytesResponse{
		Header:    util.Header{MessageType: util.BucketGetBytesResponseMessageType, Version: 1},
		ErrorCode: 0,
		Size:      -1,
	}

	bucketPath := path.Join(s.config.BucketPath, uniqueIdentifier)
	fileInfo, err := os.Stat(bucketPath)
	if err != nil {
		log.Printf("cannot find bucket '%s' err: %+v", bucketPath, err)
		bucketGetBytesResponse.ErrorCode = 1
		util.WriteMessageToWriter(w, bucketGetBytesResponse)
		return errors.Wrapf(err, "error reading bucket")
	}

	bucketGetBytesResponse.Size = fileInfo.Size()

	util.WriteMessageToWriter(w, bucketGetBytesResponse)
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

func (s *Server) bucketPutBytes2(r io.Reader, w *bufio.Writer, request util.BucketPutBytesRequest) (util.BucketPutBytesResponse, error) {
	uniqueIdentifier := string(request.UniqueIdentifier[:])
	bucketPutBytesResponse := util.BucketPutBytesResponse{
		Header:    util.Header{MessageType: util.BucketPutBytesResponseMessageType, Version: 1},
		ErrorCode: 0,
	}

	bucketPath := path.Join(s.config.BucketPath, uniqueIdentifier)
	fileInfo, err := os.Stat(bucketPath)
	if err != nil {
		log.Printf("error putting bytes: %+v", err)
		bucketPutBytesResponse.ErrorCode = 1
		return bucketPutBytesResponse, err
	}

	if request.NumBytes > fileInfo.Size() {
		bucketPutBytesResponse.ErrorCode = 2
		return bucketPutBytesResponse, err
	}

	// TODO: Always send back a message saying whether or not we accept
	numBytesToRead := request.NumBytes
	log.Printf("bucketName:%s bucketSize: %d", uniqueIdentifier, numBytesToRead)
	util.WriteMessageToWriter(w, bucketPutBytesResponse)

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
