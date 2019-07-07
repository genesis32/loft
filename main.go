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
	"hash/crc32"
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
	ErrorCode        int32
	UniqueIdentifier string
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

func bucketPutBytes(r io.Reader, request BucketPutBytesRequest) (BucketPutBytesResponse, error) {
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

func bucketGetBytes(w io.Writer, request BucketGetBytesRequest) (BucketGetBytesResponse, error) {
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
			bucketGenerateRequest := BucketGenerateRequest{Header: Header{MessageType: bucketGenerateMessageType, Version: 1}, NumBytesInBucket: 1024}
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

		// TODO: Fix that bytes sent must be === to size of bucket
		var writtenCrc32 uint32
		{
			bucketPutRequest := BucketPutBytesRequest{Header: Header{MessageType: bucketPutBytesMessageType, Version: 1}, UniqueIdentifier: uniqIdentifier}
			connectionByteBuffer, err := serializeMessage(bucketPutRequest)
			if err != nil {
				log.Fatal(err)
			}
			conn.Write([]byte{byte(connectionByteBuffer.Len())})
			_, err = conn.Write(connectionByteBuffer.Next(connectionByteBuffer.Len()))

			bytesToWrite := 1024
			b := make([]byte, bytesToWrite)
			rand.Read(b[:bytesToWrite])
			offset := 0
			for offset < bytesToWrite {
				bytesWritten, _ := conn.Write(b[offset:bytesToWrite])
				log.Printf("wrote %d bytes to bucket", bytesWritten)
				offset += bytesWritten
			}
			writtenCrc32 = crc32.ChecksumIEEE(b)

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

		var readCrc32 uint32
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

			buff := make([]byte, 32*1024)
			byteBufferRead := make([]byte, 0)
			var totalBytesRead int64
			for totalBytesRead < size {
				var err error
				bytesRead, err := bufferReader.Read(buff)
				if bytesRead == 0 && err == io.EOF {
					break
				}
				byteBufferRead = append(byteBufferRead, buff[:bytesRead]...)
				totalBytesRead += int64(bytesRead)
			}
			fmt.Printf("read %d bytes\n", len(byteBufferRead))

			readCrc32 = crc32.ChecksumIEEE(byteBufferRead)
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Printf("written crc32: %d read crc32: %d\n", writtenCrc32, readCrc32)
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
			bucketPutBytesResponse, err := bucketPutBytes(bufferReader, v)
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
			binary.Write(buf, binary.BigEndian, int64(bucketPutBytesResponse.ErrorCode))
			c.Write(buf.Bytes())
		case BucketGetBytesRequest:
			log.Printf("BucketGetBytesRequest: %+v", message)
			bufferWriter := bufio.NewWriter(c)
			_, err := bucketGetBytes(bufferWriter, v)
			if err != nil {
				log.Fatal(err)
			}
			bufferWriter.Flush()
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
		//		startClient(dst)
		cl := ClientConfiguration{
			ServerAddrAndPort:     dst,
			SslEnabled:            runtimeConfig.SslEnabled,
			SslClientCertFilePath: runtimeConfig.ServerCertPath,
		}
		client := newClient(cl)
		err := client.Connect()
		if err != nil {
			log.Fatal(err)
		}
		bucketIdentifier, err := client.CreateBucket(16384)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("bucket id: %s", bucketIdentifier)
		_, err = client.PutFileInBucket(bucketIdentifier, "/home/ddm/loft-test-file")
		if err != nil {
			log.Fatal(err)
		}
		err = client.PutBucketInFile(bucketIdentifier, "/home/ddm/loft-test-file-recv")
		if err != nil {
			log.Fatal(err)
		}
	}
}
