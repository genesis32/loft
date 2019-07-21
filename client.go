package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"

	"github.com/pkg/errors"
)

type ClientConfiguration struct {
	ServerAddrAndPort     string
	SslEnabled            bool
	SslClientCertFilePath string
}

type Client struct {
	config         ClientConfiguration
	bufferedReader *bufio.Reader
	bufferedWriter *bufio.Writer
	theConn        net.Conn
}

type LoftClient interface {
	Connect() error
	CreateBucket(int64) (string, error)
	PutFileInBucket(string, string) (uint32, error)
	PutBucketInFile(string, string) error
}

func newClient(config ClientConfiguration) LoftClient {
	newClient := &Client{config: config}
	return newClient
}

func writeMessageToServer(w *bufio.Writer, message interface{}) error {
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

func writeBytesToServer(w *bufio.Writer, byteReader *bufio.Reader) error {
	bytesWritten, err := byteReader.WriteTo(w)
	if err != nil {
		return errors.Wrapf(err, "failed. wrote %d bytes to server.", bytesWritten)
	}
	log.Printf("Number of bytes written: %d", bytesWritten)
	return nil
}

func readMessageFromServer(reader *bufio.Reader) ([]byte, error) {
	var err error

	messageSizeBuffer := make([]byte, 8)
	_, err = io.ReadFull(reader, messageSizeBuffer)
	if err != nil {
		return nil, errors.Wrapf(err, "error retrieving 8 bytes for message size")
	}
	var messageSize int64
	err = binary.Read(bytes.NewBuffer(messageSizeBuffer), binary.BigEndian, &messageSize)
	if err != nil {
		return nil, errors.Wrapf(err, "error translating message size")
	}

	log.Printf("message size: %d bytes", messageSize)
	messageBuffer := make([]byte, messageSize)
	_, err = io.ReadFull(reader, messageBuffer)
	if err != nil {
		return nil, errors.Wrapf(err, "error reading message")
	}

	return messageBuffer, nil
}

func (c *Client) Connect() error {
	var err error
	if c.config.SslEnabled {
		rootCert, err := ioutil.ReadFile(c.config.SslClientCertFilePath)
		if err != nil {
			return errors.Wrap(err, "Client.Connect failed to ReadFile")
		}
		roots := x509.NewCertPool()
		ok := roots.AppendCertsFromPEM([]byte(rootCert))
		if !ok {
			log.Fatal("failed to parse root certificate")
		}
		tlsConfig := &tls.Config{RootCAs: roots}
		c.theConn, err = tls.Dial("tcp", c.config.ServerAddrAndPort, tlsConfig)
		if err != nil {
			return errors.Wrapf(err,
				"Client.Connect failed to dial tls enabled server addr:",
				c.config.ServerAddrAndPort)
		}
	} else {
		c.theConn, err = net.Dial("tcp", c.config.ServerAddrAndPort)
		if err != nil {
			return errors.Wrapf(err,
				"Client.Connect failed to dial plaintext server addr:",
				c.config.ServerAddrAndPort)
		}
	}
	c.bufferedReader = bufio.NewReader(c.theConn)
	c.bufferedWriter = bufio.NewWriter(c.theConn)
	return nil
}

func (c *Client) CreateBucket(numBytes int64) (string, error) {
	bucketGenerateRequest := BucketGenerateRequest{Header: Header{MessageType: bucketGenerateMessageType, Version: 1}, NumBytesInBucket: numBytes}
	err := writeMessageToServer(c.bufferedWriter, bucketGenerateRequest)
	if err != nil {
		return "", errors.Wrap(err, "error writing message to server.")
	}

	messageBytes, err := readMessageFromServer(c.bufferedReader)
	if err != nil {
		return "", errors.Wrap(err, "error reading message from server.")
	}

	return string(messageBytes), nil
}

func (c *Client) PutFileInBucket(bucketIdentifier string, filePath string) (uint32, error) {
	var bucketIdentifierBytes [bucketNameLength]byte
	copy(bucketIdentifierBytes[:], []byte(bucketIdentifier))

	bucketPutRequest := BucketPutBytesRequest{Header: Header{MessageType: bucketPutBytesMessageType, Version: 1}, UniqueIdentifier: bucketIdentifierBytes}
	err := writeMessageToServer(c.bufferedWriter, bucketPutRequest)
	if err != nil {
		return 0, errors.Wrap(err, "error writing message to server.")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failure opening file %s", filePath)
	}
	defer f.Close()
	err = writeBytesToServer(c.bufferedWriter, bufio.NewReader(f))
	if err != nil {
		return 0, errors.Wrapf(err, "error writing bytes to server")
	}

	buffMessageLength := make([]byte, 8)
	_, err = io.ReadFull(c.bufferedReader, buffMessageLength)
	var size int64
	binary.Read(bytes.NewBuffer(buffMessageLength), binary.BigEndian, &size)
	if err != nil {
		log.Fatal(err)
	}

	return 0, nil
}

func (c *Client) PutBucketInFile(bucketIdentifer string, filePath string) error {
	var bucketIdentifierBytes [bucketNameLength]byte
	copy(bucketIdentifierBytes[:], []byte(bucketIdentifer))
	bucketGetRequest := BucketGetBytesRequest{Header: Header{MessageType: bucketGetBytesMessageType, Version: 1}, UniqueIdentifier: bucketIdentifierBytes}
	err := writeMessageToServer(c.bufferedWriter, bucketGetRequest)
	if err != nil {
		return errors.Wrap(err, "error writing message to server.")
	}

	bufferMessage, err := readMessageFromServer(c.bufferedReader)
	var size int64
	binary.Read(bytes.NewBuffer(bufferMessage), binary.BigEndian, &size)
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Create(filePath)
	if err != nil {
		return errors.Wrapf(err, "failure opening file %s", filePath)
	}
	defer f.Close()

	buff := make([]byte, 128*1024)
	var totalBytesRead int64
	for totalBytesRead < size {
		var err error
		bytesRead, err := c.bufferedReader.Read(buff)
		if bytesRead == 0 && err == io.EOF {
			break
		}
		n, err := f.Write(buff[:bytesRead])
		if err != nil {
			return errors.Wrapf(err, "failed to write file. wrote %d bytes", n)
		}
		totalBytesRead += int64(bytesRead)
	}

	return nil
}
