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

const (
	receiveBufferSize = 1024 * 32
)

type ClientConfiguration struct {
	ServerAddrAndPort     string
	SslEnabled            bool
	SslClientCertFilePath string
}

type Client struct {
	config        ClientConfiguration
	receiveBuffer []byte
	conn          net.Conn
}

type LoftClient interface {
	Connect() error
	CreateBucket(int64) (string, error)
	PutFileInBucket(string, string) (uint32, error)
	PutBucketInFile()
}

func newClient(config ClientConfiguration) LoftClient {
	newClient := &Client{config: config}
	newClient.receiveBuffer = make([]byte, receiveBufferSize)
	return newClient
}

func writeMessageToServer(conn net.Conn, message interface{}) error {
	var err error
	serializedMessage, err := SerializeMessage2(message)
	if err != nil {
		return errors.Wrapf(err, "failed to serialize message")
	}
	// TODO: Throw in a BufferedWriter
	_, err = conn.Write([]byte{byte(serializedMessage.Len())})
	if err != nil {
		return errors.Wrapf(err, "failed to write size of message to connection")
	}
	_, err = conn.Write(serializedMessage.Next(serializedMessage.Len()))
	if err != nil {
		return errors.Wrapf(err, "failed to write message to connection")
	}
	return nil
}

func writeBytesToServer(conn net.Conn, byteReader *bufio.Reader) error {
	bufferedWriter := bufio.NewWriter(conn)
	bytesWritten, err := byteReader.WriteTo(bufferedWriter)
	if err != nil {
		return errors.Wrapf(err, "failed writing bytes to server bytes written: %d", bytesWritten)
	}
	return nil
}

func readMessageFromServer(conn net.Conn) ([]byte, error) {
	var err error
	bufferReader := bufio.NewReader(conn)

	messageSizeBuffer := make([]byte, 8)
	_, err = io.ReadFull(bufferReader, messageSizeBuffer)
	if err != nil {
		return nil, errors.Wrapf(err, "error retrieving 8 bytes for message size")
	}
	var messageSize int64
	err = binary.Read(bytes.NewBuffer(messageSizeBuffer), binary.BigEndian, &messageSize)
	if err != nil {
		return nil, errors.Wrapf(err, "error translating message size")
	}

	messageBuffer := make([]byte, messageSize)
	_, err = io.ReadFull(bufferReader, messageBuffer)
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
		c.conn, err = tls.Dial("tcp", c.config.ServerAddrAndPort, tlsConfig)
		if err != nil {
			return errors.Wrapf(err,
				"Client.Connect failed to dial tls enabled server addr:",
				c.config.ServerAddrAndPort)
		}
	} else {
		c.conn, err = net.Dial("tcp", c.config.ServerAddrAndPort)
		if err != nil {
			return errors.Wrapf(err,
				"Client.Connect failed to dial plaintext server addr:",
				c.config.ServerAddrAndPort)
		}
	}
	return nil
}

func (c *Client) CreateBucket(numBytes int64) (string, error) {
	bucketGenerateRequest := BucketGenerateRequest{Header: Header{MessageType: bucketGenerateMessageType, Version: 1}, NumBytesInBucket: numBytes}
	err := writeMessageToServer(c.conn, bucketGenerateRequest)
	if err != nil {
		return "", errors.Wrap(err, "error writing message to server.")
	}

	messageBytes, err := readMessageFromServer(c.conn)
	if err != nil {
		return "", errors.Wrap(err, "error reading message from server.")
	}

	return string(messageBytes), nil
}

func (c *Client) PutFileInBucket(bucketIdentifier string, filePath string) (uint32, error) {
	var bucketIdentifierBytes [bucketNameLength]byte
	copy(bucketIdentifierBytes[:], []byte(bucketIdentifier))

	bucketPutRequest := BucketPutBytesRequest{Header: Header{MessageType: bucketPutBytesMessageType, Version: 1}, UniqueIdentifier: bucketIdentifierBytes}
	err := writeMessageToServer(c.conn, bucketPutRequest)
	if err != nil {
		return 0, errors.Wrap(err, "error writing message to server.")
	}

	f, err := os.Open(filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failure opening file %s", filePath)
	}
	defer f.Close()
	err = writeBytesToServer(c.conn, bufio.NewReader(f))
	if err != nil {
		return 0, errors.Wrapf(err, "error writing bytes to server")
	}

	return 0, nil
}

func (c *Client) PutBucketInFile() {

}
