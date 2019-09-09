package client

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

	"github.com/genesis32/loft/util"
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

func NewClient(config ClientConfiguration) LoftClient {
	newClient := &Client{config: config}
	return newClient
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

	messageSizeBuffer := make([]byte, 1)
	_, err = io.ReadFull(reader, messageSizeBuffer)
	if err != nil {
		return nil, errors.Wrapf(err, "error retrieving 8 bytes for message size")
	}
	var messageSize byte
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
	bucketGenerateRequest := util.BucketGenerateRequest{Header: util.Header{MessageType: util.BucketGenerateMessageType, Version: 1}, NumBytesInBucket: numBytes}
	err := util.WriteMessageToWriter(c.bufferedWriter, bucketGenerateRequest)
	if err != nil {
		return "", errors.Wrap(err, "error writing message to server.")
	}

	messageBytes, err := readMessageFromServer(c.bufferedReader)
	if err != nil {
		return "", errors.Wrap(err, "error reading message from server.")
	}
	bucketGenerateResponseMessage, err := util.DeserializeMessage2(bytes.NewBuffer(messageBytes))
	switch v := bucketGenerateResponseMessage.(type) {
	case util.BucketGenerateResponse:
		return string(v.UniqueIdentifier[:]), nil
	}

	return "", nil
}

func (c *Client) PutFileInBucket(bucketIdentifier string, filePath string) (uint32, error) {
	var bucketIdentifierBytes [util.BucketNameLength]byte
	copy(bucketIdentifierBytes[:], []byte(bucketIdentifier))

	f, err := os.Open(filePath)
	if err != nil {
		return 0, errors.Wrapf(err, "failure opening file %s", filePath)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "error getting stats on file")
	}

	bucketPutRequest := util.BucketPutBytesRequest{
		Header:           util.Header{MessageType: util.BucketPutBytesMessageType, Version: 1},
		UniqueIdentifier: bucketIdentifierBytes,
		NumBytes:         fi.Size(),
	}

	err = util.WriteMessageToWriter(c.bufferedWriter, bucketPutRequest)
	if err != nil {
		return 0, errors.Wrap(err, "error writing message to server.")
	}

	messageBytes, err := readMessageFromServer(c.bufferedReader)
	if err != nil {
		return 0, errors.Wrap(err, "error reading message from server.")
	}

	msg, err := util.DeserializeMessage2(bytes.NewBuffer(messageBytes))
	switch v := msg.(type) {
	case util.BucketPutBytesResponse:
		if v.ErrorCode != 0 {
			log.Printf("error cannot write data to bucket error code: %d", v.ErrorCode)
			os.Exit(1)
		}
	}

	err = writeBytesToServer(c.bufferedWriter, bufio.NewReader(f))
	if err != nil {
		return 0, errors.Wrapf(err, "error writing bytes to server")
	}

	return 0, nil
}

func (c *Client) PutBucketInFile(bucketIdentifer string, filePath string) error {
	var bucketIdentifierBytes [util.BucketNameLength]byte
	copy(bucketIdentifierBytes[:], []byte(bucketIdentifer))
	bucketGetRequest := util.BucketGetBytesRequest{Header: util.Header{MessageType: util.BucketGetBytesMessageType, Version: 1}, UniqueIdentifier: bucketIdentifierBytes}
	err := util.WriteMessageToWriter(c.bufferedWriter, bucketGetRequest)
	if err != nil {
		return errors.Wrap(err, "error writing message to server.")
	}

	messageBytes, err := readMessageFromServer(c.bufferedReader)
	msg, err := util.DeserializeMessage2(bytes.NewBuffer(messageBytes))
	switch v := msg.(type) {
	case util.BucketGetBytesResponse:
		log.Printf("Error Code: %d", v.ErrorCode)
		if v.ErrorCode > 0 {
			return errors.New("error code not 0")
		}

		f, err := os.Create(filePath)
		if err != nil {
			return errors.Wrapf(err, "failure opening file %s", filePath)
		}
		defer f.Close()

		buff := make([]byte, 128*1024)
		var totalBytesRead int64
		for totalBytesRead < v.Size {
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
	}

	return nil
}
