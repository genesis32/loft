package main

import (
	"flag"
	"log"

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
		serverConfig := ServerConfiguration{
			ListenAddrAndPort:     runtimeConfig.ListenPort,
			SslEnabled:            runtimeConfig.SslEnabled,
			SslClientCertFilePath: runtimeConfig.ServerCertPath,
			SslClientKeyFilePath:  runtimeConfig.ServerKeyPath,
		}
		//		startServer()
		theServer := newServer(serverConfig)
		err := theServer.StartAndServe()
		if err != nil {
			log.Fatal(err)
		}
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
		_, err = client.PutFileInBucket(bucketIdentifier, "/Users/dmassey/loft-test-file")
		if err != nil {
			log.Fatal(err)
		}
		err = client.PutBucketInFile(bucketIdentifier, "/Users/dmassey/loft-test-file-recv")
		if err != nil {
			log.Fatal(err)
		}
	}
}
