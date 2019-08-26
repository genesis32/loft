package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/genesis32/loft/client"
	"github.com/genesis32/loft/server"
	"github.com/genesis32/loft/util"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

type configuration struct {
	SslEnabled     bool
	ServerKeyPath  string
	ServerCertPath string
	BucketPath     string
	ListenPort     string
}

func init() {

	BucketCreateCmd.Flags().Int64P("size", "s", 1024*1024, "size of bucket")

	BucketDownloadCmd.Flags().StringP("bucket-name", "i", "", "bucket name")
	BucketDownloadCmd.Flags().StringP("output-file", "o", "", "output file")

	BucketUploadCmd.Flags().StringP("input-file", "i", "", "filename")
	BucketUploadCmd.Flags().StringP("bucket-name", "o", "", "bucket name")

	RootCmd.PersistentFlags().BoolVarP(&util.Verbose, "verbose", "v", false, "verbose output")

	BucketCmd.AddCommand(BucketCreateCmd)
	BucketCmd.AddCommand(BucketUploadCmd)
	BucketCmd.AddCommand(BucketDownloadCmd)
	BucketCmd.AddCommand(BucketDeleteCmd)

	RootCmd.AddCommand(BucketCmd)
	RootCmd.AddCommand(ServerCmd)
	RootCmd.AddCommand(VersionCmd)
	RootCmd.AddCommand(SetCmd)
}

func loadConfiguration() *configuration {
	var runtimeConfig configuration

	util.VPrintfOut("Loading configuration\n")

	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	if err := viper.Unmarshal(&runtimeConfig); err != nil {
		log.Fatalf("Unable to decode struct, %s", err)
	}

	return &runtimeConfig
}

var RootCmd = &cobra.Command{
	Use: "loft",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("loft")
	},
}

var VersionCmd = &cobra.Command{
	Use: "version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("0.001")
	},
}

var SetCmd = &cobra.Command{
	Use: "set",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("set the server comment")
	},
}

var ServerCmd = &cobra.Command{
	Use: "server",
	Run: func(cmd *cobra.Command, args []string) {

		runtimeConfig := loadConfiguration()

		serverConfig := server.ServerConfiguration{
			ListenAddrAndPort:     runtimeConfig.ListenPort,
			SslEnabled:            runtimeConfig.SslEnabled,
			SslClientCertFilePath: runtimeConfig.ServerCertPath,
			SslClientKeyFilePath:  runtimeConfig.ServerKeyPath,
			BucketPath:            runtimeConfig.BucketPath,
		}
		theServer := server.NewServer(serverConfig)
		err := theServer.StartAndServe()
		if err != nil {
			log.Fatal(err)
		}
	},
}

var BucketCreateCmd = &cobra.Command{
	Use: "create",
	Run: func(cmd *cobra.Command, args []string) {

		bucketSize, _ := cmd.Flags().GetInt64("size")
		if bucketSize <= 0 {
			fmt.Fprintf(os.Stderr, "required bucket size > 0 got size:%d\n", bucketSize)
			os.Exit(1)
		}

		runtimeConfig := loadConfiguration()

		cl := client.ClientConfiguration{
			ServerAddrAndPort:     "localhost:8089",
			SslEnabled:            runtimeConfig.SslEnabled,
			SslClientCertFilePath: runtimeConfig.ServerCertPath,
		}
		client := client.NewClient(cl)
		err := client.Connect()
		if err != nil {
			log.Fatal(err)
		}
		bucketIdentifier, err := client.CreateBucket(bucketSize)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("created bucket:%s\n", bucketIdentifier)

		/*
			log.Printf("bucket id: %s", bucketIdentifier)
			_, err = client.PutFileInBucket(bucketIdentifier, "/Users/dmassey/loft-test-file")
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("get bytes from bucket")
			err = client.PutBucketInFile(bucketIdentifier, "/Users/dmassey/loft-test-file-recv")
			if err != nil {
				log.Fatal(err)
			}
		*/
	},
}

var BucketDeleteCmd = &cobra.Command{
	Use: "delete",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("delete TODO")
	},
}

var BucketDownloadCmd = &cobra.Command{
	Use: "download",
	Run: func(cmd *cobra.Command, args []string) {
		bucketName, _ := cmd.Flags().GetString("bucket-name")
		if bucketName == "" {
			log.Fatalf("bucket-name is required")
		}

		outputFile, _ := cmd.Flags().GetString("output-file")
		if outputFile == "" {
			log.Fatalf("output-file is required")
		}

		runtimeConfig := loadConfiguration()

		cl := client.ClientConfiguration{
			ServerAddrAndPort:     "localhost:8089",
			SslEnabled:            runtimeConfig.SslEnabled,
			SslClientCertFilePath: runtimeConfig.ServerCertPath,
		}
		client := client.NewClient(cl)
		err := client.Connect()

		err = client.PutBucketInFile(bucketName, outputFile)
		if err != nil {
			log.Fatal(err)
		}
	},
}

var BucketUploadCmd = &cobra.Command{
	Use: "upload",
	Run: func(cmd *cobra.Command, args []string) {
		bucketName, _ := cmd.Flags().GetString("bucket-name")
		if bucketName == "" {
			log.Fatalf("bucket-name is required")
		}

		inputFile, _ := cmd.Flags().GetString("input-file")
		if inputFile == "" {
			log.Fatalf("input-file is required")
		}

		runtimeConfig := loadConfiguration()

		cl := client.ClientConfiguration{
			ServerAddrAndPort:     "localhost:8089",
			SslEnabled:            runtimeConfig.SslEnabled,
			SslClientCertFilePath: runtimeConfig.ServerCertPath,
		}
		client := client.NewClient(cl)
		err := client.Connect()

		_, err = client.PutFileInBucket(bucketName, inputFile)
		if err != nil {
			log.Fatal(err)
		}
	},
}

var BucketCmd = &cobra.Command{
	Use: "bucket",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("bucket")
	},
}
