package cmd

import (
	"fmt"
	"log"
	"os"

	"github.com/genesis32/loft/client"
	"github.com/genesis32/loft/server"
	"github.com/genesis32/loft/util"
	"github.com/spf13/cobra"
)

var serverConfig server.ServerConfiguration
var clientConfig client.ClientConfiguration

func init() {

	ServerCmd.Flags().StringVarP(&serverConfig.BucketPath, "bucket-path", "b", "/homedir", "the bucket path")
	ServerCmd.Flags().StringVarP(&serverConfig.SslClientKeyFilePath, "key", "k", "", "the server private key")
	ServerCmd.Flags().StringVarP(&serverConfig.SslClientCertFilePath, "cert", "c", "", "the server certificate to present")
	ServerCmd.Flags().StringVarP(&serverConfig.ListenAddrAndPort, "listen", "l", ":8089", "the port to listen on")

	BucketCmd.PersistentFlags().StringVarP(&clientConfig.ServerAddrAndPort, "server", "s", "localhost:8089", "the server to connect to")
	BucketCmd.PersistentFlags().StringVarP(&clientConfig.SslClientCertFilePath, "cert", "c", "", "the cert cert to verify")

	BucketCreateCmd.Flags().Int64P("size", "n", 1024*1024, "number of bytes in the bucket")

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

		client := client.NewClient(clientConfig)
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

		client := client.NewClient(clientConfig)
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

		client := client.NewClient(clientConfig)
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
