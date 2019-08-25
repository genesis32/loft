package cmd

import (
	"flag"
	"fmt"
	"log"

	"github.com/genesis32/loft/client"
	"github.com/genesis32/loft/server"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
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
		var runtimeConfig configuration

		fmt.Println("server")
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
		var runtimeConfig configuration

		fmt.Println("server")
		viper.SetConfigName("config")
		viper.AddConfigPath(".")

		if err := viper.ReadInConfig(); err != nil {
			log.Fatalf("Error reading config file, %s", err)
		}

		if err := viper.Unmarshal(&runtimeConfig); err != nil {
			log.Fatalf("Unable to decode struct, %s", err)
		}

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
		bucketIdentifier, err := client.CreateBucket(1200000)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("bucket id: %s", bucketIdentifier)
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
		fmt.Println("download")
	},
}

var BucketUploadCmd = &cobra.Command{
	Use: "upload",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("upload")
	},
}

var BucketCmd = &cobra.Command{
	Use: "bucket",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("bucket")
	},
}
