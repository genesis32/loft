package main

import (
	"fmt"
	"os"

	"github.com/genesis32/loft/cmd"
)

/*
var runtimeConfig configuration
openssl ecparam -genkey -name prime256v1 -out server.key
openssl req -new -x509 -key server.key -out server.pem -days 3650
*/

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
