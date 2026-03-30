
package main

import (
	"github.com/ZeroVerify/bitstring-updater-lambda/internal/handler"
	"github.com/aws/aws-lambda-go/lambda"
)

func main() {
	lambda.Start(handler.Handle)
}
