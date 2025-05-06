package api

//go:generate buf format -w cloud/v1/message.proto
//go:generate buf lint cloud/v1/message.proto
//go:generate buf generate cloud/v1/message.proto
