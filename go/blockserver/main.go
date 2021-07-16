package main

import (
	"context"
	"fmt"
	"log"
	"net"

	cloudy "github.com/p9kim/cloudy-go/proto"

	"google.golang.org/grpc"
)

type Server struct{}

var blockMap = make(map[string][]byte)

func (s *Server) SayHello(ctx context.Context, req *cloudy.Message) (*cloudy.Message, error) {
	log.Printf("Message received from client: %s", req.Body)
	res := cloudy.Message{
		Body: "Kenobi~!! You are a BOLD ONE~!!",
	}

	return &res, nil
}

func (s *Server) UploadBlock(ctx context.Context, req *cloudy.DataBlock) (*cloudy.UploadStatus, error) {
	log.Printf("Storing data block: %s", req.Hash)
	blockMap[req.Hash] = req.Data

	res := cloudy.UploadStatus{
		Message: "Data block successfully uploaded",
		Status:  cloudy.UploadStatus_OK,
	}

	return &res, nil
}

func (s *Server) RetrieveBlock(ctx context.Context, req *cloudy.DataBlock) (*cloudy.DataBlock, error) {
	log.Printf("Retrieve data block: %s", req.Hash)

	res := cloudy.DataBlock{
		Hash: req.Hash,
		Data: blockMap[req.Hash],
	}

	return &res, nil
}

func (s *Server) HasBlock(ctx context.Context, req *cloudy.DataBlock) (*cloudy.SimpleAnswer, error) {
	log.Printf("Checking if data block exists: %s", req.Hash)
	exists := false

	if _, ok := blockMap[req.Hash]; ok {
		exists = true
	}

	res := cloudy.SimpleAnswer{
		Answer: exists,
	}

	return &res, nil

}

func main() {
	fmt.Println("Starting gRPC server!!")

	listener, err := net.Listen("tcp", ":8080")

	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	cloudy.RegisterBlockDataServer(grpcServer, &Server{})

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}

}

/*
func (s *ServerGRPC) UploadBlock(stream cloudy.Upload_UploadBlockServer) (err error) {
	for {
		n, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				goto END
			}
			log.Fatal(err)
		}

		fmt.Println(n)
	}

END:

	err = stream.SendAndClose(&cloudy.UploadStatus{
		Message: "File blocks received!!",
		Status:  cloudy.UploadStatus_OK,
	})

	if err != nil {
		log.Fatal(err)
	}

	return

}
*/
