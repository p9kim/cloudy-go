package main

import (
	"fmt"
	"io"
	"log"
	"net"

	fileserve "github.com/p9kim/cloudy-go/proto"

	"google.golang.org/grpc"
)

type ServerGRPC struct{}

func (s *ServerGRPC) UploadBlock(stream fileserve.Upload_UploadBlockServer) (err error) {
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

	err = stream.SendAndClose(&fileserve.UploadStatus{
		Message: "File blocks received!!",
		Status:  fileserve.UploadStatus_OK,
	})

	if err != nil {
		log.Fatal(err)
	}

	return

}

func main() {
	fmt.Println("Starting gRPC server!!")

	listener, err := net.Listen("tcp", ":9000")

	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	fileserve.RegisterUploadServer(grpcServer, &ServerGRPC{})

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}

}
