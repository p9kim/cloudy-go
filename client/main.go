package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	fileserve "github.com/p9kim/cloudy-go/proto"
	"google.golang.org/grpc"
)

//type ClientGRPC struct{}

/*
func (cli *ClientGRPC) UploadBlock(ctx context.Context, f string) (*fileserve.UploadStatus, error) {
	file, err := os.Open(f)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	stream, err := cli.client.
}
*/

func main() {
	filename := os.Args[1]
	writing := true

	conn, err := grpc.Dial("localhost:9000", grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}

	defer conn.Close()

	client := fileserve.NewUploadClient(conn)

	stream, err := client.UploadBlock(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	buf := make([]byte, 20)
	for writing {
		n, err := file.Read(buf)
		if err != nil {
			if err == io.EOF {
				writing = false
				err = nil
				continue
			}
			log.Fatal(err)
		}

		err = stream.Send(&fileserve.DataBlock{
			Data: buf[:n],
		})
		if err != nil {
			log.Fatal(err)
		}
	}

	status, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatal(err)
	}

	if status.Status != fileserve.UploadStatus_OK {
		log.Fatal(status.Message)
	}

	fmt.Println(status.Status)

}
