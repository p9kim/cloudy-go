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

var blockStub cloudy.BlockDataClient
var metaMap = make(map[string]cloudy.FileInfo)

func newBlockServer(ip string, port string) error {
	conn, err := grpc.Dial(ip+":"+port, grpc.WithInsecure())
	if err != nil {
		return err
	}

	blockStub = cloudy.NewBlockDataClient(conn)

	return nil
}

func (s *Server) SayHello(ctx context.Context, req *cloudy.Message) (*cloudy.Message, error) {
	log.Printf("Message received from client: %s", req.Body)
	res := cloudy.Message{
		Body: "Kenobi~!! You are a BOLD ONE~!!",
	}

	return &res, nil
}

func (s *Server) ModifyFile(ctx context.Context, req *cloudy.FileInfo) (*cloudy.WriteResult, error) {
	missing := false
	var res cloudy.WriteResult
	if _, ok := metaMap[req.Filename]; !ok || req.Version == 0 {
		metaMap[req.Filename] = *req
		missingBlocks := []string{}
		for _, hashStr := range req.Blocklist {
			if hashStr == "0" {
				continue
			}
			dataBlock := cloudy.DataBlock{}
			dataBlock.Hash = hashStr
			exists, _ := blockStub.HasBlock(context.Background(), &dataBlock)
			if !exists.Answer {
				missingBlocks = append(missingBlocks, hashStr)
				missing = true
			}
		}

		if missing {
			res = cloudy.WriteResult{
				Result:         cloudy.WriteResult_MISSING_BLOCKS,
				CurrentVersion: 1,
				MissingBlocks:  missingBlocks,
			}
		} else {
			res = cloudy.WriteResult{
				Result:         cloudy.WriteResult_MISSING_BLOCKS,
				CurrentVersion: metaMap[req.Filename].Version + 1,
				MissingBlocks:  missingBlocks,
			}
		}
	}
	return &res, nil
}

func (s *Server) ReadFile(ctx context.Context, req *cloudy.FileInfo) (*cloudy.FileInfo, error) {

	return nil, nil
}

func main() {
	fmt.Println("Starting gRPC server!!")

	blockServerErr := newBlockServer("localhost", "8080")
	if blockServerErr != nil {
		log.Fatal(blockServerErr)
	}

	listener, err := net.Listen("tcp", ":9000")

	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	cloudy.RegisterMetaDataServer(grpcServer, &Server{})

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatal(err)
	}

}
