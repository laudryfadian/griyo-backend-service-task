package main

import (
	"context"
	app "github.com/laudryfadian/griyo-backend-service-task/internal"
	"github.com/laudryfadian/griyo-backend-service-task/internal/pb"
	"github.com/laudryfadian/griyo-backend-service-task/internal/platform"
	"log"
)

func main() {
	ctx := context.Background()
	db, e := platform.Db(ctx)
	if e != nil {
		log.Fatal(e)
	}
	defer db.Close()
	w, e := platform.Dial(platform.Env("WORKSPACE_GRPC_ADDR", "workspace:50051"))
	if e != nil {
		log.Fatal(e)
	}
	defer w.Close()
	f, e := platform.Dial(platform.Env("FLEET_GRPC_ADDR", "fleet:50052"))
	if e != nil {
		log.Fatal(e)
	}
	defer f.Close()
	svc, e := app.New(ctx, db, pb.NewWorkspaceServiceClient(w), pb.NewFleetServiceClient(f))
	if e != nil {
		log.Fatal(e)
	}
	server := platform.Server()
	pb.RegisterTaskServiceServer(server, svc)
	log.Fatal(platform.ServeGrpc(server, platform.Env("GRPC_ADDR", ":50053")))
}
