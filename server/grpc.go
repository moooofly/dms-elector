package server

import (
	"context"
	"errors"
	"net"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"

	pb "github.com/moooofly/dms-elector/proto"
)

type roleService struct {
	tcpHost  string
	unixHost string

	e Elector
}

func (s *roleService) Obtain(ctx context.Context, r *pb.ObtainReq) (*pb.ObtainRsp, error) {
	cip, err := getClietAddr(ctx)
	if err != nil {
		logrus.Infof("[grpc-role-service] err: %v", err)
	}

	logrus.Infof("[grpc-role-service] <-- recv [Obtain] from [%s]", cip)

	return &pb.ObtainRsp{
		Code: pb.EnumCode_Success,
		Role: pb.EnumRole(s.e.Info().role),
		Msg:  "this is obtain response from grpc server",
	}, nil
}

func (s *roleService) Abdicate(ctx context.Context, r *pb.AbdicateReq) (*pb.AbdicateRsp, error) {
	logrus.Infof("[grpc-role-service] <-- recv [Abdicate] from [%s]")

	return &pb.AbdicateRsp{
		Code: pb.EnumCode_Failure,
		//Msg:  pb.EnumCode_name[int32(pb.EnumCode_Success)],
		Msg: "this is abdicate response from grpc server, reject",
	}, nil
}

func (s *roleService) Promote(ctx context.Context, r *pb.PromoteReq) (*pb.PromoteRsp, error) {
	logrus.Infof("[grpc-role-service] <-- recv [Promote] from [%s]")

	return &pb.PromoteRsp{
		Code: pb.EnumCode_Success,
		//Msg:  pb.EnumCode_name[int32(pb.EnumCode_Success)],
		Msg: "this is promote response from grpc server, accept",
	}, nil
}

func newRoleService(t string, u string, e Elector) *roleService {
	return &roleService{tcpHost: t, unixHost: u, e: e}
}

func (s *roleService) Start() error {
	server := grpc.NewServer()
	pb.RegisterRoleServiceServer(server, s)

	if s.tcpHost == "" && s.unixHost == "" {
		logrus.Warnf("[single-point] neither tcpHost nor unixHost is set")
		return errors.New("host not set")
	}

	// TODO: both tcp and unix sock should support
	lis, err := net.Listen("tcp", s.tcpHost)
	if err != nil {
		return err
	}

	return server.Serve(lis)
}

func (s *roleService) Stop() error {
	// TODO:
	logrus.Info("[grcp-role-service] not implement yet")
	return nil
}
