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
		logrus.Infof("[%s][role-service] err: %v", s.e.Info().role.String(), err)
	}

	logrus.Debugf("[%s][role-service] <-- recv [Obtain] from [%s], send Resp back",
		s.e.Info().role.String(), cip)

	return &pb.ObtainRsp{
		Code: pb.EnumCode_Success,
		Role: pb.EnumRole(s.e.Info().role),
		Msg:  "this is obtain response from grpc server",
	}, nil
}

func (s *roleService) Abdicate(ctx context.Context, r *pb.AbdicateReq) (*pb.AbdicateRsp, error) {
	cip, err := getClietAddr(ctx)
	if err != nil {
		logrus.Infof("[%s][role-service] err: %v", s.e.Info().role.String(), err)
	}

	logrus.Infof("[%s][role-service] <-- recv [Abdicate] from [%s]",
		s.e.Info().role.String(), cip)

	if s.e.Info().role != RoleLeader {
		logrus.Warnf("[%s] abdicate failed, reason: only Leader can abdicate",
			s.e.Info().role.String())
	} else {
		s.e.Abdicate()
	}

	logrus.Infof("[%s][role-service] --> send [Abdicate] Resp back",
		s.e.Info().role.String())

	// FIXME: 是否应该基于 role 来判定是否执行 abdicate 和应答内容
	return &pb.AbdicateRsp{
		Code: pb.EnumCode_Success,
		Msg:  pb.EnumCode_name[int32(pb.EnumCode_Success)],
	}, nil
}

func (s *roleService) Promote(ctx context.Context, r *pb.PromoteReq) (*pb.PromoteRsp, error) {
	cip, err := getClietAddr(ctx)
	if err != nil {
		logrus.Infof("[%s][role-service] err: %v", s.e.Info().role.String(), err)
	}

	logrus.Infof("[%s][role-service] <-- recv [Promote] from [%s]",
		s.e.Info().role.String(), cip)

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
		logrus.Warnf("[role-service] neither tcpHost nor unixHost is set")
		return errors.New("host not set")
	}

	// TODO: both tcp and unix sock should support
	lis, err := net.Listen("tcp", s.tcpHost)
	if err != nil {
		return err
	}

	logrus.Infof("[role-service] launch role service at [%s]", s.tcpHost)

	return server.Serve(lis)
}

func (s *roleService) Stop() error {
	// TODO:
	logrus.Info("[grcp-role-service] not implement yet")
	return nil
}
