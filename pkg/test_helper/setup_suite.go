package testhelper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"k8s.io/client-go/kubernetes"
)

type ControllerTestSuite struct {
	suite.Suite
	ctx       context.Context
	clientset *kubernetes.Clientset
	logger    *zap.Logger
}

func TestGenerateTestSuite(t *testing.T) {
	suite.Run(t, new(ControllerTestSuite))
}

func (s *ControllerTestSuite) GetContext() context.Context {
	return s.ctx
}

func (s *ControllerTestSuite) GetClientset() *kubernetes.Clientset {
	return s.clientset
}
