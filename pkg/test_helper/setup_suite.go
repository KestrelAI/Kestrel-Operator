package testhelper

import (
	"context"
	"operator/pkg/k8s_helper"
	"os/exec"
	"strings"
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

func (suite *ControllerTestSuite) SetupSuite() {
	suite.ctx = context.Background()
	var err error
	err = SetupTestCluster()
	if err != nil {
		suite.T().Fatal("Failed to set up test cluster " + err.Error())
	}
	// Create a new clientset
	suite.clientset, err = k8s_helper.NewClientSet()
	if err != nil {
		suite.T().Fatal("Failed to get client set " + err.Error())
	}

}

func (suite *ControllerTestSuite) TearDownSuite() {
	err := TearDownTestCluster()
	if err != nil {
		suite.T().Log("Failed to delete test cluster on first attempt: " + err.Error())

		// Retry deletion
		err = TearDownTestCluster()
		if err != nil {
			suite.T().Fatal("Failed to delete test cluster after retry: " + err.Error())
		}
	}

	// Verify cluster deletion
	cmd := exec.Command("kind", "get", "clusters")
	output, err := cmd.Output()
	if err != nil {
		suite.T().Fatal("Failed to verify cluster deletion: " + err.Error())
	}

	if strings.Contains(string(output), "my-test-cluster") {
		suite.T().Fatal("Cluster 'my-test-cluster' still exists after deletion")
	}
}
