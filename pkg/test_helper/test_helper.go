package testhelper

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"runtime"
)

// setupTestCluster creates a new KIND cluster for testing.
func SetupTestCluster() error {
	var stdout, stderr bytes.Buffer

	// Get the directory of the current file to locate kind-config.yaml
	_, currentFile, _, _ := runtime.Caller(0)
	testHelperDir := filepath.Dir(currentFile)
	configPath := filepath.Join(testHelperDir, "kind-config.yaml")

	cmd := exec.Command("kind", "create", "cluster", "--name", "my-test-cluster", "--config", configPath)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to create test cluster: %w\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}

	return nil
}

// tearDownTestCluster destroys the KIND test cluster.
func TearDownTestCluster() error {
	var stdout, stderr bytes.Buffer

	cmd := exec.Command("kind", "delete", "cluster", "--name", "my-test-cluster")
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to delete test cluster: %w\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}

	return nil
}
