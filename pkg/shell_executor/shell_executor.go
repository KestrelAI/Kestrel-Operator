package shell_executor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap"
)

// ShellExecutor handles execution of shell commands
type ShellExecutor struct {
	Logger *zap.Logger
}

// NewShellExecutor creates a new shell command executor
func NewShellExecutor(logger *zap.Logger) *ShellExecutor {
	return &ShellExecutor{
		Logger: logger,
	}
}

// ExecuteShellCommands processes a batch of shell commands
func (e *ShellExecutor) ExecuteShellCommands(ctx context.Context, request *v1.ShellCommandRequest) *v1.ShellCommandResponse {
	e.Logger.Info("Executing shell commands",
		zap.String("request_id", request.RequestId),
		zap.Int("commands_count", len(request.Commands)),
		zap.Int32("timeout_seconds", request.TimeoutSeconds))

	// Set up timeout context
	timeout := 30 * time.Second // default timeout
	if request.TimeoutSeconds > 0 {
		timeout = time.Duration(request.TimeoutSeconds) * time.Second
	}

	ctxWithTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	response := &v1.ShellCommandResponse{
		RequestId: request.RequestId,
		Results:   make([]*v1.ShellCommandResult, 0, len(request.Commands)),
	}

	// Execute each command
	for _, command := range request.Commands {
		result := e.executeCommand(ctxWithTimeout, command)
		response.Results = append(response.Results, result)
	}

	return response
}

// executeCommand performs a single shell command execution
func (e *ShellExecutor) executeCommand(ctx context.Context, command string) *v1.ShellCommandResult {
	e.Logger.Debug("Executing shell command", zap.String("command", command))

	result := &v1.ShellCommandResult{
		Command: command,
	}

	// Validate command is not empty
	if strings.TrimSpace(command) == "" {
		result.Success = false
		result.Stderr = "Command cannot be empty"
		result.ExitCode = 1
		return result
	}

	// Split command into parts for exec.CommandContext
	parts := strings.Fields(command)
	if len(parts) == 0 {
		result.Success = false
		result.Stderr = "Invalid command format"
		result.ExitCode = 1
		return result
	}

	// Create the command with context
	cmd := exec.CommandContext(ctx, parts[0], parts[1:]...)

	// Execute the command and capture output
	stdout, err := cmd.Output()
	if err != nil {
		result.Success = false
		result.Stdout = string(stdout)
		
		// Handle different types of errors
		if exitError, ok := err.(*exec.ExitError); ok {
			result.Stderr = string(exitError.Stderr)
			result.ExitCode = int32(exitError.ExitCode())
		} else {
			result.Stderr = fmt.Sprintf("Command execution failed: %v", err)
			result.ExitCode = 1
		}

		e.Logger.Error("Shell command failed",
			zap.String("command", command),
			zap.Error(err),
			zap.Int32("exit_code", result.ExitCode))
		return result
	}

	// Success
	result.Success = true
	result.Stdout = string(stdout)
	result.ExitCode = 0

	e.Logger.Debug("Shell command completed successfully",
		zap.String("command", command),
		zap.Int("stdout_size", len(stdout)))

	return result
}