package shell_executor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
	"unicode"

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

// parseShellArgs splits a command line into arguments, honouring quoting
// rules close to POSIX sh.
//   - Single quotes: literal everything until next '
//   - Double quotes: \ only escapes \" and \\
//   - Outside quotes: \ escapes space, \\, ', "
func parseShellArgs(command string) ([]string, error) {

	// always return a non-nil slice on success
	args := make([]string, 0)
	var current strings.Builder
	inSingle, inDouble := false, false

	runes := []rune(command)
	for i := 0; i < len(runes); i++ {
		r := runes[i]

		// Back-slash processing
		if r == '\\' && !inSingle {
			// If this is the last rune just keep it literal
			if i+1 == len(runes) {
				current.WriteRune(r)
				continue
			}
			next := runes[i+1]

			// Decide if the back-slash should escape the next rune
			shouldEscape := false
			if inDouble {
				// In double-quotes it only escapes " and \
				shouldEscape = next == '"' || next == '\\'
			} else {
				// Outside quotes it escapes space, ', ", \
				shouldEscape = unicode.IsSpace(next) ||
					next == '\'' || next == '"' || next == '\\'
			}

			if shouldEscape {
				current.WriteRune(next)
				i++ // consume the next rune
			} else {
				// Keep the back-slash literally
				current.WriteRune(r)
			}
			continue
		}

		// Quote toggling
		switch r {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
				continue
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
				continue
			}
		}

		// Argument boundary
		if unicode.IsSpace(r) && !inSingle && !inDouble {
			if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
			continue
		}

		current.WriteRune(r)
	}

	// Add the final arg if any
	if current.Len() > 0 {
		args = append(args, current.String())
	}

	if inSingle || inDouble {
		return nil, fmt.Errorf("unclosed quote in command")
	}
	return args, nil
}

// ExecuteShellCommands processes a batch of shell commands
func (e *ShellExecutor) ExecuteShellCommands(ctx context.Context, request *v1.ShellCommandRequest) *v1.ShellCommandResponse {
	e.Logger.Info("Executing shell commands",
		zap.String("request_id", request.RequestId),
		zap.Int("commands_count", len(request.Commands)),
		zap.Int32("timeout_seconds", request.TimeoutSeconds))

	// Set up timeout context
	timeout := 60 * time.Second // default timeout for tool call batch
	if request.TimeoutSeconds > 0 {
		e.Logger.Info("Timeout specified in request, setting timeout", zap.Int32("timeout_seconds", request.TimeoutSeconds))
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

	if strings.TrimSpace(command) == "" {
		result.Success = false
		result.Stderr = "Command cannot be empty"
		result.ExitCode = 1
		return result
	}

	var cmd *exec.Cmd

	// Check if command needs shell interpretation
	needsShell := strings.Contains(command, "|") || // pipes
		strings.Contains(command, ">") || // redirection
		strings.Contains(command, "<") || // input redirection
		strings.Contains(command, "$(") || // command substitution
		strings.Contains(command, "&&") || // logical AND
		strings.Contains(command, "||") || // logical OR
		strings.Contains(command, ";") || // command separator
		strings.Contains(command, "`") // backtick substitution

	if strings.HasPrefix(command, "bash -c") {
		// Use bash -c to interpret operators like pipes, redirection, etc.
		command = strings.TrimPrefix(command, "bash -c")
		command = strings.TrimSpace(command)
		e.Logger.Debug("Executing bash -c command", zap.String("command", command))
		cmd = exec.CommandContext(ctx, "bash", "-c", command)
	} else if strings.HasPrefix(command, "bash") {
		// Use bash -c to interpret operators like pipes, redirection, etc.
		command = strings.TrimPrefix(command, "bash")
		command = strings.TrimSpace(command)
		e.Logger.Debug("Executing bash command", zap.String("command", command))
		cmd = exec.CommandContext(ctx, "bash", "-c", command)
	} else if needsShell {
		// Command has shell operators but doesn't have bash prefix - wrap it automatically
		e.Logger.Debug("Command contains shell operators, wrapping in bash -c", zap.String("command", command))
		cmd = exec.CommandContext(ctx, "bash", "-c", command)
	} else {
		// Parse command into parts using proper shell argument parsing for simple commands
		parts, err := parseShellArgs(command)
		if err != nil {
			result.Success = false
			result.Stderr = fmt.Sprintf("Failed to parse command: %v", err)
			result.ExitCode = 1
			return result
		}

		if len(parts) == 0 {
			result.Success = false
			result.Stderr = "Invalid command format"
			result.ExitCode = 1
			return result
		}

		cmd = exec.CommandContext(ctx, parts[0], parts[1:]...)
	}

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
			zap.Int32("exit_code", result.ExitCode),
			zap.String("stderr", result.Stderr))
		return result
	}

	result.Success = true
	result.Stdout = string(stdout)
	result.ExitCode = 0

	e.Logger.Debug("Shell command completed successfully",
		zap.String("command", command),
		zap.Int("stdout_size", len(stdout)))

	return result
}
