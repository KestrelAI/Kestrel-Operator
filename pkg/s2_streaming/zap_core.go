package s2_streaming

import (
	"encoding/json"

	"go.uber.org/zap/zapcore"
)

// S2Core is a zapcore.Core that streams logs to S2
type S2Core struct {
	logManager *LogManager
	encoder    zapcore.Encoder
	enabler    zapcore.LevelEnabler
	fields     []zapcore.Field
}

// NewS2Core creates a new S2 zap core
func NewS2Core(logManager *LogManager, enabler zapcore.LevelEnabler) *S2Core {
	// Use JSON encoder for structured logs
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.EpochMillisTimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	return &S2Core{
		logManager: logManager,
		encoder:    zapcore.NewJSONEncoder(encoderConfig),
		enabler:    enabler,
		fields:     []zapcore.Field{},
	}
}

// Enabled implements zapcore.Core
func (c *S2Core) Enabled(level zapcore.Level) bool {
	return c.logManager != nil && c.logManager.IsEnabled() && c.enabler.Enabled(level)
}

// With implements zapcore.Core
func (c *S2Core) With(fields []zapcore.Field) zapcore.Core {
	clone := c.clone()
	clone.fields = append(clone.fields, fields...)
	return clone
}

// Check implements zapcore.Core
func (c *S2Core) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

// Write implements zapcore.Core
func (c *S2Core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	if !c.Enabled(entry.Level) {
		return nil
	}

	// Encode the entry with fields
	buffer, err := c.encoder.EncodeEntry(entry, append(c.fields, fields...))
	if err != nil {
		return err
	}
	defer buffer.Free()

	// Parse the encoded JSON to extract fields
	var logData map[string]interface{}
	if err := json.Unmarshal(buffer.Bytes(), &logData); err != nil {
		return err
	}

	// Convert zap level to S2 log level
	var level LogLevel
	switch entry.Level {
	case zapcore.DebugLevel:
		level = LogLevelDebug
	case zapcore.InfoLevel:
		level = LogLevelInfo
	case zapcore.WarnLevel:
		level = LogLevelWarn
	case zapcore.ErrorLevel, zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		level = LogLevelError
	default:
		level = LogLevelInfo
	}

	// Extract logger name (component)
	component := entry.LoggerName
	if component == "" {
		component = "operator"
	}

	// Remove standard fields from logData to put in fields map
	message := entry.Message
	delete(logData, "message")
	delete(logData, "level")
	delete(logData, "timestamp")
	delete(logData, "logger")

	// Keep caller in fields if present
	caller, _ := logData["caller"].(string)
	if caller != "" {
		logData["caller"] = caller
	}

	// Create S2 log entry
	logEntry := LogEntry{
		Timestamp: entry.Time.UnixMilli(),
		Level:     level,
		Message:   message,
		Component: component,
		Fields:    logData,
	}

	// Stream to S2 (non-blocking)
	return c.logManager.StreamCustomLog(logEntry)
}

// Sync implements zapcore.Core
func (c *S2Core) Sync() error {
	// S2 streaming handles its own syncing
	return nil
}

// clone creates a shallow copy of the core
func (c *S2Core) clone() *S2Core {
	return &S2Core{
		logManager: c.logManager,
		encoder:    c.encoder.Clone(),
		enabler:    c.enabler,
		fields:     c.fields[:len(c.fields):len(c.fields)], // Copy slice
	}
}

// WrapLogger wraps an existing zap core to also stream logs to S2
func WrapLogger(core zapcore.Core, logManager *LogManager, minLevel zapcore.Level) zapcore.Core {
	if logManager == nil || !logManager.IsEnabled() {
		return core
	}

	s2Core := NewS2Core(logManager, minLevel)
	return zapcore.NewTee(core, s2Core)
}
