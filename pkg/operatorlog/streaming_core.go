package operatorlog

import (
	"encoding/json"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	v1 "operator/api/gen/cloud/v1"

	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// StreamingCore is a zapcore.Core that intercepts log entries and sends them
// as protobuf LogEntry messages to a channel for streaming to the server.
// It is designed to be used as part of a zapcore.NewTee alongside the standard
// stderr core, so logs are both printed locally and streamed remotely.
type StreamingCore struct {
	fields      []zapcore.Field
	level       zapcore.LevelEnabler
	entryCh     chan<- *v1.LogEntry
	enabled     *atomic.Bool
	lineCounter *atomic.Int64
}

// NewStreamingCore creates a new StreamingCore that sends log entries to entryCh.
// The enabled flag controls whether entries are actually sent (allows disabling at runtime).
func NewStreamingCore(level zapcore.LevelEnabler, entryCh chan<- *v1.LogEntry, enabled *atomic.Bool) *StreamingCore {
	return &StreamingCore{
		level:       level,
		entryCh:     entryCh,
		enabled:     enabled,
		lineCounter: &atomic.Int64{},
	}
}

// Enabled returns true if the given log level is enabled.
func (c *StreamingCore) Enabled(lvl zapcore.Level) bool {
	return c.level.Enabled(lvl)
}

// With returns a new StreamingCore with the given fields appended.
// The new core shares the same channel, enabled flag, and line counter.
func (c *StreamingCore) With(fields []zapcore.Field) zapcore.Core {
	newFields := make([]zapcore.Field, len(c.fields)+len(fields))
	copy(newFields, c.fields)
	copy(newFields[len(c.fields):], fields)
	return &StreamingCore{
		fields:      newFields,
		level:       c.level,
		entryCh:     c.entryCh,
		enabled:     c.enabled,
		lineCounter: c.lineCounter,
	}
}

// Check adds this core to the CheckedEntry if the level is enabled.
func (c *StreamingCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(ent.Level) {
		ce = ce.AddCore(ent, c)
	}
	return ce
}

// Write converts the zap entry to a protobuf LogEntry and sends it to the channel.
// This method NEVER blocks — if the channel is full, the entry is silently dropped.
// This method must NEVER call any zap logging methods to avoid infinite recursion.
func (c *StreamingCore) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	if !c.enabled.Load() {
		return nil
	}

	allFields := make([]zapcore.Field, 0, len(c.fields)+len(fields))
	allFields = append(allFields, c.fields...)
	allFields = append(allFields, fields...)

	entry := c.convertToLogEntry(ent, allFields)

	// Non-blocking send — drop under backpressure
	select {
	case c.entryCh <- entry:
	default:
	}

	return nil
}

// Sync is a no-op for the streaming core.
func (c *StreamingCore) Sync() error {
	return nil
}

// convertToLogEntry converts a zap entry and fields into a protobuf LogEntry.
func (c *StreamingCore) convertToLogEntry(ent zapcore.Entry, fields []zapcore.Field) *v1.LogEntry {
	// Build message with structured fields appended as JSON
	message := ent.Message
	if len(fields) > 0 {
		structuredFields := make(map[string]interface{}, len(fields))
		for _, f := range fields {
			structuredFields[f.Key] = fieldValue(f)
		}
		if jsonBytes, err := json.Marshal(structuredFields); err == nil {
			message = fmt.Sprintf("%s %s", ent.Message, string(jsonBytes))
		}
	}

	// Map zap level to string
	var level string
	switch ent.Level {
	case zapcore.DebugLevel:
		level = "DEBUG"
	case zapcore.InfoLevel:
		level = "INFO"
	case zapcore.WarnLevel:
		level = "WARN"
	case zapcore.ErrorLevel:
		level = "ERROR"
	case zapcore.DPanicLevel, zapcore.PanicLevel, zapcore.FatalLevel:
		level = "FATAL"
	default:
		level = "INFO"
	}

	entry := &v1.LogEntry{
		Timestamp:  timestamppb.New(ent.Time),
		Message:    message,
		Level:      level,
		LineNumber: c.lineCounter.Add(1),
	}

	// Populate error info for ERROR+ levels
	if ent.Level >= zapcore.ErrorLevel {
		errorInfo := &v1.LogErrorInfo{}
		hasErrorInfo := false

		// Extract error info from fields
		for _, f := range fields {
			if f.Type == zapcore.ErrorType {
				if err, ok := f.Interface.(error); ok {
					errorInfo.ErrorType = fmt.Sprintf("%T", err)
					errorInfo.ErrorMessage = err.Error()
					hasErrorInfo = true
				}
			}
		}

		// Add stack trace from the entry
		if ent.Stack != "" {
			errorInfo.StackTrace = ent.Stack
			hasErrorInfo = true
		}

		if hasErrorInfo {
			entry.ErrorInfo = errorInfo
		}
	}

	return entry
}

// fieldValue extracts the value from a zapcore.Field for JSON serialization.
func fieldValue(f zapcore.Field) interface{} {
	switch f.Type {
	case zapcore.BoolType:
		return f.Integer == 1
	case zapcore.Int8Type, zapcore.Int16Type, zapcore.Int32Type, zapcore.Int64Type:
		return f.Integer
	case zapcore.Uint8Type, zapcore.Uint16Type, zapcore.Uint32Type, zapcore.Uint64Type, zapcore.UintptrType:
		return uint64(f.Integer)
	case zapcore.Float32Type:
		return math.Float32frombits(uint32(f.Integer))
	case zapcore.Float64Type:
		return math.Float64frombits(uint64(f.Integer))
	case zapcore.StringType:
		return f.String
	case zapcore.ErrorType:
		if err, ok := f.Interface.(error); ok {
			return err.Error()
		}
		return f.Interface
	case zapcore.DurationType:
		return time.Duration(f.Integer).String()
	case zapcore.TimeType:
		if t, ok := f.Interface.(time.Time); ok {
			return t.Format(time.RFC3339Nano)
		}
		return time.Unix(0, f.Integer).Format(time.RFC3339Nano)
	case zapcore.StringerType:
		if s, ok := f.Interface.(fmt.Stringer); ok {
			return s.String()
		}
		return fmt.Sprintf("%v", f.Interface)
	default:
		if f.Interface != nil {
			return fmt.Sprintf("%v", f.Interface)
		}
		return f.String
	}
}
