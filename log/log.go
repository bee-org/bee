package log

import (
	"fmt"
	"log"
	"os"
)

// ILogger does underlying logging work for bee.
type ILogger interface {
	// Debugln logs to DEBUG log. Arguments are handled in the manner of fmt.Println.
	Debugln(args ...interface{})
	// Debugf logs to DEBUG log. Arguments are handled in the manner of fmt.Printf.
	Debugf(format string, args ...interface{})
	// Infoln logs to INFO log. Arguments are handled in the manner of fmt.Println.
	Infoln(args ...interface{})
	// Infof logs to INFO log. Arguments are handled in the manner of fmt.Printf.
	Infof(format string, args ...interface{})
	// Warningln logs to WARNING log. Arguments are handled in the manner of fmt.Println.
	Warningln(args ...interface{})
	// Warningf logs to WARNING log. Arguments are handled in the manner of fmt.Printf.
	Warningf(format string, args ...interface{})
	// Errorln logs to ERROR log. Arguments are handled in the manner of fmt.Println.
	Errorln(args ...interface{})
	// Errorf logs to ERROR log. Arguments are handled in the manner of fmt.Printf.
	Errorf(format string, args ...interface{})
	// SetLevel Set the log printing level, default is DebugLevel
	SetLevel(level Level) ILogger
}

type Level uint8

const (
	DebugLevel Level = iota
	InfoLevel
	WarningLevel
	ErrorLevel
)

func NewDefaultLogger() ILogger {
	return &Logger{
		debug:   log.New(os.Stderr, "bee DEBUG ", log.LstdFlags|log.Lshortfile),
		info:    log.New(os.Stderr, "bee INFO ", log.LstdFlags|log.Lshortfile),
		warning: log.New(os.Stderr, "bee WARNING ", log.LstdFlags|log.Lshortfile),
		error:   log.New(os.Stderr, "bee ERROR ", log.LstdFlags|log.Lshortfile),
	}
}

type Logger struct {
	level   Level
	debug   *log.Logger
	info    *log.Logger
	warning *log.Logger
	error   *log.Logger
}

func (l *Logger) Debugln(args ...interface{}) {
	if l.level > DebugLevel {
		return
	}
	_ = l.debug.Output(2, fmt.Sprintln(args...))
}

func (l *Logger) Debugf(format string, args ...interface{}) {
	if l.level > DebugLevel {
		return
	}
	_ = l.debug.Output(2, fmt.Sprintf(format, args...))
}

func (l *Logger) Infoln(args ...interface{}) {
	if l.level > InfoLevel {
		return
	}
	_ = l.info.Output(2, fmt.Sprintln(args...))
}

func (l *Logger) Infof(format string, args ...interface{}) {
	if l.level > InfoLevel {
		return
	}
	_ = l.info.Output(2, fmt.Sprintf(format, args...))
}

func (l *Logger) Warningln(args ...interface{}) {
	if l.level > WarningLevel {
		return
	}
	_ = l.warning.Output(2, fmt.Sprintln(args...))
}

func (l *Logger) Warningf(format string, args ...interface{}) {
	if l.level > WarningLevel {
		return
	}
	_ = l.warning.Output(2, fmt.Sprintf(format, args...))
}

func (l *Logger) Errorln(args ...interface{}) {
	if l.level > ErrorLevel {
		return
	}
	_ = l.error.Output(2, fmt.Sprintln(args...))
}

func (l *Logger) Errorf(format string, args ...interface{}) {
	if l.level > ErrorLevel {
		return
	}
	_ = l.error.Output(2, fmt.Sprintf(format, args...))
}

func (l *Logger) SetLevel(level Level) ILogger {
	l.level = level
	return l
}
