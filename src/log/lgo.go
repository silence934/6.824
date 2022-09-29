package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var errorLogger *zap.SugaredLogger
var atomicLevel zap.AtomicLevel

func init() {

	// 设置一些基本日志格式 具体含义还比较好理解，直接看zap源码也不难懂

	encoder := zapcore.NewConsoleEncoder(zapcore.EncoderConfig{

		MessageKey: "msg",

		LevelKey: "level",

		EncodeLevel: zapcore.LowercaseColorLevelEncoder,

		TimeKey: "ts",

		EncodeTime: zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05.000"),

		CallerKey: "caller",

		EncodeCaller: zapcore.ShortCallerEncoder,

		EncodeDuration: zapcore.SecondsDurationEncoder,
	})

	atomicLevel = zap.NewAtomicLevel()
	atomicLevel.SetLevel(zapcore.ErrorLevel)
	// 最后创建具体的Logger
	core := zapcore.NewCore(encoder, zapcore.NewMultiWriteSyncer(zapcore.AddSync(os.Stdout)), atomicLevel)

	log := zap.New(core, zap.AddCaller())

	errorLogger = log.Sugar()
}

func SetLogLevel(level zapcore.Level) {
	atomicLevel.SetLevel(level)
}

func Logger() *zap.SugaredLogger {
	return errorLogger
}
