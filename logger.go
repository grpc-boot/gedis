package gedis

import (
	"github.com/grpc-boot/base"
	"go.uber.org/zap"
)

var (
	Error = base.Error
	Debug = base.Debug
)

type Log func(msg string, fields ...zap.Field)
