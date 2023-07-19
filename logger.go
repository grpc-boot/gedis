package gedis

import (
	"github.com/grpc-boot/base"
	"go.uber.org/zap"
)

var (
	Error = base.Error
)

type Log func(msg string, fields ...zap.Field)
