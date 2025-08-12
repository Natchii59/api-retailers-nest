package grpc

// ServerConfig contient la configuration du serveur gRPC
type ServerConfig struct {
	Port              int    `envconfig:"GRPC_PORT" default:"50051"`
	Host              string `envconfig:"GRPC_HOST" default:"0.0.0.0"`
	MaxRecvMsgSize    int    `envconfig:"GRPC_MAX_RECV_MSG_SIZE" default:"104857600"` // 100MB
	MaxSendMsgSize    int    `envconfig:"GRPC_MAX_SEND_MSG_SIZE" default:"104857600"` // 100MB
	EnableReflection  bool   `envconfig:"GRPC_ENABLE_REFLECTION" default:"true"`
	EnableHealthCheck bool   `envconfig:"GRPC_ENABLE_HEALTH_CHECK" default:"true"`
}
