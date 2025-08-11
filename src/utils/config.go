
// Config holds RabbitMQ connection configuration
type MiddlewareConfig struct {
	Host     string
	Port     int32
	Username string
	Password string
}

const(
	DATASET_EXCHANGE = "dataset-exchange"
	BATCH_SIZE = 30
	
)