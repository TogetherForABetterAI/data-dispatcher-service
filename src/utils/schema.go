// BatchData represents the data structure to be published to RabbitMQ
type BatchData struct {
	ClientID    string    `json:"client_id"`
	BatchIndex  int32     `json:"batch_index"`
	Data        []byte    `json:"data"`
	IsLastBatch bool      `json:"is_last_batch"`
	Timestamp   time.Time `json:"timestamp"`
}