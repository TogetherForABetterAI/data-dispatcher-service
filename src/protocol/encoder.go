// src/protocol/encoder.go
package protocol

import (
	"github.com/mlops-eval/data-dispatcher-service/src/pb"
	"google.golang.org/protobuf/proto"
	"fmt"
)

func EncodeClientBatchMessage(data_batch *pb.DataBatch) ([]byte, error) {
	new_batch := &pb.ClientDataBatch{
		Data:        data_batch.Data,
		BatchIndex:  data_batch.BatchIndex,
		IsLastBatch: data_batch.IsLastBatch,
	}

	encoded, err := proto.Marshal(new_batch)
	if err != nil {
		return nil, fmt.Errorf("error serializing batch message: %w", err)
	}

	return encoded, err
}

func EncodeBatchMessage(data_batch *pb.DataBatch) ([]byte, error) {
	encoded, err := proto.Marshal(data_batch)
	if err != nil {
		return nil, fmt.Errorf("error serializing batch message: %w", err)
	}

	return encoded, err
}
