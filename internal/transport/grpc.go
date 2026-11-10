package transport

import (
	"context"
	"errors"
	"strconv"

	"github.com/adityaanilraut/distributed-kv-store/api"
	"github.com/adityaanilraut/distributed-kv-store/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type KeyValue struct {
	api.UnimplementedKeyValueServer
	Node *raft.Node
}

func (s *KeyValue) operation(ctx context.Context, op raft.Operation) (raft.Result, error) {
	if len(op.Key) == 0 || len(op.Key) > 256 || len(op.Value) > 4096 {
		return raft.Result{}, status.Error(codes.InvalidArgument, "keys must be 1–256 bytes and values at most 4096 bytes")
	}
	res := s.Node.Propose(ctx, op)
	switch {
	case res.Err == nil:
		return res, nil
	case errors.Is(res.Err, raft.ErrNotLeader):
		if res.Leader != 0 {
			_ = grpc.SetTrailer(ctx, metadata.Pairs("leader-id", strconv.FormatUint(res.Leader, 10)))
		}
		return res, status.Error(codes.FailedPrecondition, "not leader")
	case errors.Is(res.Err, raft.ErrBusy):
		return res, status.Error(codes.ResourceExhausted, "proposal queue full")
	case errors.Is(res.Err, context.DeadlineExceeded), errors.Is(res.Err, context.Canceled):
		return res, status.FromContextError(res.Err).Err()
	default:
		return res, status.Error(codes.Unavailable, "operation outcome unknown")
	}
}

func (s *KeyValue) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	res, err := s.operation(ctx, raft.Operation{Kind: "get", Key: req.Key})
	if err != nil {
		return nil, err
	}
	return &api.GetResponse{Found: res.Found, Value: res.Value, Index: res.Index}, nil
}
func (s *KeyValue) Put(ctx context.Context, req *api.PutRequest) (*api.WriteResponse, error) {
	res, err := s.operation(ctx, raft.Operation{Kind: "put", Key: req.Key, Value: req.Value})
	if err != nil {
		return nil, err
	}
	return &api.WriteResponse{Index: res.Index}, nil
}
func (s *KeyValue) Delete(ctx context.Context, req *api.DeleteRequest) (*api.WriteResponse, error) {
	res, err := s.operation(ctx, raft.Operation{Kind: "delete", Key: req.Key})
	if err != nil {
		return nil, err
	}
	return &api.WriteResponse{Index: res.Index}, nil
}
