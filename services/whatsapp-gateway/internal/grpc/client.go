// Package grpc provides the gRPC client for the LLM bot hot path.
package grpc

import (
	"context"
	"fmt"
	"io"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/alan/ia-pipeline/whatsapp-gateway/internal/grpc/botpb"
)

// BotClient wraps the generated gRPC stub with a higher-level Process call.
type BotClient struct {
	conn   *grpc.ClientConn
	client botpb.BotClient
}

// NewBotClient dials addr and returns a ready client.
// Uses insecure credentials (TLS is terminated at the ingress layer).
func NewBotClient(addr string) (*BotClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %w", addr, err)
	}
	return &BotClient{conn: conn, client: botpb.NewBotClient(conn)}, nil
}

// Process sends msg to the bot and assembles the streamed reply into a single string.
// It blocks until the bot sends a chunk with done=true or the stream closes.
func (c *BotClient) Process(ctx context.Context, msg *botpb.InboundMessage) (string, error) {
	stream, err := c.client.Process(ctx, msg)
	if err != nil {
		return "", fmt.Errorf("bot.Process RPC: %w", err)
	}
	var sb strings.Builder
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("recv chunk: %w", err)
		}
		sb.WriteString(chunk.Text)
		if chunk.Done {
			break
		}
	}
	return sb.String(), nil
}

// Close shuts the underlying gRPC connection.
func (c *BotClient) Close() { _ = c.conn.Close() }
