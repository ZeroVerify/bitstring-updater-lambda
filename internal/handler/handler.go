package handler

import (
    "context"
    "log"

    "github.com/ZeroVerify/bitstring-updater-lambda/internal/s3"
    "github.com/ZeroVerify/bitstring-updater-lambda/internal/stream"
    "github.com/aws/aws-lambda-go/events"
)

func Handle(ctx context.Context, event events.DynamoDBEvent) error {
    mutations := stream.Parse(event.Records)

    if len(mutations) == 0 {
        log.Printf("no mutations to apply (batch size=%d, all skipped)", len(event.Records))
        return nil
    }

    log.Printf("applying %d bit mutations from %d stream records", len(mutations), len(event.Records))

    if err := s3.ApplyMutations(ctx, mutations); err != nil {

        return err
    }

    return nil
}