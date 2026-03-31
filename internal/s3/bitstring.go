package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/ZeroVerify/bitstring-updater-lambda/internal/stream"
)

const (
	maxRetries = 5
	bucket     = "zeroverify-artifacts"
	key        = "bitstring/v1/bitstring.gz"
)

var client *awss3.Client

func init() {
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}
	client = awss3.NewFromConfig(cfg)
}

func ApplyMutations(ctx context.Context, mutations []stream.BitMutation) error {
	for attempt := 0; attempt < maxRetries; attempt++ {
		etag, bits, err := download(ctx)
		if err != nil {
			return fmt.Errorf("download failed: %w", err)
		}

		for _, m := range mutations {
			bytePos := m.BitIndex / 8
			bitPos := uint(7 - m.BitIndex%8)
			if bytePos >= len(bits) {
				log.Printf("SKIP: bit_index %d out of range (bitstring len=%d bytes)", m.BitIndex, len(bits))
				continue
			}
			if m.TargetBit == 1 {
				bits[bytePos] |= 1 << bitPos
			} else {
				bits[bytePos] &^= 1 << bitPos
			}
		}

		err = upload(ctx, etag, bits)
		if err == nil {
			return nil
		}

		var apiErr interface{ ErrorCode() string }

        if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
			log.Printf("ETag conflict on attempt %d/%d, retrying with fresh download", attempt+1, maxRetries)
			continue
		}

		return fmt.Errorf("upload failed: %w", err)
	}

	return fmt.Errorf("exceeded %d retries on ETag conflict", maxRetries)
}

func download(ctx context.Context) (etag string, bits []byte, err error) {
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", nil, err
	}
	defer out.Body.Close()

	gr, err := gzip.NewReader(out.Body)
	if err != nil {
		return "", nil, fmt.Errorf("gzip open: %w", err)
	}
	defer gr.Close()

	b64, err := io.ReadAll(gr)
	if err != nil {
		return "", nil, fmt.Errorf("gzip read: %w", err)
	}

	bits, err = base64.StdEncoding.DecodeString(string(b64))
	if err != nil {
		return "", nil, fmt.Errorf("base64 decode: %w", err)
	}

	return aws.ToString(out.ETag), bits, nil
}


func upload(ctx context.Context, etag string, bits []byte) error {
	b64 := base64.StdEncoding.EncodeToString(bits)

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write([]byte(b64)); err != nil {
		return fmt.Errorf("gzip write: %w", err)
	}
	if err := gw.Close(); err != nil {
		return fmt.Errorf("gzip close: %w", err)
	}

	_, err := client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader(buf.Bytes()),
		IfMatch:      aws.String(etag),
		ContentType:  aws.String("application/gzip"),
		CacheControl: aws.String("public, max-age=300"),
	})
	return err
}