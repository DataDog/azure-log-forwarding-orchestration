// Unless explicitly stated otherwise all files in this repository are licensed under the Apache-2 License.

// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

package storage

import (
	// stdlib
	"context"
	"fmt"
	"io"

	// 3p
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"

	// project
	"github.com/DataDog/azure-log-forwarding-orchestration/forwarder/internal/environment"
)

// BlobSegment represents a segment of a blob that could be partial or full
type BlobSegment struct {
	Name          string
	Container     string
	Reader        io.ReadCloser
	Offset        int64
	ContentLength int64
}

// DownloadSegment downloads a segment of a blob starting from an offset
func (c *Client) DownloadSegment(ctx context.Context, blob Blob, offset int64, contentLength int64) (BlobSegment, error) {
	// Create span for download segment operation
	if environment.APMEnabled() {
		span, spanCtx := tracer.StartSpanFromContext(ctx, "storage.Client.DownloadSegment")
		defer func() {
			span.SetTag("azure.container.name", blob.Container.Name)
			span.SetTag("azure.blob.name", blob.Name)
			span.SetTag("azure.blob.offset", offset)
			span.SetTag("azure.blob.content_length", contentLength)
			span.SetTag("azure.blob.segment_size", contentLength-offset)
			span.Finish()
		}()
		ctx = spanCtx
	}

	options := &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{
			Offset: offset,
			Count:  contentLength - offset,
		},
	}

	resp, err := c.azBlobClient.DownloadStream(ctx, blob.Container.Name, blob.Name, options)

	if err != nil {
		return BlobSegment{}, fmt.Errorf("failed to download blob: %w", err)
	}
	return BlobSegment{
		Name:          blob.Name,
		Container:     blob.Container.Name,
		Reader:        resp.Body,
		Offset:        offset,
		ContentLength: blob.ContentLength,
	}, nil
}
