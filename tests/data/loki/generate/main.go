// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Generate real Loki object-store envelopes, not an independent reimplementation.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/grafana/loki/v3/pkg/chunkenc"
	"github.com/grafana/loki/v3/pkg/compression"
	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/grafana/loki/v3/pkg/storage/chunk"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

func generate(version byte, codec compression.Codec, empty bool) ([]byte, error) {
	c := chunkenc.NewMemChunk(version, codec, chunkenc.ChunkHeadFormatFor(version), 16, 0)
	for i := 0; !empty && i < 3; i++ {
		entry := logproto.Entry{
			Timestamp: time.Unix(0, 1700000000123456789+int64(i)),
			Line:      []string{"hello loki", "你好 loki", "{\"status\":200}"}[i],
		}
		if version == chunkenc.ChunkFormatV4 {
			entry.StructuredMetadata = []logproto.LabelAdapter{{Name: "trace_id", Value: fmt.Sprintf("trace-%d", i)}}
		}
		if _, err := c.Append(&entry); err != nil {
			return nil, err
		}
	}
	if err := c.Close(); err != nil {
		return nil, err
	}
	outer := chunk.NewChunk("test-tenant", model.Fingerprint(123),
		labels.FromStrings("service", "api", "env", "test"),
		chunkenc.NewFacade(c, 16, 0), model.Time(1700000000123), model.Time(1700000000124))
	return outer.Encoded()
}

func main() {
	root := ".."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	if err := os.MkdirAll(filepath.Join(root, "chunks"), 0755); err != nil {
		panic(err)
	}
	for version := byte(1); version <= 4; version++ {
		for _, codec := range []compression.Codec{compression.None, compression.GZIP, compression.LZ4_64k,
			compression.Snappy, compression.LZ4_256k, compression.LZ4_1M, compression.LZ4_4M,
			// typos:ignore-next-line
			compression.Flate, compression.Zstd} {
			if version == 1 && codec != compression.GZIP {
				continue
			}
			data, err := generate(version, codec, false)
			if err != nil {
				panic(err)
			}
			path := filepath.Join(root, "chunks", fmt.Sprintf("v%d-codec%d", version, codec))
			if err := os.WriteFile(path, data, 0644); err != nil {
				panic(err)
			}
		}
	}
	data, err := generate(4, compression.GZIP, true)
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(filepath.Join(root, "empty"), data, 0644); err != nil {
		panic(err)
	}
	// A bad inner block/meta CRC with an otherwise valid envelope.
	data, err = generate(3, compression.None, false)
	if err != nil {
		panic(err)
	}
	data[len(data)-9] ^= 1
	if err := os.WriteFile(filepath.Join(root, "bad-crc"), data, 0644); err != nil {
		panic(err)
	}
}
