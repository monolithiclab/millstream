package s3

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/monolithiclab/millstream/internal/artifacts"
	"github.com/monolithiclab/millstream/internal/store"
)

// What the artifact sweep needs of the bucket (artifacts.Files, spec section 9.3): the objects under the prefix
// listed a page at a time (ListObjectsV2), an object's Last-Modified, server-side copies and deletes. A bucket holds
// no temporary files: Put spools a content on the engine's disk before sending it.

// listResult is the part of a ListObjectsV2 answer the store reads.
type listResult struct {
	IsTruncated           bool
	NextContinuationToken string
	Contents              []struct {
		Key          string
		LastModified time.Time
	}
}

// List implements artifacts.Files: every artifact under the prefix. Keys that are no artifact (another prefix
// level, a name that is no SHA-256) are not the store's and are left out.
func (s *Store) List(ctx context.Context) ([]artifacts.File, error) {
	root := ""
	if s.cfg.Prefix != "" {
		root = s.cfg.Prefix + "/"
	}
	var out []artifacts.File
	for token := ""; ; {
		page, err := s.listPage(ctx, root, token)
		if err != nil {
			return nil, err
		}
		for _, c := range page.Contents {
			pipeline, name, ok := strings.Cut(strings.TrimPrefix(c.Key, root), "/")
			if ok && artifacts.CheckPipeline(pipeline) == nil && store.ValidSHA256(name) {
				out = append(out, artifacts.File{Pipeline: pipeline, Name: name, ModTime: c.LastModified})
			}
		}
		if !page.IsTruncated || page.NextContinuationToken == "" {
			return out, nil
		}
		token = page.NextContinuationToken
	}
}

// listPage lists one page of the keys under prefix, from the continuation token when it is not empty.
func (s *Store) listPage(ctx context.Context, prefix, token string) (*listResult, error) {
	q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
	if token != "" {
		q.Set("continuation-token", token)
	}
	u := *s.endpoint
	u.Path = strings.TrimSuffix(u.Path, "/") + "/" + s.cfg.Bucket
	u.RawPath, u.RawQuery = "", canonicalQuery(q)
	resp, err := s.doURL(ctx, http.MethodGet, &u, nil, 0, emptySHA256, nil)
	if err != nil {
		return nil, fmt.Errorf("list artifacts: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, statusErr("list", prefix, resp)
	}
	var page listResult
	if err := xml.NewDecoder(resp.Body).Decode(&page); err != nil {
		return nil, fmt.Errorf("list artifacts: %w", err)
	}
	return &page, nil
}

// ModTime implements artifacts.Files: the object's Last-Modified.
func (s *Store) ModTime(ctx context.Context, pipeline, sha string) (time.Time, error) {
	key, err := s.key(pipeline, sha)
	if err != nil {
		return time.Time{}, err
	}
	resp, err := s.head(ctx, key)
	if err != nil {
		return time.Time{}, fmt.Errorf("artifact %s of %s: %w", sha, pipeline, err)
	}
	t, err := http.ParseTime(resp.Header.Get("Last-Modified"))
	if err != nil {
		return time.Time{}, fmt.Errorf("artifact %s of %s: Last-Modified: %w", sha, pipeline, err)
	}
	return t, nil
}

// Remove implements artifacts.Files.
func (s *Store) Remove(ctx context.Context, pipeline, name string) error {
	key, err := s.key(pipeline, name)
	if err != nil {
		return fmt.Errorf("remove artifact: %w", err)
	}
	resp, err := s.do(ctx, http.MethodDelete, key, nil, 0, emptySHA256, nil)
	if err != nil {
		return fmt.Errorf("remove artifact: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	}
	return statusErr("delete", key, resp)
}

// Touch implements artifacts.Toucher.
func (s *Store) Touch(ctx context.Context, pipeline, sha string) error {
	key, err := s.key(pipeline, sha)
	if err != nil {
		return err
	}
	if err := s.touch(ctx, key); err != nil {
		return fmt.Errorf("artifact %s of %s: %w", sha, pipeline, err)
	}
	return nil
}

// touch copies key onto itself, its metadata replaced by the same, which sets its Last-Modified to now: S3 has no
// other way to touch an object. An absent key wraps fs.ErrNotExist.
func (s *Store) touch(ctx context.Context, key string) error {
	resp, err := s.head(ctx, key)
	if err != nil {
		return err
	}
	h := http.Header{"X-Amz-Metadata-Directive": {"REPLACE"}}
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		h.Set("Content-Type", ct)
	}
	return s.copyObject(ctx, key, key, h)
}
