package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/monolithiclab/millstream/internal/redact"
)

// Harness modes (MILLSTREAM_HARNESS_MODE): live calls the harness; record calls it and stores each response;
// replay answers from stored responses only, so tests and evals never reach a model.
const (
	ModeLive   = "live"
	ModeRecord = "record"
	ModeReplay = "replay"
)

// Environment variables of the record/replay layer.
const (
	EnvHarnessMode = "MILLSTREAM_HARNESS_MODE"
	EnvHarnessDir  = "MILLSTREAM_HARNESS_DIR"
)

// harnessMode reads the harness mode and the recordings directory from MILLSTREAM_HARNESS_MODE (live when unset)
// and MILLSTREAM_HARNESS_DIR, which record and replay need.
func harnessMode(env func(string) (string, bool)) (mode, dir string, err error) {
	mode, _ = env(EnvHarnessMode)
	dir, _ = env(EnvHarnessDir)
	switch mode {
	case "":
		mode = ModeLive
	case ModeLive:
	case ModeRecord, ModeReplay:
		if dir == "" {
			return "", "", fmt.Errorf("%s=%s needs %s, the recordings directory", EnvHarnessMode, mode, EnvHarnessDir)
		}
	default:
		return "", "", fmt.Errorf("%s: %q is not one of %s, %s, %s", EnvHarnessMode, mode, ModeLive, ModeRecord,
			ModeReplay)
	}
	return mode, dir, nil
}

// Recorder wraps a harness with record/replay. A recording is keyed by the harness, the model, the rendered prompt
// and the output schema (Key), so anything that changes the prompt, a lessons file included, misses the old
// recordings rather than replaying a stale answer.
type Recorder struct {
	Harness Harness
	// Name is the harness's name (claude-code, api), part of the key.
	Name string
	// Mode is ModeLive, ModeRecord or ModeReplay; Dir holds the recordings, one <key>.json file each.
	Mode string
	Dir  string
	// Redact redacts the secret values out of what a recording stores, the prompt turn and the model's output
	// included: recordings are committed. The key is still the hash of the request as sent.
	Redact *redact.Redactor
}

// Recording is a stored harness response with what it was recorded for.
type Recording struct {
	Key      string   `json:"key"`
	Harness  string   `json:"harness"`
	Model    string   `json:"model"`
	Response Response `json:"response"`
}

// Key is the recording key of a request to the harness name: the hex SHA-256 of the harness, the model, the
// rendered prompt and the output schema.
func Key(harness string, req Request) string {
	enc, _ := json.Marshal([]string{harness, req.Model, req.Prompt, string(req.Schema)}) // strings always encode
	sum := sha256.Sum256(enc)
	return hex.EncodeToString(sum[:])
}

// Run implements Harness.
func (r Recorder) Run(ctx context.Context, req Request) (Response, error) {
	switch r.Mode {
	case ModeReplay:
		return r.replay(req)
	case ModeRecord:
		resp, err := r.Harness.Run(ctx, req)
		if err != nil {
			return resp, err
		}
		return resp, r.store(req, resp)
	default:
		return r.Harness.Run(ctx, req)
	}
}

func (r Recorder) path(key string) string { return filepath.Join(r.Dir, key+".json") }

func (r Recorder) replay(req Request) (Response, error) {
	key := Key(r.Name, req)
	data, err := os.ReadFile(r.path(key))
	if errors.Is(err, fs.ErrNotExist) {
		return Response{}, permanent("replay: no recording %s for harness %s and model %s in %s: record it with %s=%s",
			key, r.Name, req.Model, r.Dir, EnvHarnessMode, ModeRecord)
	}
	if err != nil {
		return Response{}, permanent("replay: %v", err)
	}
	var rec Recording
	if err := json.Unmarshal(data, &rec); err != nil {
		return Response{}, permanent("replay: recording %s: %v", key, err)
	}
	return rec.Response, nil
}

// store writes the recording through a temporary file, so a reader never sees half of one.
func (r Recorder) store(req Request, resp Response) error {
	key := Key(r.Name, req)
	raw, err := json.Marshal(Recording{Key: key, Harness: r.Name, Model: req.Model, Response: resp})
	if err == nil {
		raw, err = r.Redact.JSON(raw) // every string, so a field added to Response is redacted too
	}
	var data bytes.Buffer
	if err == nil {
		err = json.Indent(&data, raw, "", "  ")
	}
	if err != nil {
		return fmt.Errorf("record: %w", err)
	}
	if err := os.MkdirAll(r.Dir, 0o750); err != nil {
		return fmt.Errorf("record: %w", err)
	}
	tmp, err := os.CreateTemp(r.Dir, ".recording-*")
	if err != nil {
		return fmt.Errorf("record: %w", err)
	}
	data.WriteByte('\n')
	_, err = tmp.Write(data.Bytes())
	if cerr := tmp.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp.Name(), r.path(key))
	}
	if err != nil {
		_ = os.Remove(tmp.Name())
		return fmt.Errorf("record: %w", err)
	}
	return nil
}
