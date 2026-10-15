package pipeline

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUnmarshalPipelineRoundTrip(t *testing.T) {
	t.Parallel()
	p, err := Parse([]byte(`
pipeline: listings
vars: { city: Lyon, maxPrice: 400000, ratio: 0.5, tags: [1, 2.5], nested: { n: 3 } }
triggers:
  daily: { cron: "0 6 * * *", tz: Europe/Paris }
  adhoc: { manual: {}, inputSchema: { type: object, properties: { n: { maximum: 5 } } } }
streams:
  listings.raw: { schema: { type: object, minProperties: 1 } }
steps:
  crawl:
    action: ./actions/crawl
    in: trigger.daily
    out: listings.raw
    config: { pages: 3, filters: { maxPrice: "${vars.maxPrice}" } }
    retry: { maxAttempts: 5 }
    timeout: 30s
insights: { window: 7 }
improve: { budget: 2 }
`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalPipeline(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, p) {
		t.Errorf("round trip differs:\n got %#v\nwant %#v", got, p)
	}
}

func TestUnmarshalPipelineErrors(t *testing.T) {
	t.Parallel()
	for _, in := range []string{`{`, `{"Vars":{"x":1e999}}`} {
		if _, err := UnmarshalPipeline([]byte(in)); err == nil {
			t.Errorf("UnmarshalPipeline(%s) succeeded", in)
		}
	}
}

func TestDecodeValue(t *testing.T) {
	t.Parallel()
	v, err := DecodeValue(`{"a": 1, "b": 1.5, "c": [2]}`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"a": int64(1), "b": 1.5, "c": []any{int64(2)}}
	if !reflect.DeepEqual(v, want) {
		t.Errorf("DecodeValue = %#v", v)
	}
	if _, err := DecodeValue(`1 2`); err == nil {
		t.Error("trailing data accepted")
	}
}
