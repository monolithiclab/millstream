package cli

import (
	"strings"
	"testing"
)

func Test_varsCommand(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		argv     []string
		cmd      runner // run directly instead of argv
		env      map[string]string
		wantCode int
		wantOut  string
		wantErr  string
	}{
		{
			name: "defaults, without needing secrets",
			cmd:  VarsCmd{Dir: fixture("valid")},
			wantOut: "NAME      VALUE   SOURCE   ENVIRONMENT\n" +
				"city      \"Lyon\"  default  MILLSTREAM_VAR_CITY\n" +
				"maxPrice  400000  default  MILLSTREAM_VAR_MAX_PRICE\n",
		},
		{
			name: "flag over environment",
			argv: []string{"vars", fixture("valid"), "--var", "city=Paris"},
			env:  map[string]string{"MILLSTREAM_VAR_CITY": "Nantes", "MILLSTREAM_VAR_MAX_PRICE": "350000"},
			wantOut: "NAME      VALUE    SOURCE  ENVIRONMENT\n" +
				"city      \"Paris\"  flag    MILLSTREAM_VAR_CITY\n" +
				"maxPrice  350000   env     MILLSTREAM_VAR_MAX_PRICE\n",
		},
		{
			name:     "required variable missing",
			cmd:      VarsCmd{Dir: fixture("config-schema")},
			wantCode: 1,
			wantOut: "NAME      VALUE     SOURCE   ENVIRONMENT\n" +
				"maxPrice  \"400000\"  default  MILLSTREAM_VAR_MAX_PRICE\n" +
				"region    -         unset    MILLSTREAM_VAR_REGION\n",
			wantErr: "millstream: resolve pipeline p: variable region is required: set --var region=… or MILLSTREAM_VAR_REGION\n",
		},
		{
			name:     "wrong type and unknown flag",
			argv:     []string{"vars", fixture("valid"), "--var", "nope=1"},
			env:      map[string]string{"MILLSTREAM_VAR_MAX_PRICE": "cheap"},
			wantCode: 1,
			wantOut: "NAME      VALUE   SOURCE   ENVIRONMENT\n" +
				"city      \"Lyon\"  default  MILLSTREAM_VAR_CITY\n" +
				"maxPrice  -       unset    MILLSTREAM_VAR_MAX_PRICE\n",
			wantErr: "--var nope: pipeline listings declares no such variable; variable maxPrice from env: not valid JSON",
		},
		{name: "load error", cmd: VarsCmd{Dir: fixture("mixed-out")}, wantCode: 1, wantErr: "never both"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			run := func() (int, string, string) { return runCLIEnv(t, "dev", tt.env, tt.argv...) }
			if tt.cmd != nil {
				run = func() (int, string, string) { return direct(tt.env, tt.cmd) }
			}
			code, out, errOut := run()
			if code != tt.wantCode {
				t.Fatalf("code = %d, want %d; stderr %q", code, tt.wantCode, errOut)
			}
			if out != tt.wantOut {
				t.Fatalf("stdout:\n%s\nwant:\n%s", out, tt.wantOut)
			}
			if !strings.Contains(errOut, tt.wantErr) {
				t.Fatalf("stderr = %q, want %q", errOut, tt.wantErr)
			}
		})
	}
}
