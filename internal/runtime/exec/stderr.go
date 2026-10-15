package exec

import (
	"bufio"
)

// maxLogLine is the longest stderr line re-emitted; the rest of a longer line is dropped.
const maxLogLine = 64 << 10

// relayStderr re-emits each stderr line at info level through the current delivery's logger, so it carries the
// delivery's keys. It keeps reading past an overlong line, so the action never blocks on a full pipe.
func (p *Process) relayStderr() {
	br := bufio.NewReaderSize(p.stderr, maxLogLine)
	for {
		line, more, err := br.ReadLine()
		if err != nil {
			return
		}
		p.logger().Info(string(line))
		for more && err == nil {
			_, more, err = br.ReadLine()
		}
	}
}
