package millstream

import "embed"

// Source is this module's source, go.mod and its Go files: the engine builds proposed Go actions against it in its
// network-less sandbox (spec section 15.3), where no module can be downloaded. Actions never need it.
//
//go:embed go.mod *.go
var Source embed.FS
