//go:build linux

package supervisor

import (
	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/backend/linux"
)

// PlatformBackendFactory selects the native Linux execution backend.
func PlatformBackendFactory() backend.Factory { return linux.New }
