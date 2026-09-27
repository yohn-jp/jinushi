//go:build windows

package supervisor

import (
	"github.com/yohn-jp/jinushi/internal/backend"
	"github.com/yohn-jp/jinushi/internal/backend/windows"
)

// PlatformBackendFactory selects the native Windows execution backend.
func PlatformBackendFactory() backend.Factory { return windows.New }
