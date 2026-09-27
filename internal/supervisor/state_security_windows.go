//go:build windows

package supervisor

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"golang.org/x/sys/windows"
)

// secureStateDir protects supervisor state before config or the database is
// opened. Output retention is stored in state.db and inherits the directory
// DACL when the database is first created.
func secureStateDir(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return fmt.Errorf("inspect state directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state path is not a directory")
	}

	tokenUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fmt.Errorf("read state owner SID: %w", err)
	}
	ownerSID := tokenUser.User.Sid.String()
	directoryACL, err := privateStateACL(ownerSID, true)
	if err != nil {
		return err
	}
	if err := applyStateACL(root, directoryACL); err != nil {
		return fmt.Errorf("protect state directory DACL: %w", err)
	}

	// These can predate the protected root DACL. Apply protected ACLs directly
	// before config is read or the database is opened. All retained output is
	// inside state.db.
	fileACL, err := privateStateACL(ownerSID, false)
	if err != nil {
		return err
	}
	for _, name := range []string{"config.json", "state.db"} {
		path := filepath.Join(root, name)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("inspect state file: %w", err)
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("state file %s is not a regular file", name)
		}
		if err := applyStateACL(path, fileACL); err != nil {
			return fmt.Errorf("protect %s DACL: %w", name, err)
		}
	}
	return nil
}

func privateStateACL(ownerSID string, directory bool) (*windows.ACL, error) {
	inheritance := ""
	if directory {
		inheritance = "OICI"
	}
	sd, err := windows.SecurityDescriptorFromString("D:P(A;" + inheritance + ";FA;;;" + ownerSID + ")")
	if err != nil {
		return nil, fmt.Errorf("build owner-only state DACL: %w", err)
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return nil, fmt.Errorf("read owner-only state DACL: %w", err)
	}
	return dacl, nil
}

func applyStateACL(path string, dacl *windows.ACL) error {
	return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		nil, nil, dacl, nil)
}
