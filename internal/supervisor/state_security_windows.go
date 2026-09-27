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
	info, err := statePathInfo(root)
	if err != nil {
		return fmt.Errorf("inspect state directory: %w", err)
	}
	if !info.IsDir() {
		return errors.New("state path is not a directory")
	}

	tokenUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return fmt.Errorf("read state owner SID: %w", err)
	}
	ownerSID := tokenUser.User.Sid
	directoryACL, err := privateStateACL(ownerSID.String(), true)
	if err != nil {
		return err
	}
	if err := applyStateACL(root, ownerSID, directoryACL); err != nil {
		return fmt.Errorf("protect state directory owner and DACL: %w", err)
	}

	ownerSIDText := ownerSID.String()
	fileACL, err := privateStateACL(ownerSIDText, false)
	if err != nil {
		return err
	}
	for _, name := range []string{"config.json", "state.db", "state.db-wal", "state.db-shm", "state.db-journal"} {
		path := filepath.Join(root, name)
		if err := secureExistingStateFile(path, ownerSID, fileACL); err != nil {
			return fmt.Errorf("protect %s: %w", name, err)
		}
	}
	if err := secureRunTree(filepath.Join(root, "runs"), ownerSID, directoryACL, fileACL); err != nil {
		return fmt.Errorf("protect persisted Run state: %w", err)
	}
	return nil
}

func secureExistingStateFile(path string, ownerSID *windows.SID, fileACL *windows.ACL) error {
	info, err := statePathInfo(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect state file: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("state item is not a regular file")
	}
	if err := applyStateACL(path, ownerSID, fileACL); err != nil {
		return fmt.Errorf("set owner-only protected DACL: %w", err)
	}
	return nil
}

func secureRunTree(path string, ownerSID *windows.SID, directoryACL, fileACL *windows.ACL) error {
	info, err := statePathInfo(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("runs path is not a directory")
	}
	return secureRunDirectory(path, ownerSID, directoryACL, fileACL)
}

func secureRunDirectory(path string, ownerSID *windows.SID, directoryACL, fileACL *windows.ACL) error {
	if err := applyStateACL(path, ownerSID, directoryACL); err != nil {
		return fmt.Errorf("set directory owner-only protected DACL: %w", err)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read Run state directory: %w", err)
	}
	for _, entry := range entries {
		child := filepath.Join(path, entry.Name())
		info, err := statePathInfo(child)
		if err != nil {
			return fmt.Errorf("inspect Run state item %q: %w", entry.Name(), err)
		}
		if info.IsDir() {
			if err := secureRunDirectory(child, ownerSID, directoryACL, fileACL); err != nil {
				return fmt.Errorf("protect Run state directory %q: %w", entry.Name(), err)
			}
			continue
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("Run state item %q is neither a directory nor a regular file", entry.Name())
		}
		if err := applyStateACL(child, ownerSID, fileACL); err != nil {
			return fmt.Errorf("protect Run state file %q: %w", entry.Name(), err)
		}
	}
	return nil
}

func statePathInfo(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	path16, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, fmt.Errorf("encode state path: %w", err)
	}
	attributes, err := windows.GetFileAttributes(path16)
	if err != nil {
		return nil, fmt.Errorf("read state item attributes: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || attributes&windows.FILE_ATTRIBUTE_REPARSE_POINT != 0 {
		return nil, errors.New("state item is a symlink or reparse point")
	}
	return info, nil
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

func applyStateACL(path string, ownerSID *windows.SID, dacl *windows.ACL) error {
	return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT,
		windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION,
		ownerSID, nil, dacl, nil)
}
