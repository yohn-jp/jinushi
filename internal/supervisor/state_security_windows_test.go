//go:build windows

package supervisor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestSecureStateDirRestrictsExistingRunState(t *testing.T) {
	root := t.TempDir()
	runs := filepath.Join(root, "runs")
	run := filepath.Join(runs, "run-123")
	nested := filepath.Join(run, "spools")
	for _, path := range []string{run, nested} {
		if err := os.MkdirAll(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	files := []string{
		filepath.Join(root, "config.json"),
		filepath.Join(root, "state.db"),
		filepath.Join(root, "state.db-wal"),
		filepath.Join(root, "state.db-shm"),
		filepath.Join(root, "state.db-journal"),
		filepath.Join(run, "descriptor.json"),
		filepath.Join(run, "status.json"),
		filepath.Join(run, "stdout.out"),
		filepath.Join(nested, "stderr.out"),
	}
	for _, path := range files {
		if err := os.WriteFile(path, []byte("private state"), 0600); err != nil {
			t.Fatal(err)
		}
	}

	directories := []string{root, runs, run, nested}
	for _, path := range append(directories, files...) {
		if err := setPermissiveTestDACL(path); err != nil {
			t.Fatalf("make %s permissive: %v", path, err)
		}
	}
	if err := secureStateDir(root); err != nil {
		t.Fatal(err)
	}

	tokenUser, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatal(err)
	}
	ownerSID := tokenUser.User.Sid.String()
	cases := []struct {
		path      string
		directory bool
	}{
		{root, true},
		{runs, true},
		{run, true},
		{nested, true},
	}
	for _, path := range files {
		cases = append(cases, struct {
			path      string
			directory bool
		}{path, false})
	}
	for _, test := range cases {
		t.Run(filepath.Base(test.path), func(t *testing.T) {
			assertOwnerOnlyStatePath(t, test.path, ownerSID, test.directory)
		})
	}
}

func TestSecureStateDirRejectsRunTreeReparsePoint(t *testing.T) {
	root := t.TempDir()
	runs := filepath.Join(root, "runs")
	if err := os.MkdirAll(runs, 0700); err != nil {
		t.Fatal(err)
	}
	target := t.TempDir()
	link := filepath.Join(runs, "linked-run")
	if err := os.Symlink(target, link); err != nil {
		t.Skipf("creating a Windows directory symlink is unavailable: %v", err)
	}
	if err := secureStateDir(root); err == nil {
		t.Fatal("state migration accepted a Run directory symlink/reparse point")
	}
}

func assertOwnerOnlyStatePath(t *testing.T, path, ownerSID string, directory bool) {
	t.Helper()
	acl, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	inheritance := ""
	if directory {
		inheritance = "OICI"
	}
	wantACL := fmt.Sprintf("D:P(A;%s;FA;;;%s)", inheritance, ownerSID)
	if got := acl.String(); got != wantACL {
		t.Fatalf("DACL = %q, want owner-only %q", got, wantACL)
	}
	control, _, err := acl.Control()
	if err != nil {
		t.Fatal(err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Fatal("state DACL is not protected from inherited access")
	}

	ownerDescriptor, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		t.Fatal(err)
	}
	owner, _, err := ownerDescriptor.Owner()
	if err != nil {
		t.Fatal(err)
	}
	if got := owner.String(); got != ownerSID {
		t.Fatalf("owner SID = %q, want current user %q", got, ownerSID)
	}
}

func setPermissiveTestDACL(path string) error {
	sd, err := windows.SecurityDescriptorFromString("D:(A;;FA;;;WD)")
	if err != nil {
		return err
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION, nil, nil, dacl, nil)
}
