//go:build windows

package supervisor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/windows"
)

func TestSecureStateDirRestrictsRootAndExistingStateFiles(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{"config.json", "state.db"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte("test"), 0600); err != nil {
			t.Fatal(err)
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
		path string
		want string
	}{
		{root, fmt.Sprintf("D:P(A;OICI;FA;;;%s)", ownerSID)},
		{filepath.Join(root, "config.json"), fmt.Sprintf("D:P(A;;FA;;;%s)", ownerSID)},
		{filepath.Join(root, "state.db"), fmt.Sprintf("D:P(A;;FA;;;%s)", ownerSID)},
	}
	for _, test := range cases {
		t.Run(filepath.Base(test.path), func(t *testing.T) {
			sd, err := windows.GetNamedSecurityInfo(test.path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
			if err != nil {
				t.Fatal(err)
			}
			if got := sd.String(); got != test.want {
				t.Fatalf("DACL = %q, want owner-only %q", got, test.want)
			}
			control, _, err := sd.Control()
			if err != nil {
				t.Fatal(err)
			}
			if control&windows.SE_DACL_PROTECTED == 0 {
				t.Fatal("state DACL is not protected from inherited access")
			}
		})
	}
}
