package iface_test

import (
	"os/exec"
	"strings"
	"testing"
)

// std: yoke-sdk-go:the-interface-library.09
func TestAPanelLinksNothingOfTheAdministrativeContract(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("the interface library's packages cannot be listed: %v", err)
	}
	for _, path := range strings.Fields(string(out)) {
		if strings.Contains(path, "/administrative/") || strings.HasSuffix(path, "/admin") || strings.HasSuffix(path, "/operatorapi") {
			t.Errorf("the interface library is built from %s, which carries the administrative contract", path)
		}
	}
}
