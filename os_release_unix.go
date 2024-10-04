//go:build !windows
// +build !windows

package gospice

import (
	"golang.org/x/sys/unix"
)

func GetOSRelease() string {
	u := unix.Utsname{}
	err := unix.Uname(&u)
	if err != nil {
		return "unknown"
	}

	// convert []byte to string
	return string(u.Release[:])
}
