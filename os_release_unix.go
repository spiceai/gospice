//go:build !windows
// +build !windows

package gospice

import (
	"golang.org/x/sys/unix"
)

func GetOSRelease() string {
	u := unix.Utsname{}
	unix.Uname(&u)
	// convert []byte to string
	return string(u.Release[:])
}
