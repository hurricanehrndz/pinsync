//go:build unix

package manifest

import "syscall"

// openFlags makes verifyFile's open fail on a symlink at the final component
// and return at once on a FIFO instead of waiting for a writer.
const openFlags = syscall.O_NOFOLLOW | syscall.O_NONBLOCK
