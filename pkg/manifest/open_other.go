//go:build !unix

package manifest

// openFlags adds nothing outside Unix.
// CEILING: on Windows a reparse point (symlink or junction) at the final
// component is caught only by the Lstat type check and the handle identity
// comparison in verifyFile, so one swapped in between them to the same file
// would pass. A no-follow open there needs CreateFile with
// FILE_FLAG_OPEN_REPARSE_POINT.
const openFlags = 0
