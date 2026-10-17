package pull

import "syscall"

// soleName reports whether p, not followed if a reparse point, is its file's
// only name. The handle asks for no access and shares everything, so it
// neither changes the file nor stops anyone else using it.
func soleName(p string) (bool, error) {
	name, err := syscall.UTF16PtrFromString(p)
	if err != nil {
		return false, err
	}
	h, err := syscall.CreateFile(name, 0,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE, nil,
		syscall.OPEN_EXISTING, syscall.FILE_FLAG_BACKUP_SEMANTICS|syscall.FILE_FLAG_OPEN_REPARSE_POINT, 0)
	if err != nil {
		return false, err
	}
	defer func() { _ = syscall.CloseHandle(h) }() // no access; nothing to flush
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(h, &info); err != nil {
		return false, err
	}
	return info.NumberOfLinks == 1, nil
}
