// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedded

import (
	"bytes"
	"strings"
	"syscall"
	"unsafe"
)

// F_GETPATH retrieves the path of the file associated with a file descriptor.
const darwinFGetPath = 50

const maxFDScan = 4096

func openFilesUnderDir(dir string) ([]string, bool, error) {
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err != nil {
		return nil, true, err
	}
	maxFD := int(rlim.Cur)
	if maxFD > maxFDScan {
		maxFD = maxFDScan
	}

	buf := make([]byte, 1024)
	var found []string
	for fd := 0; fd < maxFD; fd++ {
		_, _, errno := syscall.Syscall(syscall.SYS_FCNTL,
			uintptr(fd), darwinFGetPath, uintptr(unsafe.Pointer(&buf[0])))
		if errno != 0 {
			continue
		}
		n := bytes.IndexByte(buf, 0)
		if n < 0 {
			n = len(buf)
		}
		path := string(buf[:n])
		if strings.HasPrefix(path, dir) {
			found = append(found, path)
		}
	}
	return found, true, nil
}
