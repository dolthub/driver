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
	"os"
	"path/filepath"
	"strings"
)

func openFilesUnderDir(dir string) ([]string, bool, error) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil, true, err
	}
	var found []string
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			// fd may have closed between ReadDir and Readlink
			continue
		}
		if strings.HasPrefix(target, dir) {
			found = append(found, target)
		}
	}
	return found, true, nil
}
