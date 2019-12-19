/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package goplumber

import (
	"github.com/intel/rsp-sw-toolkit-im-suite-expect"
	"testing"
)

func TestNewPlumber(t *testing.T) {
	w := expect.WrapT(t)
	p := NewPlumber()
	w.As("default tasks").ShouldContain(p.Clients,
		[]string{"http", "validation", "input"})
}
