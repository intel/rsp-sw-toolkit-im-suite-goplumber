/* Apache v2 license
*  Copyright (C) <2019> Intel Corporation
*
*  SPDX-License-Identifier: Apache-2.0
 */

package goplumber

import (
	"github.impcloud.net/RSP-Inventory-Suite/expect"
	"testing"
)

func TestNewPlumber(t *testing.T) {
	w := expect.WrapT(t)
	p := NewPlumber()
	w.As("default tasks").ShouldContain(p.Clients,
		[]string{"http", "validation", "input"})
}
