// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package main

import (
	"cmp"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
	"golang.org/x/time/rate"
)

type accountant struct {
	name string
	rate float64
	mut  sync.Mutex
	cur  map[protocol.DeviceID]int
	prev map[protocol.DeviceID]int
	rls  map[protocol.DeviceID]*rate.Limiter
}

func (a *accountant) account(d *protocol.DeviceID) time.Duration {
	var wait time.Duration
	a.mut.Lock()
	a.cur[*d]++
	if a.cur[*d] > 1 {
		lim, ok := a.rls[*d]
		if !ok {
			lim = rate.NewLimiter(rate.Limit(a.rate), 1)
			a.rls[*d] = lim
		}
		wait = lim.Reserve().Delay()
	}
	a.mut.Unlock()
	return wait
}

func (a *accountant) rotate() {
	a.mut.Lock()
	a.prev = a.cur
	a.cur = make(map[protocol.DeviceID]int, len(a.prev))
	a.mut.Unlock()

	if len(a.prev) > 10 {
		es := make([]entry, 0, len(a.prev))
		for k, v := range a.prev {
			es = append(es, entry{d: k, c: v})
		}
		slices.SortFunc(es, func(a, b entry) int { return -cmp.Compare(a.c, b.c) })
		slog.Info("Accountant aggregation", "name", a.name, "count", len(es), "median", es[len(es)/2].c, "95%", es[len(es)/20].c, "max", es[0].c)
		slog.Info("Accountant leaders", "name", a.name, "top10", es[:10])
	} else {
		slog.Info("Accountant aggregation", "name", a.name, "count", len(a.prev))
	}
}

type entry struct {
	d protocol.DeviceID
	c int
}

func (e entry) String() string {
	return fmt.Sprintf("%s:%d", e.d, e.c)
}
