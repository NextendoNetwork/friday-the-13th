package main

import (
	"fmt"
	"sync"
	"time"

	nex "github.com/NextendoNetwork/nextendo-nex"
)

// Scope failures to live connections, not accounts or broad NAT labels. A
// reconnect or a successful private retry permits automatic matching again.
type peerPair struct{ from, to uint32 }
type reachability struct {
	mu       sync.Mutex
	attempts map[peerPair]time.Time
	failed   map[peerPair]time.Time
}

func newReachability() *reachability {
	return &reachability{attempts: make(map[peerPair]time.Time), failed: make(map[peerPair]time.Time)}
}

func (r *reachability) allowed(a, b *nex.Connection) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for _, key := range []peerPair{{a.ID, b.ID}, {b.ID, a.ID}} {
		if until := r.failed[key]; now.Before(until) {
			return false
		}
		delete(r.failed, key)
	}
	return true
}

func (r *reachability) remove(id uint32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k := range r.attempts {
		if k.from == id || k.to == id {
			delete(r.attempts, k)
		}
	}
	for k := range r.failed {
		if k.from == id || k.to == id {
			delete(r.failed, k)
		}
	}
}

func (r *reachability) observe(c *nex.Connection, req *nex.RMCMessage, mm *nex.Matchmaking) {
	if req.Protocol != nex.ProtocolNATTraversal {
		return
	}
	in := nex.NewStreamIn(req.Body, c.Settings)
	now := time.Now()
	if req.Method == nex.MethodRequestProbeInitiationExt {
		n := in.U32()
		if n > 64 {
			return
		}
		var targets []uint32
		for i := uint32(0); i < n; i++ {
			u := nex.ParseStationURL(in.String())
			if u != nil {
				targets = append(targets, uint32(u.GetInt("RVCID")))
			}
		}
		_ = in.String()
		if in.Err() != nil {
			return
		}
		r.mu.Lock()
		for k, t := range r.attempts {
			if now.Sub(t) > time.Minute {
				delete(r.attempts, k)
			}
		}
		for k, until := range r.failed {
			if !now.Before(until) {
				delete(r.failed, k)
			}
		}
		for _, id := range targets {
			if id != 0 && id != c.ID {
				r.attempts[peerPair{c.ID, id}] = now
			}
		}
		r.mu.Unlock()
		return
	}
	if req.Method != nex.MethodReportNATTraversalResult && req.Method != nex.MethodReportNATTraversalResultDetail {
		return
	}
	id, success := in.U32(), in.Bool()
	if req.Method == nex.MethodReportNATTraversalResultDetail {
		_ = in.U32()
		_ = in.U32()
	}
	if in.Err() != nil || c.Endpoint.FindConnectionByID(id) == nil {
		return
	}
	key := peerPair{c.ID, id}
	r.mu.Lock()
	at, attempted := r.attempts[key]
	if !attempted || now.Sub(at) > time.Minute {
		r.mu.Unlock()
		return
	}
	delete(r.attempts, key)
	if success {
		delete(r.failed, key)
		delete(r.failed, peerPair{id, c.ID})
	} else {
		r.failed[key] = now.Add(10 * time.Minute)
	}
	r.mu.Unlock()
	if !success {
		mm.ReleaseFailedHostJoin(c, id)
		fmt.Printf("[Friday13 Matchmaking] pid=%d failed host connection=%d: released reservation; automatic pair cooldown=10m\n", c.PID, id)
	}
}
