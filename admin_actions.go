package worker

import "sync/atomic"

type adminActionCounters struct {
	pause  atomic.Int64
	resume atomic.Int64
	replay atomic.Int64
}

func (c *adminActionCounters) snapshot() AdminActionCounters {
	if c == nil {
		return AdminActionCounters{}
	}

	return AdminActionCounters{
		Pause:  c.pause.Load(),
		Resume: c.resume.Load(),
		Replay: c.replay.Load(),
	}
}

func (c *adminActionCounters) incPause() {
	if c != nil {
		c.pause.Add(1)
	}
}

func (c *adminActionCounters) incResume() {
	if c != nil {
		c.resume.Add(1)
	}
}

func (c *adminActionCounters) incReplay() {
	if c != nil {
		c.replay.Add(1)
	}
}
