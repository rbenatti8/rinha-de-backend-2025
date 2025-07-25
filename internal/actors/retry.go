package actors

import (
	"github.com/anthdm/hollywood/actor"
	"github.com/rbenatti8/rinha-de-backend-2025/internal/messages"
	"time"
)

type RetryActor struct {
	heap      *RetryHeap
	repeater  actor.SendRepeater
	engine    *actor.Engine
	retryTime time.Duration
}

func (r *RetryActor) Receive(c *actor.Context) {
	switch msg := c.Message().(type) {
	case actor.Started:
		r.engine = c.Engine()
		r.repeater = c.SendRepeat(c.PID(), messages.Retry{}, r.retryTime)
	case messages.ScheduleRetry:
		r.heap.Push(RetryItem{
			Sender:  msg.Sender,
			Payment: msg.Payment,
			Tries:   msg.Tries,
			NextTry: msg.NextTry,
		})
	case messages.Retry:
		now := time.Now().UTC()

		for {
			item, ok := r.heap.Peek()
			if !ok || item.NextTry.After(now) {
				break
			}

			item, _ = r.heap.Pop()
			r.engine.Send(item.Sender, messages.ProcessPayment{
				Payment: item.Payment,
				Tries:   item.Tries + 1,
			})
		}
	}
}

type RetryItem struct {
	Sender  *actor.PID
	Payment messages.Payment
	NextTry time.Time
	Tries   int
}

type RetryHeap struct {
	items []RetryItem
}

func (h *RetryHeap) Len() int {
	return len(h.items)
}

func (h *RetryHeap) Push(item RetryItem) {
	h.items = append(h.items, item)
	h.up(h.Len() - 1)
}

func (h *RetryHeap) Pop() (RetryItem, bool) {
	n := h.Len()
	if n == 0 {
		return RetryItem{}, false
	}
	top := h.items[0]
	last := h.items[n-1]
	h.items = h.items[:n-1]
	if n > 1 {
		h.items[0] = last
		h.down(0)
	}
	return top, true
}

func (h *RetryHeap) Peek() (RetryItem, bool) {
	if h.Len() == 0 {
		return RetryItem{}, false
	}
	return h.items[0], true
}

func (h *RetryHeap) up(i int) {
	for {
		parent := (i - 1) / 2
		if i == 0 || !h.less(i, parent) {
			break
		}
		h.swap(i, parent)
		i = parent
	}
}

func (h *RetryHeap) down(i int) {
	n := h.Len()
	for {
		left := 2*i + 1
		right := 2*i + 2
		smallest := i

		if left < n && h.less(left, smallest) {
			smallest = left
		}
		if right < n && h.less(right, smallest) {
			smallest = right
		}
		if smallest == i {
			break
		}
		h.swap(i, smallest)
		i = smallest
	}
}

func (h *RetryHeap) less(i, j int) bool {
	return h.items[i].NextTry.Before(h.items[j].NextTry)
}

func (h *RetryHeap) swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}
