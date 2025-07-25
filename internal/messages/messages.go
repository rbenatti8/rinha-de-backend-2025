package messages

import (
	"github.com/anthdm/hollywood/actor"
	"time"
)

type Payment struct {
	CID         string  `json:"correlationId"`
	Amount      float64 `json:"amount"`
	RequestedAt string  `json:"requestedAt"`
}

type PushPayment struct {
	Payment     Payment
	ProcessedBy string
}

type ProcessPayment struct {
	Payment Payment
	Tries   int
}

type ScheduleRetry struct {
	Sender  *actor.PID
	Payment Payment
	Tries   int
	NextTry time.Time
}

type Retry struct {
}
type PaymentProcessorChanged struct {
	Processor string
}
