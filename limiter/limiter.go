/*
 * gorate
 * For the full copyright and license information, please view the LICENSE.txt file.
 */

// Package limiter provides a rate limiter
package limiter

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/time/rate"
)

// Options represents the options that can be set when creating a new limiter
type Options struct {
	// Concurrency level
	Concurrency uint32
	// Limit is the limit for the total number of queries
	Limit uint32
	// QPS is the limit for the number of queries per second
	QPS uint32
	// Duration is the limit for making queries
	Duration time.Duration
	// Callback is the function that is invoked on every query
	Callback func(cbp CallbackParams) error
	// SignalHandler enables the signal handler
	SignalHandler bool
}

// CallbackParams represents the callback function parameters
type CallbackParams struct {
	// Limiter is the limiter
	Limiter *Limiter
	// GroupID is the id for the concurrency group
	GroupID int
}

// New creates a new limiter by the given options
func New(o Options) (*Limiter, error) {
	// Init the limiter
	limiter := Limiter{
		concurrency:   o.Concurrency,
		limit:         o.Limit,
		qps:           o.QPS,
		duration:      o.Duration,
		callback:      o.Callback,
		signalHandler: o.SignalHandler,
	}

	// Check the options
	if o.Limit > 0 && o.Limit < o.Concurrency {
		return nil, errors.New("limit value must be greater than concurrency value")
	} else if o.Limit == 0 && o.Duration == 0 {
		return nil, errors.New("set either limit or duration value")
	}

	return &limiter, nil
}

// Limiter represents a limiter
type Limiter struct {
	concurrency     uint32
	limit           uint32
	qps             uint32
	duration        time.Duration
	callback        func(cbp CallbackParams) error
	signalHandler   bool
	lim             *rate.Limiter
	limContext      context.Context
	limCancelFunc   context.CancelFunc
	counters        []uint32
	wg              sync.WaitGroup
	start           time.Time
	since           time.Duration
	done            bool
	lastError       error
	isDeadline      bool
	isCanceled      bool
	isQueryLimit    bool
	isRateError     bool
	isCallbackError bool
}

// Run runs the limiter
func (limiter *Limiter) Run() error {
	// Context
	if limiter.duration > 0 {
		limiter.limContext, limiter.limCancelFunc = context.WithTimeout(context.Background(), limiter.duration)
	} else {
		limiter.limContext, limiter.limCancelFunc = context.WithCancel(context.Background())
	}
	defer limiter.limCancelFunc()

	// Singal handling
	if limiter.signalHandler {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		go func() {
			<-ch
			limiter.limCancelFunc()
		}()
	}

	// Wait group
	limiter.wg.Add(int(limiter.concurrency))

	// Limiter
	limiter.start = time.Now()
	if limiter.qps > 0 {
		limiter.lim = rate.NewLimiter(rate.Limit(float64(limiter.qps)), 1) // burst should be 1
	} else {
		limiter.lim = rate.NewLimiter(rate.Inf, 0)
	}

	// Concurrency loop
	l := int(limiter.concurrency) + 1
	limiter.counters = make([]uint32, l)
	for i := 1; i < l; i++ {
		go func(i int) {
			// Request loop
			for {
				// Limiter
				err := limiter.lim.Wait(limiter.limContext)
				if err != nil {
					if err == context.DeadlineExceeded || strings.Contains(err.Error(), "context deadline") {
						limiter.isDeadline = true
					} else if err == context.Canceled {
						limiter.isCanceled = true
					} else {
						limiter.isRateError = true
						limiter.lastError = err
					}
					limiter.wg.Done()
					break
				}
				// Check the query limit
				if limiter.limit > 0 && atomic.LoadUint32(&limiter.counters[0]) >= limiter.limit {
					limiter.isQueryLimit = true
					limiter.wg.Done()
					return
				}

				// Update counters
				atomic.AddUint32(&limiter.counters[i], 1)
				atomic.AddUint32(&limiter.counters[0], 1) // total

				// Callback
				if limiter.callback != nil {
					cbp := CallbackParams{Limiter: limiter, GroupID: i}
					if err := limiter.callback(cbp); err != nil {
						limiter.isCallbackError = true
						limiter.lastError = err
						limiter.wg.Done()
						break
					}
				}
			}
		}(i)
	}
	limiter.wg.Wait()
	limiter.since = time.Since(limiter.start)
	limiter.done = true

	return limiter.lastError
}

// Context returns the context
func (limiter *Limiter) Context() context.Context {
	return limiter.limContext
}

// CancelFunc returns the cancel function
func (limiter *Limiter) CancelFunc() context.CancelFunc {
	return limiter.limCancelFunc
}

// Since returns the since value
func (limiter *Limiter) Since() time.Duration {
	if limiter.done {
		return limiter.since
	}
	return time.Since(limiter.start)
}

// NumOfQueries returns the number of queries
func (limiter *Limiter) NumOfQueries() int {
	return int(atomic.LoadUint32(&limiter.counters[0]))
}

// NumOfQueriesByGroupID returns the number of queries by the given group id
func (limiter *Limiter) NumOfQueriesByGroupID(id int) int {
	if id > 0 && id < len(limiter.counters) {
		return int(atomic.LoadUint32(&limiter.counters[id]))
	}
	return 0
}

// LastError returns the last error
func (limiter *Limiter) LastError() error {
	return limiter.lastError
}

// IsDeadline returns whether the limiter reached deadline
func (limiter *Limiter) IsDeadline() bool {
	return limiter.isDeadline
}

// IsCanceled returns whether the limiter is interupted
func (limiter *Limiter) IsCanceled() bool {
	return limiter.isCanceled
}

// IsQueryLimit returns whether the limiter reached query limit
func (limiter *Limiter) IsQueryLimit() bool {
	return limiter.isQueryLimit
}

// IsRateError returns whether the limiter had a rate error
func (limiter *Limiter) IsRateError() bool {
	return limiter.isRateError
}

// IsCallbackError returns whether the limiter had a rate error
func (limiter *Limiter) IsCallbackError() bool {
	return limiter.isCallbackError
}
