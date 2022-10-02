package lock

import (
	"6.824/log"
	"sync"
	"sync/atomic"
	"time"
)

var logger = log.Logger()

type Lock struct {
	key       *int32
	isTimeOut bool
	d         time.Duration
	timer     *time.Timer

	unlock bool
	msg    string
}

func (lo *Lock) Lock() bool {
	ok := atomic.CompareAndSwapInt32(lo.key, 0, 1)
	if ok {
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func() {
			lo.timer = time.NewTimer(lo.d)
			wg.Done()
			<-lo.timer.C
			lo.Unlock()
			lo.isTimeOut = true
		}()
		wg.Wait()
	}
	logger.Debugf("%s 上锁 %v", lo.msg, ok)
	return ok
}

func (lo *Lock) TimeOut() bool {
	return lo.isTimeOut
}

func (lo *Lock) Unlock() {
	if lo.TimeOut() || lo.unlock {
		return
	}
	lo.unlock = true
	lo.timer.Stop()
	*lo.key = int32(0)
	logger.Debugf("%s 解锁", lo.msg)
}

func (lo *Lock) Reset(d time.Duration) bool {
	return lo.timer.Reset(d)
}

func Make(key *int32, d time.Duration, msg string) *Lock {
	lock := Lock{key: key, isTimeOut: false, d: d, msg: msg, unlock: false}
	return &lock
}
