package raft

import (
	"fmt"
	"time"
)

func commandToString(command interface{}) string {
	str := fmt.Sprintf("%v", command)

	if len(str) > 6 {
		return str[0:3] + "..."
	}
	return fmt.Sprintf("%6s", str)
}

func (rf *Raft) acceptVote(args *RequestVoteArgs) bool {

	lastLog := rf.lastEntry()

	if lastLog.Term == args.LastLogTerm {
		return args.LastLogIndex >= lastLog.Index
	} else {
		return args.LastLogTerm > lastLog.Term
	}
}

func (rf *Raft) addLogEntry(entry *LogEntry) int {
	rf.logUpdateLock.Lock()
	defer rf.logUpdateLock.Unlock()

	//fmt.Printf("%d %d\n", len(rf.logs), rf.lastIncludedIndex)
	index := rf.logLength()
	entry.Index = index
	rf.logs = append(rf.logs, *entry)
	rf.persist()

	for i := range rf.peers {
		if i != rf.me {
			go rf.sendLogEntry(i, entry)
		}
	}

	return index
}

func (rf *Raft) flushLog(commitIndex int) {
	for i := rf.applyIndex + 1; i <= commitIndex; i++ {
		ok, item := rf.entry(i)
		if !ok {
			return
		}
		rf.applyCh <- ApplyMsg{
			CommandValid:  true,
			Command:       item.Command,
			CommandIndex:  item.Index,
			SnapshotValid: false,
		}
		//rf.logger.Printf(dCommit, fmt.Sprintf("向applyCh输入数据 %+v", item))
		rf.applyIndex++
		rf.commitIndex = rf.applyIndex
	}
	//rf.persist()
}

//func (rf *Raft) binarySearch(start, end int) int {
//
//	n := len(rf.peers)
//	mid := (start + end) >> 1
//	med := mid + 1
//
//	return 1
//}
//
//func (rf *Raft) test(n, index int) bool {
//	count := 1
//
//	for _, d := range rf.peerInfos {
//		if d.index >= index {
//			count++
//			if count >= n {
//				return true
//			}
//		}
//	}
//
//	return false
//}

func (rf *Raft) entry(index int) (b bool, l LogEntry) {
	defer func() {
		//乐观认为可以直接获取，出现并发时直接返回false(小概率事件，可以依赖心跳补偿)
		if err := recover(); err != nil {
			rf.logger.Printf(dError, fmt.Sprintf("entry() err:%v", err))
			b = false
			l = LogEntry{}
		}
		if l.Index != index {
			rf.logger.Printf(dError, fmt.Sprintf("entry() err expIndex:%d ,but got:%d", index, l.Index))
			b = false
			l = LogEntry{}
		}
	}()
	actualIndex := rf.logIndex(index)
	if actualIndex < 0 || actualIndex >= len(rf.logs) {
		rf.logger.Printf(dError, fmt.Sprintf("entry() out of range [%d] with capacity [%d,%d]", index, rf.lastIncludedIndex, rf.logLength()-1))
		return false, LogEntry{Index: -1}
	}
	return true, rf.logs[actualIndex]
}

func (rf *Raft) lastEntry() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) logIndex(realIndex int) int {
	return realIndex - rf.lastIncludedIndex
}

//func (rf *Raft) setLog(log *LogEntry, index int) {
//	rf.logs[rf.logIndex(index)] = *log
//	rf.persist()
//}

func (rf *Raft) logLength() int {
	return rf.lastIncludedIndex + len(rf.logs)
}

func (rf *Raft) setTerm(term int32) {
	if rf.term != term {
		rf.term = term
		rf.persist()
	}
}

func (rf *Raft) lastTime() time.Time {
	return rf.lastCallTime
}

func (rf *Raft) updateLastTime() {
	rf.lastCallTime = time.Now()
}

func (rf *Raft) generateCoalesceLog(startIndex, server int) (bool, CoalesceSyncLogArgs) {

	rf.logUpdateLock.Lock()
	defer rf.logUpdateLock.Unlock()

	//保证发送的第一个日志是对方期望的
	peerIndex := rf.getPeerIndex(server)
	ok, firstLog := rf.entry(startIndex)
	if !ok {
		return false, CoalesceSyncLogArgs{}
	}

	if peerIndex+1 != firstLog.Index {
		rf.logger.Printf(dError, fmt.Sprintf("sendCoalesceSyncLog failed,ratf[%d] exp:%d ,bug first:%d", server, peerIndex+1, startIndex))
		return false, CoalesceSyncLogArgs{}
	}

	ok, preLog := rf.entry(startIndex - 1)
	if !ok {
		return false, CoalesceSyncLogArgs{}
	}

	req := CoalesceSyncLogArgs{Id: rf.me, Term: rf.term, Logs: []*RequestSyncLogArgs{{Index: firstLog.Index, Term: firstLog.Term, Command: firstLog.Command, PreLogTerm: preLog.Term}}}
	preLog = firstLog

	length := rf.logLength()
	for i := startIndex + 1; i < length; i++ {
		ok, log := rf.entry(i)
		if ok {
			req.Logs = append(req.Logs, &RequestSyncLogArgs{Index: log.Index, Term: log.Term, Command: log.Command, PreLogTerm: preLog.Term})
		} else {
			return false, CoalesceSyncLogArgs{}
		}
		preLog = log
	}

	return true, req
}
