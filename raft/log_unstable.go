// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import pb "go.etcd.io/etcd/raft/v3/raftpb"

// unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.
//unstable 使用内存数据维护其中所有的entry记录，对于leader节点，它维护了客户端请求对应的entry记录
// 对于follower节点，它维护了从leader节点复制来的entry记录，无乱是leader节点还是follower节点，对于刚刚接收
//的entry记录都会先存储在unstable中。然后按照raft协议将unstable中缓存的entry记录交给上层模块处理
//上层模块会将这些entry记录发送到集群其他节点或者写入storage中。之后，上层模块会调用Advance()方法通知底层raft模块将unstable中对应的entry记录删除
type unstable struct {
	// the incoming unstable snapshot, if any.
	snapshot *pb.Snapshot //快照，该快照也是没有写入storage中的。
	// all entries that have not yet been written to storage.
	entries []pb.Entry //保存未写入storage中的entry记录
	offset  uint64     //第一条entry记录的索引值

	logger Logger
}

// maybeFirstIndex returns the index of the first possible entry in entries
// if it has a snapshot.尝试获取unstable第一条entry记录的索引值
func (u *unstable) maybeFirstIndex() (uint64, bool) {
	//tip: 没有直接返回entry数组第一条记录的值，主要与raftLog.firstIndex方法相关
	if u.snapshot != nil {
		//如果snapshot不为nil,通过snapshot元数据返回相应的索引值
		return u.snapshot.Metadata.Index + 1, true
	}
	return 0, false
}

// maybeLastIndex returns the last index if it has at least one
// unstable entry or snapshot.尝试获取unstable最后一条entry记录的索引值
func (u *unstable) maybeLastIndex() (uint64, bool) {
	//如果entry数组中有记录，则返回entry数组中最条一条记录的索引值
	if l := len(u.entries); l != 0 {
		return u.offset + uint64(l) - 1, true
	}
	//如果entry数组中没有记录，则通过读取快照元数据返回索引值
	if u.snapshot != nil {
		return u.snapshot.Metadata.Index, true
	}
	return 0, false
}

// maybeTerm returns the term of the entry at index i, if there
// is any. 获取指定索引的term
func (u *unstable) maybeTerm(i uint64) (uint64, bool) {
	if i < u.offset {
		if u.snapshot != nil && u.snapshot.Metadata.Index == i {
			return u.snapshot.Metadata.Term, true
		}
		return 0, false
	}

	last, ok := u.maybeLastIndex()
	if !ok {
		return 0, false
	}
	if i > last {
		return 0, false
	}

	return u.entries[i-u.offset].Term, true
}

//当unstable中的entry记录写入storage，会调用stableTo方法清除entry数组中的记录
func (u *unstable) stableTo(i, t uint64) {
	gt, ok := u.maybeTerm(i)
	if !ok {
		return
	}
	// if i < offset, term is matched with the snapshot
	// only update the unstable entries if term is matched with
	// an unstable entry.
	if gt == t && i >= u.offset {
		u.entries = u.entries[i+1-u.offset:]
		u.offset = i + 1
		//随着多次追加日志和截断日志的操作,unstable.entires 底层的数组会越来越大，
		//shrinkEntriesArray()方法会在底层数组长度超过实际占用的两倍时，对底层数组进行缩减
		u.shrinkEntriesArray()
	}
}

// shrinkEntriesArray discards the underlying array used by the entries slice
// if most of it isn't being used. This avoids holding references to a bunch of
// potentially large entries that aren't needed anymore. Simply clearing the
// entries wouldn't be safe because clients might still be using them.
//shrinkEntriesArray 缩减底层数组的长度
func (u *unstable) shrinkEntriesArray() {
	// We replace the array if we're using less than half of the space in
	// it. This number is fairly arbitrary, chosen as an attempt to balance
	// memory usage vs number of allocations. It could probably be improved
	// with some focused tuning.
	const lenMultiple = 2
	if len(u.entries) == 0 {
		u.entries = nil
	} else if len(u.entries)*lenMultiple < cap(u.entries) {
		newEntries := make([]pb.Entry, len(u.entries))
		copy(newEntries, u.entries)
		u.entries = newEntries
	}
}

//当unstable.snapshot字段指向的快照被写入storage之后，会调用stableSnapTo()方法将snapshot字段清空
func (u *unstable) stableSnapTo(i uint64) {
	if u.snapshot != nil && u.snapshot.Metadata.Index == i {
		u.snapshot = nil
	}
}

func (u *unstable) restore(s pb.Snapshot) {
	u.offset = s.Metadata.Index + 1
	u.entries = nil
	u.snapshot = &s
}

//truncateAndAppend 向unstable.entries中追加Entry记录
func (u *unstable) truncateAndAppend(ents []pb.Entry) {
	after := ents[0].Index
	switch {
	case after == u.offset+uint64(len(u.entries)):
		// after is the next index in the u.entries
		// directly append
		u.entries = append(u.entries, ents...)
	case after <= u.offset:
		u.logger.Infof("replace the unstable entries from index %d", after)
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		u.offset = after
		u.entries = ents
	default:
		// truncate to after and copy to u.entries
		// then append
		u.logger.Infof("truncate the unstable entries before index %d", after)
		u.entries = append([]pb.Entry{}, u.slice(u.offset, after)...)
		u.entries = append(u.entries, ents...)
	}
}

func (u *unstable) slice(lo uint64, hi uint64) []pb.Entry {
	u.mustCheckOutOfBounds(lo, hi)
	return u.entries[lo-u.offset : hi-u.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (u *unstable) mustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		u.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := u.offset + uint64(len(u.entries))
	if lo < u.offset || hi > upper {
		u.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, u.offset, upper)
	}
}
