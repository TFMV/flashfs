// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flashfs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type SnapshotT struct {
	Entries []*FileEntryT `json:"entries"`
}

func (t *SnapshotT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	entriesOffset := flatbuffers.UOffsetT(0)
	if t.Entries != nil {
		entriesLength := len(t.Entries)
		entriesOffsets := make([]flatbuffers.UOffsetT, entriesLength)
		for j := 0; j < entriesLength; j++ {
			entriesOffsets[j] = t.Entries[j].Pack(builder)
		}
		SnapshotStartEntriesVector(builder, entriesLength)
		for j := entriesLength - 1; j >= 0; j-- {
			builder.PrependUOffsetT(entriesOffsets[j])
		}
		entriesOffset = builder.EndVector(entriesLength)
	}
	SnapshotStart(builder)
	SnapshotAddEntries(builder, entriesOffset)
	return SnapshotEnd(builder)
}

func (rcv *Snapshot) UnPackTo(t *SnapshotT) {
	entriesLength := rcv.EntriesLength()
	t.Entries = make([]*FileEntryT, entriesLength)
	for j := 0; j < entriesLength; j++ {
		x := FileEntry{}
		rcv.Entries(&x, j)
		t.Entries[j] = x.UnPack()
	}
}

func (rcv *Snapshot) UnPack() *SnapshotT {
	if rcv == nil {
		return nil
	}
	t := &SnapshotT{}
	rcv.UnPackTo(t)
	return t
}

type Snapshot struct {
	_tab flatbuffers.Table
}

func GetRootAsSnapshot(buf []byte, offset flatbuffers.UOffsetT) *Snapshot {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Snapshot{}
	x.Init(buf, n+offset)
	return x
}

func FinishSnapshotBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsSnapshot(buf []byte, offset flatbuffers.UOffsetT) *Snapshot {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Snapshot{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedSnapshotBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Snapshot) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Snapshot) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Snapshot) Entries(obj *FileEntry, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Snapshot) EntriesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func SnapshotStart(builder *flatbuffers.Builder) {
	builder.StartObject(1)
}
func SnapshotAddEntries(builder *flatbuffers.Builder, entries flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(entries), 0)
}
func SnapshotStartEntriesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func SnapshotEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
