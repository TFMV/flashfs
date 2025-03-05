// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flashfs

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type DiffEntry struct {
	_tab flatbuffers.Table
}

func GetRootAsDiffEntry(buf []byte, offset flatbuffers.UOffsetT) *DiffEntry {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &DiffEntry{}
	x.Init(buf, n+offset)
	return x
}

func FinishDiffEntryBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsDiffEntry(buf []byte, offset flatbuffers.UOffsetT) *DiffEntry {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &DiffEntry{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedDiffEntryBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *DiffEntry) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *DiffEntry) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *DiffEntry) Path() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *DiffEntry) Type() int8 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetInt8(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateType(n int8) bool {
	return rcv._tab.MutateInt8Slot(6, n)
}

func (rcv *DiffEntry) OldSize() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateOldSize(n int64) bool {
	return rcv._tab.MutateInt64Slot(8, n)
}

func (rcv *DiffEntry) NewSize() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateNewSize(n int64) bool {
	return rcv._tab.MutateInt64Slot(10, n)
}

func (rcv *DiffEntry) OldMtime() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateOldMtime(n int64) bool {
	return rcv._tab.MutateInt64Slot(12, n)
}

func (rcv *DiffEntry) NewMtime() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(14))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateNewMtime(n int64) bool {
	return rcv._tab.MutateInt64Slot(14, n)
}

func (rcv *DiffEntry) OldPermissions() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(16))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateOldPermissions(n uint32) bool {
	return rcv._tab.MutateUint32Slot(16, n)
}

func (rcv *DiffEntry) NewPermissions() uint32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(18))
	if o != 0 {
		return rcv._tab.GetUint32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *DiffEntry) MutateNewPermissions(n uint32) bool {
	return rcv._tab.MutateUint32Slot(18, n)
}

func (rcv *DiffEntry) OldHash(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *DiffEntry) OldHashLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *DiffEntry) OldHashBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *DiffEntry) MutateOldHash(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(20))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func (rcv *DiffEntry) NewHash(j int) byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(22))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetByte(a + flatbuffers.UOffsetT(j*1))
	}
	return 0
}

func (rcv *DiffEntry) NewHashLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(22))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *DiffEntry) NewHashBytes() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(22))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *DiffEntry) MutateNewHash(j int, n byte) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(22))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateByte(a+flatbuffers.UOffsetT(j*1), n)
	}
	return false
}

func DiffEntryStart(builder *flatbuffers.Builder) {
	builder.StartObject(10)
}
func DiffEntryAddPath(builder *flatbuffers.Builder, path flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(path), 0)
}
func DiffEntryAddType(builder *flatbuffers.Builder, type_ int8) {
	builder.PrependInt8Slot(1, type_, 0)
}
func DiffEntryAddOldSize(builder *flatbuffers.Builder, oldSize int64) {
	builder.PrependInt64Slot(2, oldSize, 0)
}
func DiffEntryAddNewSize(builder *flatbuffers.Builder, newSize int64) {
	builder.PrependInt64Slot(3, newSize, 0)
}
func DiffEntryAddOldMtime(builder *flatbuffers.Builder, oldMtime int64) {
	builder.PrependInt64Slot(4, oldMtime, 0)
}
func DiffEntryAddNewMtime(builder *flatbuffers.Builder, newMtime int64) {
	builder.PrependInt64Slot(5, newMtime, 0)
}
func DiffEntryAddOldPermissions(builder *flatbuffers.Builder, oldPermissions uint32) {
	builder.PrependUint32Slot(6, oldPermissions, 0)
}
func DiffEntryAddNewPermissions(builder *flatbuffers.Builder, newPermissions uint32) {
	builder.PrependUint32Slot(7, newPermissions, 0)
}
func DiffEntryAddOldHash(builder *flatbuffers.Builder, oldHash flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(8, flatbuffers.UOffsetT(oldHash), 0)
}
func DiffEntryStartOldHashVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func DiffEntryAddNewHash(builder *flatbuffers.Builder, newHash flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(9, flatbuffers.UOffsetT(newHash), 0)
}
func DiffEntryStartNewHashVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(1, numElems, 1)
}
func DiffEntryEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
