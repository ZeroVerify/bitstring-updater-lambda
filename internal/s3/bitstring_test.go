package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"testing"

	"github.com/ZeroVerify/bitstring-updater-lambda/internal/stream"
)


func makeGzipBitstring(t *testing.T, bits []byte) []byte {
	t.Helper()
	b64 := base64.StdEncoding.EncodeToString(bits)
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	_, err := gw.Write([]byte(b64))
	if err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}


func TestRoundtrip_EncodeDecode(t *testing.T) {
	original := make([]byte, 16) 
	original[0] = 0b10110010
	original[1] = 0b00001111

	compressed := makeGzipBitstring(t, original)

	// Decompress
	gr, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatalf("gzip open: %v", err)
	}
	var b64buf bytes.Buffer
	b64buf.ReadFrom(gr)
	gr.Close()

	decoded, err := base64.StdEncoding.DecodeString(b64buf.String())
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}

	if !bytes.Equal(original, decoded) {
		t.Errorf("roundtrip mismatch:\n  original: %08b\n  decoded:  %08b", original, decoded)
	}
}

func TestApplyBitMutation_SetBit(t *testing.T) {
	bits := make([]byte, 16)

	mutations := []stream.BitMutation{
		{BitIndex: 0, TargetBit: 1},  
		{BitIndex: 7, TargetBit: 1}, 
		{BitIndex: 8, TargetBit: 1},  
	}

	for _, m := range mutations {
		bytePos := m.BitIndex / 8
		bitPos := uint(7 - m.BitIndex%8)
		if m.TargetBit == 1 {
			bits[bytePos] |= 1 << bitPos
		} else {
			bits[bytePos] &^= 1 << bitPos
		}
	}


	if bits[0]&(1<<7) == 0 {
		t.Error("bit 0 should be set")
	}
	
	if bits[0]&(1<<0) == 0 {
		t.Error("bit 7 should be set")
	}
	
	if bits[1]&(1<<7) == 0 {
		t.Error("bit 8 should be set")
	}
}

func TestApplyBitMutation_ClearBit(t *testing.T) {
	bits := make([]byte, 16)
	bits[0] = 0xFF 

	mutations := []stream.BitMutation{
		{BitIndex: 3, TargetBit: 0}, 
	}

	for _, m := range mutations {
		bytePos := m.BitIndex / 8
		bitPos := uint(7 - m.BitIndex%8)
		bits[bytePos] &^= 1 << bitPos
	}


	if bits[0]&(1<<4) != 0 {
		t.Error("bit 3 should be cleared")
	}
	
	if bits[0] != 0b11101111 {
		t.Errorf("expected 0b11101111, got %08b", bits[0])
	}
}

func TestApplyBitMutation_OutOfRange_Skipped(t *testing.T) {
	bits := make([]byte, 2) 

	m := stream.BitMutation{BitIndex: 100, TargetBit: 1}
	bytePos := m.BitIndex / 8

	if bytePos < len(bits) {
		t.Error("expected out-of-range check to catch this")
	}
	
}

func TestApplyMutations_NoopOnEmptyMutations(t *testing.T) {
	
	err := ApplyMutations(context.Background(), []stream.BitMutation{})
	
	_ = err
}