package stream

import (
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func makeRecord(oldStatus, newStatus, bitIndex string) events.DynamoDBEventRecord {
	return events.DynamoDBEventRecord{
		EventID: "test-event-id",
		Change: events.DynamoDBStreamRecord{
			OldImage: map[string]events.DynamoDBAttributeValue{
				"status":    events.NewStringAttribute(oldStatus),
				"bit_index": events.NewNumberAttribute(bitIndex),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"status":    events.NewStringAttribute(newStatus),
				"bit_index": events.NewNumberAttribute(bitIndex),
			},
		},
	}
}

func TestParse_ClaimedToRevoked(t *testing.T) {
	records := []events.DynamoDBEventRecord{
		makeRecord("CLAIMED", "REVOKED", "5"),
	}
	mutations := Parse(records)

	if len(mutations) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(mutations))
	}
	if mutations[0].BitIndex != 5 {
		t.Errorf("expected BitIndex=5, got %d", mutations[0].BitIndex)
	}
	if mutations[0].TargetBit != 1 {
		t.Errorf("expected TargetBit=1, got %d", mutations[0].TargetBit)
	}
}

func TestParse_ClaimedToFree(t *testing.T) {
	records := []events.DynamoDBEventRecord{
		makeRecord("CLAIMED", "FREE", "10"),
	}
	mutations := Parse(records)

	if len(mutations) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(mutations))
	}
	if mutations[0].TargetBit != 1 {
		t.Errorf("expected TargetBit=1 for FREE, got %d", mutations[0].TargetBit)
	}
}

func TestParse_FreeToClaimed(t *testing.T) {
	records := []events.DynamoDBEventRecord{
		makeRecord("FREE", "CLAIMED", "3"),
	}
	mutations := Parse(records)

	if len(mutations) != 1 {
		t.Fatalf("expected 1 mutation, got %d", len(mutations))
	}
	if mutations[0].TargetBit != 0 {
		t.Errorf("expected TargetBit=0 for CLAIMED, got %d", mutations[0].TargetBit)
	}
}

func TestParse_RevokedToFree_Skipped(t *testing.T) {
	records := []events.DynamoDBEventRecord{
		makeRecord("REVOKED", "FREE", "7"),
	}
	mutations := Parse(records)

	if len(mutations) != 0 {
		t.Errorf("expected REVOKED->FREE to be skipped, got %d mutations", len(mutations))
	}
}

func TestParse_MissingBitIndex_Skipped(t *testing.T) {
	record := events.DynamoDBEventRecord{
		EventID: "bad-record",
		Change: events.DynamoDBStreamRecord{
			OldImage: map[string]events.DynamoDBAttributeValue{
				"status": events.NewStringAttribute("CLAIMED"),
			},
			NewImage: map[string]events.DynamoDBAttributeValue{
				"status": events.NewStringAttribute("REVOKED"),
				
			},
		},
	}
	mutations := Parse([]events.DynamoDBEventRecord{record})

	if len(mutations) != 0 {
		t.Errorf("expected malformed record to be skipped, got %d mutations", len(mutations))
	}
}

func TestParse_MixedBatch(t *testing.T) {
	records := []events.DynamoDBEventRecord{
		makeRecord("CLAIMED", "REVOKED", "1"), 
		makeRecord("REVOKED", "FREE", "2"),     
		makeRecord("FREE", "CLAIMED", "3"),     
		makeRecord("CLAIMED", "FREE", "4"),     
	}
	mutations := Parse(records)

	if len(mutations) != 3 {
		t.Fatalf("expected 3 mutations, got %d", len(mutations))
	}

	
	if mutations[0].BitIndex != 1 || mutations[0].TargetBit != 1 {
		t.Errorf("mutation[0]: expected (1, 1), got (%d, %d)", mutations[0].BitIndex, mutations[0].TargetBit)
	}
	
	if mutations[1].BitIndex != 3 || mutations[1].TargetBit != 0 {
		t.Errorf("mutation[1]: expected (3, 0), got (%d, %d)", mutations[1].BitIndex, mutations[1].TargetBit)
	}

	if mutations[2].BitIndex != 4 || mutations[2].TargetBit != 1 {
		t.Errorf("mutation[2]: expected (4, 1), got (%d, %d)", mutations[2].BitIndex, mutations[2].TargetBit)
	}
}