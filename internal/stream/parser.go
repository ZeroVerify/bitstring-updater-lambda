package stream

import (
	"log"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
)

type BitMutation struct {
	BitIndex  int
	TargetBit int 
}

func Parse(records []events.DynamoDBEventRecord) []BitMutation {
	var mutations []BitMutation

	for _, r := range records {
		oldStatus := r.Change.OldImage["status"].String()
		newStatus := r.Change.NewImage["status"].String()

		if oldStatus == "REVOKED" && newStatus == "FREE" {
			log.Printf("SKIP: REVOKED->FREE transition at bit_index=%s, no S3 write needed",
				r.Change.NewImage["bit_index"].Number())
			continue
		}

		indexAttr, ok := r.Change.NewImage["bit_index"]
		if !ok {
			log.Printf("SKIP: record missing bit_index, eventID=%s", r.EventID)
			continue
		}

		idx, err := strconv.Atoi(indexAttr.Number())
		if err != nil {
			log.Printf("SKIP: invalid bit_index %q: %v", indexAttr.Number(), err)
			continue
		}

		bit := 1 // REVOKED or FREE → bit=1
		if newStatus == "CLAIMED" {
			bit = 0 // CLAIMED → bit=0
		}

		mutations = append(mutations, BitMutation{
			BitIndex:  idx,
			TargetBit: bit,
		})
	}

	return mutations
}