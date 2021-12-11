package test

import (
	"fmt"
	"query-engine/datatypes"
	"query-engine/execution"
	. "query-engine/logicalplan"
	"query-engine/physicalplan"
	"testing"
	"time"
)

func Test_execution_on_large_datasets(t *testing.T) {
	t.SkipNow()

	ctx := execution.NewCtx()
	csv := ctx.CSV("./yellow_tripdata_2019-01.csv")
	groupExpr := []LogicalExpr{NewCol("passenger_count")}
	aggExpr := []AggregateExpr{NewMax(NewCast(NewCol("fare_amount"), datatypes.FloatType))}
	df := csv.Aggregate(groupExpr, aggExpr)

	originalLogicalPlan := df.LogicalPlan()
	fmt.Printf("Logical Plan:\t%s\n", PrettyFormat(originalLogicalPlan))

	ctx.Plan(df.LogicalPlan())
	fmt.Printf("Optimized Physical Plan:\t%s\n", physicalplan.PrettyFormat(ctx.PhysicalPlan))

	start := time.Now()

	for ctx.Next() {
		recordBatch := ctx.Execute()
		fmt.Printf("Schema: %s\n", recordBatch.Schema)
		fmt.Printf("CSV:\n%s\n", recordBatch.ToCSV())
	}

	end := time.Now()

	// Query took 18.084590897s
	fmt.Printf("Query took %s\n", end.Sub(start))
}
