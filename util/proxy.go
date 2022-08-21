package util

import (
	"context"
	"reflect"

	"github.com/filecoin-project/lotus/metrics"
	"go.opencensus.io/tag"
)

// copy from "github.com/filecoin-project/lots/metrics/proxy.go"

func Proxy(in interface{}, out interface{}) {
	rint := reflect.ValueOf(out).Elem()
	ra := reflect.ValueOf(in)

	for f := 0; f < rint.NumField(); f++ {
		field := rint.Type().Field(f)
		fn := ra.MethodByName(field.Name)

		rint.Field(f).Set(reflect.MakeFunc(field.Type, func(args []reflect.Value) (results []reflect.Value) {
			ctx := args[0].Interface().(context.Context)
			// upsert function name into context
			ctx, _ = tag.New(ctx, tag.Upsert(metrics.Endpoint, field.Name))
			stop := metrics.Timer(ctx, metrics.APIRequestDuration)
			defer stop()
			// pass tagged ctx back into function call
			args[0] = reflect.ValueOf(ctx)
			return fn.Call(args)
		}))

	}
}
