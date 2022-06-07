package middleware

import (
	"fmt"
	"github.com/bee-org/bee"
	"runtime"
)

type Middleware func(handler bee.Handler) bee.Handler

//RecoverPanic Catch the Panic and return an error
func RecoverPanic() Middleware {
	return func(handler bee.Handler) bee.Handler {
		return func(ctx *bee.Context) (err error) {
			// Catch panics
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 1024)
					buf = buf[:runtime.Stack(buf, false)]
					err = fmt.Errorf("%v\n%s", r, buf)
				}
			}()
			err = handler(ctx)
			return
		}
	}
}
