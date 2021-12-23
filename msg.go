package bee

import "time"

type Msg struct {
	Data  interface{}
	Delay time.Duration
}
