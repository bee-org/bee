package middleware

import "github.com/fanjindong/bee"

type Middleware func(handler bee.Handler) bee.Handler
