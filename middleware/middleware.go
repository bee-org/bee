package middleware

import "github.com/bee-org/bee"

type Middleware func(handler bee.Handler) bee.Handler
