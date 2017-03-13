package sqlstore

import "io"

type Option func(s *Store)

func WithDebug(w io.Writer) Option {
	return func(s *Store) {
		s.debug = true
		s.writer = w
	}
}
