package kiroku

import "sync"

type filenames struct {
	mux sync.Mutex

	s []string
}

func (f *filenames) Shift() (filename string, ok bool) {
	f.mux.Lock()
	defer f.mux.Unlock()
	if len(f.s) == 0 {
		return
	}

	filename = f.s[0]
	ok = true
	f.s = f.s[1:]
	return
}

func (f *filenames) Append(filenames []string) {
	f.mux.Lock()
	defer f.mux.Unlock()
	f.s = append(f.s, filenames...)
}
