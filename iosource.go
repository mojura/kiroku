package kiroku

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var _ Source = &IOSource{}

func NewIOSource(dir string) (ip *IOSource, err error) {
	var i IOSource
	i.dir = path.Join(dir, "source")

	if err = os.MkdirAll(i.dir, 0744); err != nil {
		return
	}

	ip = &i
	return
}

type IOSource struct {
	dir string
}

func (i *IOSource) Export(ctx context.Context, prefix, filename string, r io.Reader) (newFilename string, err error) {
	var f *os.File
	dir := path.Join(i.dir, prefix)
	if err = os.MkdirAll(dir, 0744); err != nil {
		return
	}

	filepath := path.Join(dir, filename)
	if f, err = os.Create(filepath); err != nil {
		err = fmt.Errorf("error creating file <%s>: %v", filename, err)
		return
	}
	defer f.Close()

	if _, err = io.Copy(f, r); err != nil {
		return
	}

	newFilename = filename
	return
}

func (i *IOSource) Import(ctx context.Context, prefix, filename string, w io.Writer) (err error) {
	var f *os.File
	dir := path.Join(i.dir, prefix)
	filepath := path.Join(dir, filename)
	if f, err = os.Open(filepath); err != nil {
		err = os.ErrNotExist
		return
	}
	defer f.Close()

	if _, err = io.Copy(w, f); err != nil {
		return
	}

	return
}

func (i *IOSource) Get(ctx context.Context, prefix, filename string, fn func(io.Reader) error) (err error) {
	var f *os.File
	dir := path.Join(i.dir, prefix)
	filepath := path.Join(dir, filename)
	if f, err = os.Open(filepath); err != nil {
		err = os.ErrNotExist
		return
	}
	defer f.Close()

	return fn(f)
}

func (i *IOSource) GetNext(ctx context.Context, prefix, lastFilename string) (filename string, err error) {
	wfn := func(walkingFile string, info os.FileInfo, ierr error) (err error) {
		if ierr != nil {
			return
		}

		if info.IsDir() {
			return
		}

		walkingFile = filepath.Base(walkingFile)
		if walkingFile <= lastFilename {
			return
		}

		if strings.Index(walkingFile, prefix) != 0 {
			return
		}

		filename = walkingFile
		return errBreak
	}

	if err = filepath.Walk(i.dir, wfn); err == errBreak {
		return filename, nil
	}

	return filename, io.EOF
}

func (i *IOSource) GetNextList(ctx context.Context, prefix, lastFilename string, maxKeys int64) (filenames []string, err error) {
	wfn := func(walkingFile string, info os.FileInfo, ierr error) (err error) {
		if ierr != nil {
			return
		}

		if info.IsDir() {
			return
		}

		walkingFile = filepath.Base(walkingFile)
		if walkingFile <= lastFilename {
			return
		}

		if strings.Index(walkingFile, prefix) != 0 {
			return
		}

		filenames = append(filenames, walkingFile)

		if len(filenames) >= int(maxKeys) {
			return errBreak
		}

		return
	}

	dir := path.Join(i.dir, prefix)
	err = filepath.Walk(dir, wfn)
	switch {
	case len(filenames) == 0:
		return nil, io.EOF
	case err == nil || err == errBreak:
		return filenames, nil
	case len(filenames) == 0:
		return nil, io.EOF
	default:
		return nil, err
	}
}
