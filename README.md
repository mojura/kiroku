# Kiroku [![GoDoc](https://godoc.org/github.com/mojura/kiroku?status.svg)](https://godoc.org/github.com/mojura/kiroku) ![Status](https://img.shields.io/badge/status-beta-yellow.svg) [![Go Report Card](https://goreportcard.com/badge/github.com/mojura/kiroku)](https://goreportcard.com/report/github.com/mojura/kiroku) ![Go Test Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)
Kiroku is a general purpose historical record system which utilizes data blocks. It was built to be used as the action persistence layer for Mojura.

## Usage
### New
```go
func ExampleNewProducer() {
	var err error
	if testProducer, err = NewProducer("./test_data", "tester", nil); err != nil {
		log.Fatal(err)
		return
	}
}
```

### Kiroku.Transaction
```go
func ExampleKiroku_Transaction() {
	var err error
	if err = testProducer.Transaction(func(t *Transaction) (err error) {
		return t.AddBlock(TypeWriteAction, []byte("hello world!"))
	}); err != nil {
		log.Fatal(err)
		return
	}
}
```

### Kiroku.Snapshot
```go
func ExampleKiroku_Snapshot() {
	var err error
	if err = testProducer.Snapshot(func(s *Snapshot) (err error) {
		return s.Write([]byte("hello world!"))
	}); err != nil {
		log.Fatal(err)
		return
	}
}
```

### NewWriter
```go
func ExampleNewWriter() {
	var err error
	if testWriter, err = NewWriter("./test_data", "testie"); err != nil {
		log.Fatal(err)
		return
	}
}
```

### Writer.AddBlock
```go
func ExampleWriter_AddBlock() {
	var err error
	if err = testWriter.AddBlock(TypeWriteAction, []byte("Hello world!")); err != nil {
		log.Fatalf("error adding row: %v", err)
		return
	}
}
```

### NewReader
```go
func ExampleNewReader() {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Open("filename.kir"); err != nil {
		log.Fatalf("error opening: %v", err)
		return
	}

	if testReader, err = NewReader(f); err != nil {
		log.Fatalf("error initializing reader: %v", err)
		return
	}
}
```

### Reader.Meta
```go
func ExampleReader_Meta() {
	var m Meta
	m = testReader.Meta()
	fmt.Println("Meta!", m)
}
```

### Reader.ForEach
```go
func ExampleReader_ForEach() {
	var err error
	if err = testReader.ForEach(0, func(b Block) (err error) {
		fmt.Println("Block data:", string(b.Value))
		return
	}); err != nil {
		log.Fatalf("Error iterating through blocks: %v", err)
	}
}
```

### Reader.Copy
```go
func ExampleReader_Copy() {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Create("chunk.copy.kir"); err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	if _, err = testReader.Copy(f); err != nil {
		log.Fatalf("Error copying chunk: %v", err)
	}
}
```

### Read
```go
func ExampleRead() {
	var err error
	if err = Read("filename.kir", func(r *Reader) (err error) {
		var m Meta
		m = testReader.Meta()
		fmt.Println("Meta!", m)

		if err = r.ForEach(0, func(b Block) (err error) {
			fmt.Println("Block data:", string(b.Value))
			return
		}); err != nil {
			log.Fatalf("Error iterating through blocks: %v", err)
		}

		return
	}); err != nil {
		log.Fatal(err)
		return
	}
}
```
