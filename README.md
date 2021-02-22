# Kiroku [![GoDoc](https://godoc.org/github.com/mojura/kiroku?status.svg)](https://godoc.org/github.com/mojura/kiroku) ![Status](https://img.shields.io/badge/status-beta-yellow.svg) 
Kiroku is a general purpose historical record system which utilizes data blocks. It was built to be used as the action persistence layer for Mojura.

## Usage
### New
```go
func ExampleNew() {
	var err error
	if testKiroku, err = New("./test_data", "tester", nil); err != nil {
		log.Fatal(err)
		return
	}
}
```

### New (with custom Processor)
```go
func ExampleNew_with_custom_Processor() {
	var err error
	pfn := func(r *Reader) (err error) {
		fmt.Println("Hello chunk!", r.Meta())
		return
	}

	if testKiroku, err = New("./test_data", "tester", pfn); err != nil {
		log.Fatal(err)
		return
	}
}
```

### Kiroku.Transaction
```go
func ExampleKiroku_Transaction() {
	var err error
	if err = testKiroku.Transaction(func(w *Writer) (err error) {
		w.SetIndex(1337)
		w.AddBlock(TypeWriteAction, []byte("hello world!"))
		return
	}); err != nil {
		log.Fatal(err)
		return
	}
}
```

### Writer.GetIndex
```go
func ExampleWriter_GetIndex() {
	var (
		index uint64
		err   error
	)

	if index, err = testWriter.GetIndex(); err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println("Current index:", index)
}
```

### Writer.SetIndex
```go
func ExampleWriter_SetIndex() {
	var err error
	if err = testWriter.SetIndex(1337); err != nil {
		log.Fatal(err)
	}
}
```

### Writer.NextIndex
```go
func ExampleWriter_NextIndex() {
	var (
		index uint64
		err   error
	)

	if index, err = testWriter.NextIndex(); err != nil {
		log.Fatal(err)
		return
	}

	fmt.Println("Next index:", index)
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

	if f, err = os.Open("filename.moj"); err != nil {
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
	if err = testReader.ForEach(0, func(b *Block) (err error) {
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

	if f, err = os.Create("chunk.copy.moj"); err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	if _, err = testReader.Copy(f); err != nil {
		log.Fatalf("Error copying chunk: %v", err)
	}
}
```

### Reader.CopyBlocks
```go
func ExampleReader_CopyBlocks() {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Create("chunk.blocksOnly.copy.moj"); err != nil {
		log.Fatal(err)
		return
	}
	defer f.Close()

	if _, err = testReader.CopyBlocks(f); err != nil {
		log.Fatalf("Error copying chunk: %v", err)
	}
}
```

### Read
```go
func ExampleRead() {
	var err error
	if err = Read("filename.moj", func(r *Reader) (err error) {
		var m Meta
		m = testReader.Meta()
		fmt.Println("Meta!", m)

		if err = r.ForEach(0, func(b *Block) (err error) {
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
