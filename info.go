package kiroku

type Info struct {
	Key          string `json:"key"`
	Hash         string `json:"hash"`
	Size         int64  `json:"size"`
	LastModified int64  `json:"lastModified"`
}
