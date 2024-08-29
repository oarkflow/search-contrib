package main

import (
	"encoding/json"
	"fmt"
	"github.com/oarkflow/search-contrib/flydb"
	"time"

	"github.com/oarkflow/search"
	"github.com/oarkflow/search/lib"
)

func main() {
	bt := []byte(`{"query":"Evenchik"}`)
	var params search.Params
	json.Unmarshal(bt, &params)
	icds := lib.ReadFileAsMap("billing-providers.json")
	db, _ := search.New[map[string]any]()
	store, err := flydb.New[int64, map[string]any]("fts", 100)
	if err != nil {
		panic(err)
	}
	db.SetStorage(store)
	var startTime = time.Now()
	before := lib.Stats()
	db.InsertWithPool(icds, 3, 100)
	after := lib.Stats()
	fmt.Println(fmt.Sprintf("Usage: %dMB; Before: %dMB; After: %dMB", after-before, before, after))
	fmt.Println("Total Documents", db.DocumentLen())
	fmt.Println("Indexing took", time.Since(startTime))
	startTime = time.Now()
	s, err := db.Search(&params)
	if err != nil {
		panic(err)
	}
	fmt.Println("Searching took", time.Since(startTime), s.Message)
	fmt.Println(s.Total, s.Hits, s.Count)
}
