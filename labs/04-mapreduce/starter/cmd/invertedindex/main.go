package main

import (
        "cs452/internal/mapreduce"
        "fmt"
        "os"
        "strings"
        "unicode"
        "path/filepath"
        "sort"
)

// mapF produces (word, document) pairs for each unique word in a document.
func mapF(document string, value string) (res []mapreduce.KeyValue) {
        words := strings.FieldsFunc(value, func(r rune) bool {
                return !unicode.IsLetter(r)
        })

        uniqueWords := make(map[string]bool)
        for _, w := range words {
                if w != "" {
                        uniqueWords[w] = true
                }
        }

        res = make([]mapreduce.KeyValue, 0, len(uniqueWords))
        for w := range uniqueWords {
                res = append(res,mapreduce.KeyValue{Key: w, Value: filepath.Base(document)})
        }
        return res
}

// reduceF aggregates all documents containing a given word.
func reduceF(key string, values []string) string {
        docSet := make(map[string]bool)
        for _, doc := range values {
                docSet[doc] = true
        }

        docs := make([]string, 0, len(docSet))
        for doc := range docSet {
                docs = append(docs, doc)
        }
        sort.Strings(docs)

        return fmt.Sprintf("%d %s", len(docs), strings.Join(docs, ","))
}

func main() {
        if len(os.Args) < 4 {
                fmt.Printf("%s: see usage comments in file\n", os.Args[0])
        } else if os.Args[1] == "master" {
                var mr *mapreduce.Master
                if os.Args[2] == "sequential" {
                        mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
                } else {
                        mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
                }
                mr.Wait()
        } else {
                mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
        }
}

