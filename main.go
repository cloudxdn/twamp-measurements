package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/elastic/go-elasticsearch/esapi"
	elasticsearch "github.com/elastic/go-elasticsearch/v8"
	"github.com/fsnotify/fsnotify"
	"github.com/joho/godotenv"
)

type TwampRecord struct {
	SessionID       int    `json:"session_id"`
	SourcePort      int    `json:"source_port"`
	DestinationPort int    `json:"destination_port"`
	Interval        int    `json:"interval"`
	PacketRate      int    `json:"packet_rate"`
	PacketSize      int    `json:"packet_size"`
	StatRound       int    `json:"stat_round"`
	IntervalMs      int    `json:"interval_ms"`
	SyncStatus      int    `json:"sync_status"`
	Timestamp       string `json:"@timestamp"`
	AlarmID         string `json:"alarmid"`
	// other fields as needed
}

func main() {
	err := godotenv.Load(".env")

	if err != nil {
		log.Fatal("Error loading .env file")
	}

	directoryPath := os.Getenv("FILE_PATH")
	esURL := os.Getenv("ES_SERVER")
	esUser := os.Getenv("ES_USER")
	esPassword := os.Getenv("ES_PASSWORD")

	cfg := elasticsearch.Config{
		Addresses: []string{esURL},
		Username:  esUser,
		Password:  esPassword,
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}

	es, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Printf("Error createing Elasticsearch client: %s", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal("Watcher 생성 에러: ", err)
	}

	defer watcher.Close()

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create && strings.HasSuffix(event.Name, ".gz") {
					fmt.Println("New .gz file detected:", event.Name)
					processGzipFile(es, event.Name)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Println("Error:", err)
			}
		}
	}()

	// 디렉토리 감시 시작
	err = watcher.Add(directoryPath)
	if err != nil {
		log.Fatal(err)
	}

	// 프로그램이 종료되지 않도록 블록
	select {}
}

func processGzipFile(es *elasticsearch.Client, filePath string) {
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Fatal(err)
	}
	defer gz.Close()

	reader := csv.NewReader(bufio.NewReader(gz))
	reader.Comma = ','

	headers, err := reader.Read()
	log.Println("header: ", headers)
	if err != nil {
		log.Fatal(err)
	}

	var dataList []map[string]interface{}

	rows, _ := reader.ReadAll()
	for _, row := range rows {
		dataMap := make(map[string]interface{})
		for j, header := range headers {
			dataMap[header] = row[j]
		}
		dataList = append(dataList, dataMap)
	}
	log.Println("length: ", len(dataList))
	bulkInsertToElasticsearch(dataList, es)
}

func bulkInsertToElasticsearch(dataList []map[string]interface{}, es *elasticsearch.Client) error {
	var buf bytes.Buffer

	for _, dataMap := range dataList {
		log.Println("dataMap: ", dataMap)
		// Elasticsearch 메타데이터
		meta := []byte(fmt.Sprintf(`{ "create" : { "_index" : "%s" } }%s`, "twamp-data", "\n"))
		data, err := json.Marshal(dataMap)
		if err != nil {
			return fmt.Errorf("error marshalling dataMap: %w", err)
		}
		data = append(data, "\n"...)

		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	req := esapi.BulkRequest{
		Body:    bytes.NewReader(buf.Bytes()),
		Refresh: "true",
	}

	res, err := req.Do(context.Background(), es)
	if err != nil {
		return fmt.Errorf("failure indexing batch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var resBody map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&resBody); err != nil {
			return fmt.Errorf("error parsing the response body: %w", err)
		}
		return fmt.Errorf("error indexing batch: %s", resBody)
	} else {
		log.Printf("Successfully batch of %d messages", len(dataList))
	}
	return nil
}
