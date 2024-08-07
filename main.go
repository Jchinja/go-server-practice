package main

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var logger TransactionLogger
var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

var ErrorNoSuchKey = errors.New("No Such Key")

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

type EventType byte

const (
	_ = iota
	EventDelete
	EventPut
)

type Event struct {
	Sequence  uint64
	EventType EventType
	Key       string
	Value     string
}

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	select {
	case l.events <- Event{EventType: EventPut, Key: key, Value: value}:
	default:
	}
}
func (l *PostgresTransactionLogger) WriteDelete(key string) {
	select {
	case l.events <- Event{EventType: EventDelete, Key: key, Value: "delete"}:
	default:
	}
}
func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

// func (l *PostgresTransactionLogger) Run() {
// 	events := make(chan Event, 16)
// 	l.events = events

// 	errors := make(chan error, 1)

// 	l.errors = errors
// 	go func() {
// 		for e := range events {
// 			l.lastSequence++
// 			_, err := fmt.Fprintf(
// 				l.file,
// 				"%d\t%d\t%s\t%s\n",
// 				l.lastSequence, e.EventType, e.Key, e.Value)
// 			fmt.Println(err)
// 			if err != nil {
// 				errors <- err
// 				return
// 			}
// 		}
// 	}()
// }
// func (l *FileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
// 	scanner := bufio.NewScanner(l.file)
// 	outEvent := make(chan Event)
// 	outError := make(chan error, 1)
// 	go func() {
// 		var e Event
// 		defer close(outError)
// 		defer close(outEvent)
// 		for scanner.Scan() {
// 			line := scanner.Text()
// 			_, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value)
// 			if err != nil {
// 				outError <- fmt.Errorf("input parse error: %w", err)
// 				return
// 			}
// 			if l.lastSequence >= e.Sequence {
// 				outError <- fmt.Errorf("transaction numbers out of sequence")
// 				return
// 			}
// 			l.lastSequence = e.Sequence
// 			outEvent <- e
// 		}
// 		if err := scanner.Err(); err != nil {
// 			outError <- fmt.Errorf("transaction log read failure: %w", err)
// 			return
// 		}
// 	}()
// 	return outEvent, outError
// }

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf("host=%s dbname=%s user=%s password=%s", config.host, config.dbName, config.user, config.password)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}
	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}
	logger := &PostgresTransactionLogger{db: db}
	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}

func initalizeTransactionLog() error {
	var err error
	logger, err = NewPostgresTransactionLogger(PostgresDBParams{
		host:     "localhost",
		dbName:   "kvs",
		user:     "",
		password: "",
	})

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true
	logger.Run()
	for ok && err == nil {
		select {
		case err, ok = <-errors:
			fmt.Println(err, ok)
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				fmt.Println("Reading in delete")
				err = Delete(e.Key)
			case EventPut:
				fmt.Println("Reading in put")
				err = Put(e.Key, e.Value)
			}
		}
	}
	fmt.Println("finished loading data", store.m)
	return err
}

func Put(key, value string) error {
	store.Lock()
	store.m[key] = value
	store.Unlock()
	return nil
}
func Get(key string) (string, error) {
	store.RLock()
	value, ok := store.m[key]
	store.RUnlock()
	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}
func Delete(key string) error {
	store.Lock()
	delete(store.m, key)
	store.Unlock()
	return nil
}
func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)

	key := vars["key"]

	value, err := io.ReadAll(r.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
}
func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value))
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]
	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WriteDelete(key)
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).
		Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).
		Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).
		Methods("DELETE")

	log.Fatal((http.ListenAndServe(":8080", r)))
}
