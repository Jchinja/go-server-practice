package main

import (
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
	_ "github.com/lib/pq"
)

var logger TransactionLogger

const transactionLogTableStr = "transactionlogs"
const dbName = "mydb"

var store = struct {
	sync.RWMutex
	m map[string]string
}{m: make(map[string]string)}

var errorNoSuchKey = errors.New("No Such Key")
var errorTableDoesNotExist = errors.New(fmt.Sprintf("pq: relation '%s' does not exist", transactionLogTableStr))

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	verifyTableExists() (bool, error)
	createTable() error
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
	case l.events <- Event{EventType: EventDelete, Key: key}:
	default:
	}
}
func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error)
	rows, err := l.db.Query(fmt.Sprintf(`SELECT type, key, value FROM %s ORDER BY logId;`, transactionLogTableStr))

	go func() {
		defer close(outEvent)
		defer close(outError)
		for rows.Next() {
			var e Event
			err = rows.Scan(&e.EventType, &e.Key, &e.Value)
			fmt.Println(e)
			if err != nil {
				fmt.Println(err)
				outError <- err
			}
			outEvent <- e
		}
	}()

	return outEvent, outError
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	errors := make(chan error, 1)

	l.events = events
	l.errors = errors
	go func() {
		for e := range events {
			ok, err := l.db.Exec(fmt.Sprintf(
				`INSERT INTO %s(type, key, value) VALUES (%d,'%s','%s');`, transactionLogTableStr, e.EventType, e.Key, e.Value))
			fmt.Println("run", ok, err)
			if err != nil {
				errors <- err
				return
			}
		}
	}()
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	query := fmt.Sprintf(`SELECT * FROM %s;`, transactionLogTableStr)
	ok, err := l.db.Query(query)
	fmt.Println("verifyTableExists", ok, err)
	if errors.Is(err, errorTableDoesNotExist) {
		return bool(false), nil
	} else if err != nil {
		return bool(false), nil
	}
	return bool(true), nil
}
func (l *PostgresTransactionLogger) createTable() error {
	createTableString := fmt.Sprintf(
		`CREATE  TABLE IF NOT EXISTS %s (
			logId 	SERIAL PRIMARY KEY,
			type	INT NOT NULL,
			key		VARCHAR(255) NOT NULL,
			value	TEXT
		);`, transactionLogTableStr)
	_, err := l.db.Exec(createTableString)
	return err
}

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
	logger = &PostgresTransactionLogger{db: db}
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

func initalizeTransactionLog(username, password string) error {
	var err error
	logger, err = NewPostgresTransactionLogger(PostgresDBParams{
		host:     "localhost",
		dbName:   dbName,
		user:     username,
		password: password,
	})
	if err != nil {
		fmt.Println(err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true
	logger.Run()
	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.Key)
			case EventPut:
				err = Put(e.Key, e.Value)
			}
		}
	}
	fmt.Println("finished loading data from postgres: ", store.m)
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
		return "", errorNoSuchKey
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
	if errors.Is(err, errorNoSuchKey) {
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
	dbUser := flag.String("username", "postgres", "username for db")
	password := flag.String("password", "", "password for db")
	flag.Parse()
	initalizeTransactionLog(*dbUser, *password)

	r := mux.NewRouter()
	r.HandleFunc("/v1/{key}", keyValuePutHandler).
		Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).
		Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).
		Methods("DELETE")

	log.Fatal((http.ListenAndServe(":8080", r)))
}
