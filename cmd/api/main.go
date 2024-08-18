package main

import (
	"encoding/json"
	"errors"
	"fmt"
	geograph "github.com/dzfranklin/plantopo-geograph"
	"github.com/tidwall/sjson"
	"log"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

var serverHost string
var imageSecret []byte
var store *geograph.Store

func main() {
	addr := "0.0.0.0:8080"
	imageSecret = []byte(geograph.GetEnvString("IMAGE_SECRET"))
	metaDir := geograph.GetEnvString("META_DIR")
	serverHost = geograph.GetEnvString("HOST")

	store = geograph.Open(metaDir)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("GET /v1/gridimage/{id}", handleGetByID)
	mux.HandleFunc("GET /v1/within", handleGetWithin)
	mux.HandleFunc("GET /v1/near", handleGetNear)

	slog.Info(fmt.Sprintf("Listening on %s", addr))
	if err := http.ListenAndServe(addr, applyCORS(mux)); err != nil {
		log.Fatal(err)
	}
}

func handleGetByID(w http.ResponseWriter, r *http.Request) {
	idValue := r.PathValue("id")
	id, err := strconv.ParseInt(idValue, 10, 32)
	if err != nil {
		respondErr(w, http.StatusNotFound)
		return
	}

	forBatchProcessing := getReqOptBool(r, "for_batch_processing")

	meta, err := store.Get(int32(id))
	if errors.Is(err, os.ErrNotExist) {
		respondErr(w, http.StatusNotFound)
		return
	} else if err != nil {
		respondISE(w, err)
		return
	}

	value, err := setImageSrc(meta, forBatchProcessing)
	if err != nil {
		respondISE(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(value))
}

func handleGetWithin(w http.ResponseWriter, r *http.Request) {
	minPoint, ok := getReqPoint(w, r, "min")
	if !ok {
		return
	}
	maxPoint, ok := getReqPoint(w, r, "max")
	if !ok {
		return
	}
	pageSize, ok := getReqOptInt(w, r, "page_size", 100)
	if !ok {
		return
	}
	cursor, ok := getReqOptInt(w, r, "cursor", 0)
	if !ok {
		return
	}
	bySubject := getReqOptBool(r, "by_subject")
	forBatchProcessing := getReqOptBool(r, "for_batch_processing")

	var index = geograph.ViewpointIndex
	if bySubject {
		index = geograph.SubjectIndex
	}

	hasNext, nextCursor, pictures, err := store.Within(minPoint, maxPoint, index, pageSize, cursor)
	if err != nil {
		respondISE(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	out := struct {
		Pictures []json.RawMessage `json:"pictures"`
		Next     *string           `json:"next"`
	}{}

	for _, meta := range pictures {
		value, err := setImageSrc(meta, forBatchProcessing)
		if err != nil {
			respondISE(w, err)
			return
		}
		out.Pictures = append(out.Pictures, []byte(value))
	}

	if hasNext {
		nextURL := copyURLWithCursor(r.URL, nextCursor).String()
		out.Next = &nextURL
	}

	outJSON, err := json.Marshal(out)
	if err != nil {
		respondISE(w, err)
		return
	}
	_, _ = w.Write(outJSON)
}

func handleGetNear(w http.ResponseWriter, r *http.Request) {
	targetPoint, ok := getReqPoint(w, r, "target")
	if !ok {
		return
	}
	pageSize, ok := getReqOptInt(w, r, "page_size", 10)
	if !ok {
		return
	}
	cursor, ok := getReqOptInt(w, r, "cursor", 0)
	if !ok {
		return
	}
	bySubject := getReqOptBool(r, "by_subject")
	forBatchProcessing := getReqOptBool(r, "for_batch_processing")

	var index = geograph.ViewpointIndex
	if bySubject {
		index = geograph.SubjectIndex
	}

	hasNext, nextCursor, pictures, err := store.Near(targetPoint, index, pageSize, cursor)
	if err != nil {
		respondISE(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	out := struct {
		Pictures []json.RawMessage `json:"pictures"`
		Next     *string           `json:"next"`
	}{}

	for _, meta := range pictures {
		value, err := setImageSrc(meta, forBatchProcessing)
		if err != nil {
			respondISE(w, err)
			return
		}
		out.Pictures = append(out.Pictures, []byte(value))
	}

	if hasNext {
		nextURL := copyURLWithCursor(r.URL, nextCursor).String()
		out.Next = &nextURL
	}

	outJSON, err := json.Marshal(out)
	if err != nil {
		respondISE(w, err)
		return
	}
	_, _ = w.Write(outJSON)
}

func setImageSrc(meta string, forBatchProcessing bool) (string, error) {
	src := geograph.GetImageSrc(imageSecret, meta, forBatchProcessing)
	return sjson.Set(meta, "src", src)
}

func getReqPoint(w http.ResponseWriter, r *http.Request, param string) ([2]float32, bool) {
	v := r.URL.Query().Get(param)
	if v == "" {
		respondBadReq(w, fmt.Sprintf("parameter %s required", param))
		return [2]float32{}, false
	}

	parts := strings.Split(v, ",")
	if len(parts) != 2 {
		respondBadReq(w, fmt.Sprintf("parameter %s should be a point lng,lat (like -0.12,51.49)", param))
		return [2]float32{}, false
	}
	lng, err := strconv.ParseFloat(parts[0], 32)
	if err != nil {
		respondBadReq(w, fmt.Sprintf("parameter %s should be a point lng,lat (like -0.12,51.49)", param))
		return [2]float32{}, false
	}
	lat, err := strconv.ParseFloat(parts[1], 32)
	if err != nil {
		respondBadReq(w, fmt.Sprintf("parameter %s should be a point lng,lat (like -0.12,51.49)", param))
		return [2]float32{}, false
	}
	return [2]float32{float32(lng), float32(lat)}, true
}

func getReqOptInt(w http.ResponseWriter, r *http.Request, param string, defaultVal int) (int, bool) {
	s := r.URL.Query().Get(param)
	if s == "" {
		return defaultVal, true
	}

	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		respondBadReq(w, fmt.Sprintf("parameter %s should be an integer", param))
		return 0, false
	}

	return int(v), true
}

func getReqOptBool(r *http.Request, param string) bool {
	s := r.URL.Query().Get(param)
	if s == "" || s == "false" || s == "f" {
		return false
	}
	return true
}

func copyURLWithCursor(url *url.URL, cursor int) *url.URL {
	u := *url
	u.Scheme = "https"
	u.Host = serverHost

	q := u.Query()
	q.Set("cursor", strconv.Itoa(cursor))
	u.RawQuery = q.Encode()

	return &u
}

func applyCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == "" ||
			strings.HasSuffix(origin, "://plantopo.com") ||
			strings.HasSuffix(origin, ".plantopo.com") {

			w.Header().Set("Access-Control-Allow-Methods", "GET")
			w.Header().Set("Access-Control-Allow-Origin", origin)

			next.ServeHTTP(w, r)
		} else {
			http.Error(w, "Invalid Origin header", http.StatusForbidden)
		}
	})
}

func respondErr(w http.ResponseWriter, status int) {
	http.Error(w, http.StatusText(status), status)
}

func respondBadReq(w http.ResponseWriter, msg string) {
	http.Error(w, "Bad Request: "+msg, http.StatusBadRequest)
}

func respondISE(w http.ResponseWriter, err error) {
	slog.Error("internal server error", "error", err)
	respondErr(w, http.StatusInternalServerError)
}
