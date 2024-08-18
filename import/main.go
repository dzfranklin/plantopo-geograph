package main

import (
	"archive/tar"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	geograph "github.com/dzfranklin/plantopo-geograph"
	"github.com/go-sql-driver/mysql"
	"github.com/twpayne/go-proj/v10"
	"github.com/valyala/gozstd"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
	"unicode/utf8"
)

var srcDb *sql.DB

func main() {
	onlyWriteSample := flag.Bool("sample", false, "Only write a sample of the data")
	outPath := flag.String("out", "./out/meta.tar", "File to write output to")
	flag.Parse()

	dbCfg := mysql.Config{
		User:   "root",
		Net:    "tcp",
		Addr:   "localhost:3000",
		DBName: "geograph",
	}
	var err error
	srcDb, err = sql.Open("mysql", dbCfg.FormatDSN())
	if err != nil {
		panic(err)
	}
	if err := srcDb.Ping(); err != nil {
		panic(err)
	}

	log.Println("Building dictionary")

	i := 0
	var samples [][]byte
	scanRows(true, func(_ metaData, row []byte) {
		samples = append(samples, row)
		i++
		if i%1000 == 0 {
			fmt.Println("Sampled", i)
		}
	})
	log.Println("Took", i, "samples for dictionary")

	dict := gozstd.BuildDict(samples, 100*1024)
	log.Println("Built dictionary:", len(dict)/1024, "KiB")

	log.Println("Exporting")

	if err := os.RemoveAll("./out"); err != nil && !errors.Is(err, os.ErrNotExist) {
		panic(err)
	}
	if err := os.Mkdir("./out", 0750); err != nil {
		panic(err)
	}

	outContainer, err := os.Create(*outPath)
	if err != nil {
		panic(err)
	}
	out := tar.NewWriter(outContainer)

	if err := out.WriteHeader(&tar.Header{
		Name: "dictionary",
		Mode: 0600,
		Size: int64(len(dict)),
	}); err != nil {
		panic(err)
	}
	if _, err := out.Write(dict); err != nil {
		panic(err)
	}

	cdict, err := gozstd.NewCDict(dict)
	if err != nil {
		panic(err)
	}

	index := geograph.IndexContents{
		ID:           make([]int32, 0, 1<<23),
		SubjectLng:   make([]float32, 0, 1<<23),
		SubjectLat:   make([]float32, 0, 1<<23),
		ViewpointLng: make([]float32, 0, 1<<23),
		ViewpointLat: make([]float32, 0, 1<<23),
	}

	i = 0
	scanRows(*onlyWriteSample, func(meta metaData, row []byte) {
		compressed := gozstd.CompressDict(nil, row, cdict)

		if err := out.WriteHeader(&tar.Header{
			Name: geograph.IDToPath(meta.id),
			Mode: 0600,
			Size: int64(len(compressed)),
		}); err != nil {
			panic(err)
		}

		if _, err := out.Write(compressed); err != nil {
			panic(err)
		}

		index.ID = append(index.ID, meta.id)
		index.SubjectLng = append(index.SubjectLng, float32(meta.subjectLng))
		index.SubjectLat = append(index.SubjectLat, float32(meta.subjectLat))
		index.ViewpointLng = append(index.ViewpointLng, float32(meta.viewpointLng))
		index.ViewpointLat = append(index.ViewpointLat, float32(meta.viewpointLat))

		i++
		if i%100_000 == 0 {
			fmt.Println("Wrote", i)
		}
	})

	log.Println("Writing index")

	encodedIndex := geograph.EncodeIndex(index)

	if err := out.WriteHeader(&tar.Header{
		Name: "index",
		Mode: 0600,
		Size: int64(len(encodedIndex)),
	}); err != nil {
		panic(err)
	}
	if _, err := out.Write(encodedIndex); err != nil {
		panic(err)
	}

	// Finalize

	if err := out.Close(); err != nil {
		panic(err)
	}
	log.Println("Exported to", *outPath)
}

type metaData struct {
	id                         int32
	subjectLng, subjectLat     float64
	viewpointLng, viewpointLat float64
}

func scanRows(sampleOnly bool, cb func(meta metaData, row []byte)) {
	osGBToWGS84, err := proj.NewCRSToCRS("epsg:27700", "epsg:4326", nil)
	if err != nil {
		panic(err)
	}

	irishToWGS84, err := proj.NewCRSToCRS("epsg:29903", "epsg:4326", nil)
	if err != nil {
		panic(err)
	}

	rows, err := srcDb.Query(`
SELECT *
FROM gridimage_base
         LEFT JOIN gridimage_geo on gridimage_base.gridimage_id = gridimage_geo.gridimage_id
         LEFT JOIN gridimage_size on gridimage_base.gridimage_id = gridimage_size.gridimage_id
         LEFT JOIN gridimage_text on gridimage_base.gridimage_id = gridimage_text.gridimage_id`)
	if err != nil {
		panic(err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	rng := rand.New(rand.NewSource(0))

	count := 0
	for rows.Next() {
		if err := rows.Err(); err != nil {
			panic(err)
		}

		if sampleOnly && rng.Intn(1000) != 0 {
			continue
		}

		scanArgs := make([]any, len(cols))
		for i, col := range cols {
			switch col.DatabaseTypeName() {
			case "INT", "SMALLINT", "TINYINT", "UNSIGNED MEDIUMINT", "UNSIGNED TINYINT", "UNSIGNED INT", "UNSIGNED SMALLINT":
				scanArgs[i] = new(sql.NullInt64)
			case "DECIMAL":
				scanArgs[i] = new(sql.NullFloat64)
			case "VARCHAR", "ENUM", "TEXT", "DATE":
				scanArgs[i] = new(sql.NullString)
			}
		}

		if err := rows.Scan(scanArgs...); err != nil {
			panic(err)
		}

		row := make(map[string]any)
		for i, col := range cols {
			k := col.Name()
			r := scanArgs[i]
			if z, ok := r.(*sql.NullInt64); ok {
				if z.Valid {
					row[k] = &z.Int64
				} else {
					row[k] = nil
				}
			} else if z, ok := r.(*sql.NullFloat64); ok {
				if z.Valid {
					row[k] = &z.Float64
				} else {
					row[k] = nil
				}
			} else if z, ok := r.(*sql.NullString); ok {
				if col.DatabaseTypeName() == "DATE" {
					if z.Valid && z.String != "0000-00-00" {
						timeValue, err := time.Parse("2006-01-02", z.String)
						if err == nil {
							row[k] = timeValue.Format("2006-01-02")
						} else {
							row[k] = nil
						}
					} else {
						row[k] = nil
					}
				} else {
					if z.Valid {
						if !utf8.Valid([]byte(z.String)) {
							panic("invalid utf8")
						}
						row[k] = &z.String
					} else {
						row[k] = nil
					}
				}
			} else {
				panic("unimplemented type")
			}
		}

		meta := metaData{
			id:         int32(*row["gridimage_id"].(*int64)),
			subjectLng: *row["wgs84_long"].(*float64),
			subjectLat: *row["wgs84_lat"].(*float64),
		}

		viewpointE := *row["viewpoint_eastings"].(*int64)
		viewpointN := *row["viewpoint_northings"].(*int64)
		refIdx := *row["reference_index"].(*int64)
		if viewpointE != 0 && viewpointN != 0 {
			var trans *proj.PJ
			switch refIdx {
			case 1:
				trans = osGBToWGS84
			case 2:
				trans = irishToWGS84
			default:
				panic("unimplemented")
			}

			projected, err := trans.Forward(proj.NewCoord(float64(viewpointE), float64(viewpointN), 0, 0))
			if err != nil {
				panic(err)
			}

			// epsg:4326 is lat,lng
			lat := roundPlaces(projected.X(), 6)
			lng := roundPlaces(projected.Y(), 6)

			row["viewpoint_wgs84_long"] = lng
			row["viewpoint_wgs84_lat"] = lat

			meta.viewpointLng = lng
			meta.viewpointLat = lat
		}

		for _, col := range []string{"x", "y"} {
			delete(row, col)
		}

		rowJSON, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}

		cb(meta, rowJSON)

		count++
	}

	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func roundPlaces(n float64, places int) float64 {
	m := math.Pow(10, float64(places))
	return math.Round(n*m) / m
}
