package main

import (
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"github.com/go-sql-driver/mysql"
	"github.com/twpayne/go-proj/v10"
	"log"
	"math"
	"os"
	"time"
	"unicode/utf8"
)

var srcDb *sql.DB

func main() {
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

	if err := os.Mkdir("./out", 0750); err != nil && !errors.Is(err, os.ErrExist) {
		panic(err)
	}

	outF, err := os.Create("./out/meta.ndjson.gz")
	if err != nil {
		panic(err)
	}
	outW, err := gzip.NewWriterLevel(outF, gzip.BestCompression)
	if err != nil {
		panic(err)
	}

	log.Println("Scanning")

	i := 0
	scanRows(func(row []byte) {
		if _, err := outW.Write(row); err != nil {
			panic(err)
		}
		if _, err := outW.Write([]byte("\n")); err != nil {
			panic(err)
		}

		if i == 0 {
			log.Println("Wrote first row")
			_ = outW.Flush()
		}

		i++
		if i%100_000 == 0 {
			log.Println("Wrote", i)
		}
	})

	// Finalize

	if err := outW.Close(); err != nil {
		panic(err)
	}
	if err := outF.Close(); err != nil {
		panic(err)
	}
	log.Println("All done")
}

type tagJSON struct {
	Prefix string `json:"prefix"`
	Tag    string `json:"tag"`
}

func scanRows(cb func(row []byte)) {
	osGBToWGS84, err := proj.NewCRSToCRS("epsg:27700", "epsg:4326", nil)
	if err != nil {
		panic(err)
	}

	irishToWGS84, err := proj.NewCRSToCRS("epsg:29903", "epsg:4326", nil)
	if err != nil {
		panic(err)
	}

	rows, err := srcDb.Query(`
SELECT gridimage_base.*,
       gridimage_geo.*,
       gridimage_size.*,
       gridimage_text.*,
       json_arrayagg(json_object('prefix', prefix, 'tag', tag)) AS tags
FROM gridimage_base
         LEFT JOIN gridimage_geo on gridimage_base.gridimage_id = gridimage_geo.gridimage_id
         LEFT JOIN gridimage_size on gridimage_base.gridimage_id = gridimage_size.gridimage_id
         LEFT JOIN gridimage_text on gridimage_base.gridimage_id = gridimage_text.gridimage_id
         LEFT JOIN gridimage_tag on gridimage_base.gridimage_id = gridimage_tag.gridimage_id
         LEFT JOIN tag on tag.tag_id = gridimage_tag.tag_id
GROUP BY gridimage_base.gridimage_id
	`)
	if err != nil {
		panic(err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}

	count := 0
	for rows.Next() {
		if err := rows.Err(); err != nil {
			panic(err)
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
			case "JSON":
				scanArgs[i] = new(json.RawMessage)
			default:
				panic("unhandled column type: " + col.DatabaseTypeName())
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
			} else if z, ok := r.(*json.RawMessage); ok {
				var tags []tagJSON
				if err := json.Unmarshal(*z, &tags); err != nil {
					panic(err)
				}

				if len(tags) == 1 && tags[0].Tag == "" {
					empty := json.RawMessage("[]")
					row[k] = &empty
				} else {
					row[k] = z
				}
			} else {
				panic("unimplemented type")
			}
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
		}

		for _, col := range []string{"x", "y"} {
			delete(row, col)
		}

		rowJSON, err := json.Marshal(row)
		if err != nil {
			panic(err)
		}

		cb(rowJSON)

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
