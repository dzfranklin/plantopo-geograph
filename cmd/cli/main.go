package main

import (
	"encoding/json"
	"flag"
	"fmt"
	geograph "github.com/dzfranklin/plantopo-geograph"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	metaDir := geograph.GetEnvString("META_DIR")

	// Commands

	getFlag := flag.String("get", "", "<id>")
	withinFlag := flag.String("within", "", "minLng,minLat,maxLng,maxLat")
	nearFlag := flag.String("near", "", "lng,lat")
	imageFlag := flag.String("image", "", "")

	// Options

	indexFlag := flag.String("index", "subject", "subject|viewpoint")
	maxFlag := flag.Int("max", 10, "")
	cursorFlag := flag.Int("cursor", 0, "")
	imageForBatchFlag := flag.Bool("image-for-batch", false, "")

	flag.Parse()

	store := geograph.Open(metaDir)

	if *getFlag != "" {
		id, err := strconv.ParseInt(*getFlag, 10, 32)
		if err != nil {
			flag.Usage()
			os.Exit(1)
		}

		res, err := store.Get(int32(id))
		if err != nil {
			panic(err)
		}
		fmt.Println(res)
	} else if *withinFlag != "" {
		parts := strings.Split(*withinFlag, ",")
		if len(parts) != 4 {
			flag.Usage()
			os.Exit(1)
		}
		floatParts := [4]float32{}
		for i, part := range parts {
			v, err := strconv.ParseFloat(part, 32)
			if err != nil {
				flag.Usage()
				os.Exit(1)
			}
			floatParts[i] = float32(v)
		}
		minPt := [2]float32{floatParts[0], floatParts[1]}
		maxPt := [2]float32{floatParts[2], floatParts[3]}

		var index geograph.IndexType
		switch *indexFlag {
		case "viewpoint":
			index = geograph.ViewpointIndex
		case "subject":
			index = geograph.SubjectIndex
		default:
			flag.Usage()
			os.Exit(1)
		}

		hasMore, nextCursor, res, err := store.Within(minPt, maxPt, index, *maxFlag, *cursorFlag)
		if err != nil {
			panic(err)
		}

		for _, v := range res {
			fmt.Println(v)
		}

		if hasMore {
			log.Println("next: cursor=", nextCursor)
		} else {
			log.Println("no more results")
		}
	} else if *nearFlag != "" {
		parts := strings.Split(*nearFlag, ",")
		if len(parts) != 2 {
			log.Println("invalid point")
			flag.Usage()
			os.Exit(1)
		}
		floatParts := [4]float32{}
		for i, part := range parts {
			v, err := strconv.ParseFloat(part, 32)
			if err != nil {
				log.Println("invalid float")
				flag.Usage()
				os.Exit(1)
			}
			floatParts[i] = float32(v)
		}
		target := geograph.Point(floatParts[0], floatParts[1])

		var index geograph.IndexType
		switch *indexFlag {
		case "viewpoint":
			index = geograph.ViewpointIndex
		case "subject":
			index = geograph.SubjectIndex
		default:
			flag.Usage()
			os.Exit(1)
		}

		hasMore, nextCursor, res, err := store.Near(target, index, *maxFlag, *cursorFlag)
		if err != nil {
			panic(err)
		}

		for _, v := range res {
			fmt.Println(v)
		}

		if hasMore {
			log.Println("next: cursor=", nextCursor)
		} else {
			log.Println("no more results")
		}
	} else if *imageFlag != "" {
		secret := []byte(geograph.GetEnvString("IMAGE_SECRET"))

		id, err := strconv.ParseInt(*imageFlag, 10, 32)
		if err != nil {
			flag.Usage()
			os.Exit(1)
		}

		meta, err := store.Get(int32(id))
		if err != nil {
			panic(err)
		}

		sizes := geograph.GetImageSrc(secret, meta, *imageForBatchFlag)

		sizesJSON, err := json.MarshalIndent(sizes, "", "    ")
		if err != nil {
			panic(err)
		}
		fmt.Println(string(sizesJSON))
	} else {
		flag.Usage()
	}
}
