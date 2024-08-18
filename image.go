package geograph

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/tidwall/gjson"
)

func getImageHash(secret []byte, meta string) string {
	/*  <https://github.com/geograph-project/geograph-project/blob/83ec6782fc81174480c6386c6ede90b0a0411d95/libs/geograph/gridimage.class.php#L445>
	substr(md5($this->gridimage_id.$this->user_id.$CONF['photo_hashing_secret']), 0, 8)
	*/

	gridimageId := gjson.Get(meta, "gridimage_id").String()
	userId := gjson.Get(meta, "user_id").String()

	var input bytes.Buffer
	input.Write([]byte(gridimageId))
	input.Write([]byte(userId))
	input.Write(secret)

	hash := md5.Sum(input.Bytes())
	return hex.EncodeToString(hash[:])[:8]
}

type ImageSrc struct {
	Original  string `json:"original,omitempty"`
	Large     string `json:"large,omitempty"`
	Small     string `json:"small,omitempty"`
	Thumbnail string `json:"thumbnail,omitempty"`
}

func GetImageSrc(secret []byte, meta string, forBatchProcessing bool) ImageSrc {
	/* From email with geograph
	$size = largest($row['original_width'],$row['original_height']);
	if ($size == 1024) {
		$url = getGeographUrl($id, $hash, "_original"); //if largest is 1024 anyway, we dont bother creating a another copy
	} elseif ($size > 1024) {
		$url = getGeographUrl($id, $hash, " _1024x1024");
	} else { //only the nominal 640px version available (may be smaller in practice!)
		$url = getGeographUrl($id, $hash, ""); //no suffix
	}
	*/

	hash := getImageHash(secret, meta)
	id := int32(gjson.Get(meta, "gridimage_id").Int())
	size := int32(max(gjson.Get(meta, "original_width").Int(), gjson.Get(meta, "original_height").Int()))

	out := ImageSrc{
		Small:     getGeographURL(forBatchProcessing, id, hash, ""),
		Thumbnail: getGeographURL(forBatchProcessing, id, hash, "_120x120"),
	}

	if size == 0 {
		out.Original = getGeographURL(forBatchProcessing, id, hash, "")
	} else {
		out.Original = getGeographURL(forBatchProcessing, id, hash, "_original")
	}

	if size == 1024 {
		out.Large = out.Original
	} else if size > 1024 {
		out.Large = getGeographURL(forBatchProcessing, id, hash, "_1024x1024")
	} else {
		out.Large = out.Small
	}

	return out
}

func getGeographURL(forBatchProcessing bool, id int32, hash string, variant string) string {
	/* <https://github.com/geograph-project/geograph-project/blob/83ec6782fc81174480c6386c6ede90b0a0411d95/libs/geograph/gridimage.class.php#L677>
	$ab=sprintf("%02d", floor(($this->gridimage_id%1000000)/10000));
	$cd=sprintf("%02d", floor(($this->gridimage_id%10000)/100));
	$abcdef=sprintf("%06d", $this->gridimage_id);
	$hash=$this->_getAntiLeechHash();
	if ($this->gridimage_id<1000000) {
		$fullpath="/photos/$ab/$cd/{$abcdef}_{$hash}.jpg";
	} else {
		$yz=sprintf("%02d", floor($this->gridimage_id/1000000));
		$fullpath="/geophotos/$yz/$ab/$cd/{$abcdef}_{$hash}.jpg";
	}
	*/

	var host string
	if forBatchProcessing {
		host = "https://pub-d59cbb40e5654f35830e1ca202469707.r2.dev"
	} else {
		host = "https://s0.geograph.org.uk"
	}

	if id < 1000000 {
		return fmt.Sprintf("%s/photos/%02d/%02d/%d_%s%s.jpg",
			host, id%1000000/10000, id%10000/100, id, hash, variant)
	} else {
		return fmt.Sprintf("%s/geophotos/%02d/%02d/%02d/%d_%s%s.jpg",
			host, id/1000000, id%1000000/10000, id%10000/100, id, hash, variant)
	}
}
