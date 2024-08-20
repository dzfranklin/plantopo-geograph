#!/usr/bin/env bash
set -euo pipefail

if [[ ! -d "./source" ]]; then
  echo "./source does not exist, downloading dumps"
  mkdir source
  pushd source
  wget "http://data.geograph.org.uk/dumps/gridimage_base.mysql.gz"
  wget "http://data.geograph.org.uk/dumps/gridimage_geo.mysql.gz"
  wget "http://data.geograph.org.uk/dumps/gridimage_size.mysql.gz"
  wget "http://data.geograph.org.uk/dumps/gridimage_text.mysql.gz"
  wget "http://data.geograph.org.uk/dumps/gridimage_tag.mysql.gz"
  date -u '+%Y-%m-%d' > downloaded_at.txt
  popd
else
  echo "./source exists, using existing dumps"
fi

docker build --tag gg-import --file mysql.Dockerfile . >/dev/null

docker stop gg-import >/dev/null || :
docker rm gg-import >/dev/null || :

docker run \
  --name gg-import \
  --detach \
  --mount type=bind,source="$(pwd)"/source,target=/source \
  -p 3000:3306 \
  gg-import >/dev/null

echo "waiting for mysql"
sleep 5
until docker exec gg-import sh -c "mysql -u root -e 'SELECT 1'" &>/dev/null
do
  sleep 2
done
echo "mysql ready"

docker exec gg-import sh -c "mysql -u root -e 'CREATE DATABASE geograph CHARACTER SET latin1 COLLATE latin1_swedish_ci'"
docker exec gg-import sh -c "mysql -u root -e 'SET GLOBAL sql_mode=\"\"' geograph"

for f in source/*; do
  if [[ "$f" != "source/downloaded_at.txt" ]]; then
    echo "Importing $f"
    docker exec gg-import sh -c "gunzip <$f | mysql -u root geograph"
  fi
done

docker exec gg-import sh -c "mysql -u root -e 'ALTER DATABASE geograph CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci'"

exit 1
go run .

docker stop gg-import
docker rm gg-import
