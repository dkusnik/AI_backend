# WARC extraction and ES ingestion tool

# Quickstart

make
built files and direcorty structure land in out/

configure ES ingestion node address and password in .profile

extract text, store in cache:
./warc2wet.sh --url-id=123 --crawl-id=456 file.warc.gz|some/folder/with/warc/

upload cache to ES:
./es-upsert.sh --url-id 123 --crawl-id 456

remove documents from ES:
./es-delete.sh --url-id 123 --crawl-id 456


Integration note: json is printed into stdout, human readable output into stderr

# Complete tool list

Convert WARC to WET:                   warc2wet.sh --url-id=X --crawl-id=Y file(s)|folder(s)
Initialize ES stream:                  es-reinit.sh --es-stream=NAME
Add documents to ES:                   es-upsert.sh --url-id=X --crawl-id=Y (--es-stream=NAME)
Readd complete set of documents to ES: es-upsert-all.sh (--es-stream=NAME)
Remove documents from ES:              es-delete.sh --url-id=X --crawl-id=Y (--es-stream=NAME)

If merging documents is needed:        wet-merge.sh

tool for modifying/filtering WARC:     warc-cli --help
tool for operations on ES:             es-cli --help


Configuration file is in app/conf/config.yaml, notable features:
- concurrency configuration
- memory limits
- enabling/disabling ISA-L (fast decompression), readablility (boilerplate removeal), poppler (pdf2text)
- filters, including MIME types

See tool --help and dedicated README for each tool details.


# update 2026.08.23

./warc2wet.sh --data-dir=/opt/warc2es/out/tst/ --url-id=123 --crawl-id=456 ./in/plock.ap.gov.pl.warc.gz 
./es-upsert.sh --url-id=123 --crawl-id=456 --data-dir=/opt/warc2es/out/tst
./es-delete.sh --url-id=123 --crawl-id=456 --data-dir=/dev/null
