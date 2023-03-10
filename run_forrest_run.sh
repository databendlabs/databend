for i in {0..1000}; do

echo "round $i"

curl -H "stage_name:s1" -F "upload=@./tests/data/sample.csv" -XPUT http://root:@localhost:8000/v1/upload_to_stage

curl -s -u root: -XPOST "http://localhost:8000/v1/query" --header 'Content-Type: application/json' -d '{"sql": "insert into sample (Id, City, Score) values", "stage_attachment": {"location": "@s1/sample.csv", "copy_options": {"purge": "true"}}}'


done
