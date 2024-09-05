# get join large (500GB dataset)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join-500gb.duckdb


# expand groupby-small datasets to csv
~/duckdb -c groupby_large.duckdb "copy ....."


cp ../_control/data_join_xlarge.csv ../_control/data.csv

echo "Running join x-large (500GB)"
./run.sh

###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb


