# get groupby large (500GB dataset)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby-500gb.duckdb


# expand groupby-small datasets to csv
~/duckdb -c groupby_large.duckdb "copy ....."


cp ../_control/data_groupby_xlarge.csv ../_control/data.csv

echo "Running groupby x-large (500GB) datasets"
./run.sh

###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb