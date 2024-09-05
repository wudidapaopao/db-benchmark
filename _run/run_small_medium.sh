# first download and expand small data

# get groupby small (0.5GB and 5GB datasets)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby_small.duckdb
# get join small (0.5GB and 5GB datasets)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join_small.duckdb


# expand groupby-small datasets to csv
~/duckdb -c groupby_small.duckdb "copy ....."

# expand join-small datasets to csv
~/duckdb -c join_small.duckdb "copy ...."


cp ../_control/data_small.csv ../_control/data.csv


echo "Running all solutions on small (0.5GB and 5GB) datasets"
./run.sh


###
echo "done..."
echo "removing small data files"
rm data/*.csv
rm data/*.duckdb

