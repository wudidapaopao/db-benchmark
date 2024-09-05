# download and expand large data

# get groupby large (0.5GB and 5GB datasets)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby_large.duckdb
# get join small (0.5GB and 5GB datasets)
https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join_large.duckdb


# expand groupby-small datasets to csv
~/duckdb -c groupby_large.duckdb "copy ....."

# expand join-small datasets to csv
~/duckdb -c join_large.duckdb "copy ...."


cp ../_control/data_large.csv ../_control/data.csv


echo "Running all solutions on large (50GB) datasets"
./run.sh


###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb