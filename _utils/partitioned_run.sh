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


#--------------------------------- LARGE BENCHMARKS ---------------------------------

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

#--------------------------------- X-LARGE GROUP BY BENCHMARKS ---------------------------------

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

#--------------------------------- X-LARGE JOIN BENCHMARKS ---------------------------------


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


