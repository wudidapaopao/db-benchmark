# download and expand large data

# get groupby large (0.5GB and 5GB datasets)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby_large.duckdb
# get join small (0.5GB and 5GB datasets)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join_large.duckdb


# expand groupby-small datasets to csv
duckdb -c groupby_large.duckdb "copy G1_1e9_1e2_0_0 to 'G1_1e9_1e2_0_0.csv' (FORMAT CSV)"
duckdb -c groupby_large.duckdb "copy G1_1e9_1e1_0_0 to 'G1_1e9_1e1_0_0.csv' (FORMAT CSV)"
duckdb -c groupby_large.duckdb "copy G1_1e9_2e0_0_0 to 'G1_1e9_2e0_0_0.csv' (FORMAT CSV)"
duckdb -c groupby_large.duckdb "copy G1_1e9_1e2_0_1 to 'G1_1e9_1e2_0_1.csv' (FORMAT CSV)"
duckdb -c groupby_large.duckdb "copy G1_1e9_1e2_5_0 to 'G1_1e9_1e2_5_0.csv' (FORMAT CSV)"

# expand join-small datasets to csv
duckdb -c join_large.duckdb "copy x to 'J1_NA_0_0.csv' (FORMAT CSV)"
duckdb -c join_large.duckdb "copy big to 'J1_1e9_0_0.csv' (FORMAT CSV)"
duckdb -c join_large.duckdb "copy medim to 'J1_1e6_0_0.csv' (FORMAT CSV)"
duckdb -c join_large.duckdb "copy small to 'J1_1e3_0_0.csv' (FORMAT CSV)"


cp ../_control/data_large.csv ../_control/data.csv


echo "Running all solutions on large (50GB) datasets"
./run.sh


###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb