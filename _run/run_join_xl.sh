# get join large (500GB dataset)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/join-500gb.duckdb --output-document=data/join-500gb.duckdb


# expand groupby-small datasets to csv
duckdb -c data/join-500gb.duckdb "copy x to 'data/J1_NA_0_0.csv' (FORMAT CSV)"
duckdb -c data/join-500gb.duckdb "copy big to 'data/J1_1e10_0_0.csv' (FORMAT CSV)"
duckdb -c data/join-500gb.duckdb "copy medim to 'data/J1_1e7_0_0.csv' (FORMAT CSV)"
duckdb -c data/join-500gb.duckdb "copy small to 'data/J1_1e4_0_0.csv' (FORMAT CSV)"


cp ../_control/data_join_xlarge.csv ../_control/data.csv

echo "Running join x-large (500GB)"
./run.sh

###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb


