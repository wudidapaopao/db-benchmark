# get groupby large (500GB dataset)
wget https://duckdb-blobs.s3.amazonaws.com/data/db-benchmark-data/groupby-500gb.duckdb --output-document=groupby-500gb.duckdb


# expand groupby-small datasets to csv
duckdb data/groupby-500gb.duckdb -c "copy G1_1e10_1e4_10_0 to 'data/G1_1e10_1e4_10_0.csv' (FORMAT CSV)"


cp ../_control/data_groupby_xlarge.csv ../_control/data.csv

echo "Running groupby x-large (500GB) datasets"
./run.sh

###
echo "done..."
echo "removing data files"
rm data/*.csv
rm data/*.duckdb


/bin:/usr/bin