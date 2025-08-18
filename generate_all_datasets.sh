#!/bin/bash
set -e

echo "Generating all datasets using _data R scripts..."

# Create data directory
mkdir -p data
cd data

echo "Generating Small datasets (1e7)..."
# Small groupby datasets (1e7)
Rscript ../_data/groupby-datagen.R 1e7 1e2 0 0  # G1_1e7_1e2_0_0.csv
Rscript ../_data/groupby-datagen.R 1e7 1e1 0 0  # G1_1e7_1e1_0_0.csv
Rscript ../_data/groupby-datagen.R 1e7 2e0 0 0  # G1_1e7_2e0_0_0.csv
Rscript ../_data/groupby-datagen.R 1e7 1e2 0 1  # G1_1e7_1e2_0_1.csv
Rscript ../_data/groupby-datagen.R 1e7 1e2 5 0  # G1_1e7_1e2_5_0.csv

# Small join datasets (1e7)
Rscript ../_data/join-datagen.R 1e7 0 0 0       # J1_1e7_NA_0_0.csv
Rscript ../_data/join-datagen.R 1e7 0 5 0       # J1_1e7_NA_5_0.csv
Rscript ../_data/join-datagen.R 1e7 0 0 1       # J1_1e7_NA_0_1.csv

echo "Generating Medium datasets (1e8)..."
# Medium groupby datasets (1e8)
Rscript ../_data/groupby-datagen.R 1e8 1e2 0 0  # G1_1e8_1e2_0_0.csv
Rscript ../_data/groupby-datagen.R 1e8 1e1 0 0  # G1_1e8_1e1_0_0.csv
Rscript ../_data/groupby-datagen.R 1e8 2e0 0 0  # G1_1e8_2e0_0_0.csv
Rscript ../_data/groupby-datagen.R 1e8 1e2 0 1  # G1_1e8_1e2_0_1.csv
Rscript ../_data/groupby-datagen.R 1e8 1e2 5 0  # G1_1e8_1e2_5_0.csv

# Medium join datasets (1e8)
Rscript ../_data/join-datagen.R 1e8 0 0 0       # J1_1e8_NA_0_0.csv
Rscript ../_data/join-datagen.R 1e8 0 5 0       # J1_1e8_NA_5_0.csv
Rscript ../_data/join-datagen.R 1e8 0 0 1       # J1_1e8_NA_0_1.csv

echo "Generating Large datasets (1e9)..."
# Large groupby datasets (1e9)
Rscript ../_data/groupby-datagen.R 1e9 1e2 0 0  # G1_1e9_1e2_0_0.csv
Rscript ../_data/groupby-datagen.R 1e9 1e1 0 0  # G1_1e9_1e1_0_0.csv
Rscript ../_data/groupby-datagen.R 1e9 2e0 0 0  # G1_1e9_2e0_0_0.csv
Rscript ../_data/groupby-datagen.R 1e9 1e2 0 1  # G1_1e9_1e2_0_1.csv
Rscript ../_data/groupby-datagen.R 1e9 1e2 5 0  # G1_1e9_1e2_5_0.csv

# Large join datasets (1e9)
Rscript ../_data/join-datagen.R 1e9 0 0 0       # J1_1e9_NA_0_0.csv

cd ..

echo "Complete! Generated 22 datasets:"
echo "- 15 groupby datasets (5 small + 5 medium + 5 large)"
echo "- 7 join datasets (3 small + 3 medium + 1 large)"
ls -la data/*.csv | wc -l
