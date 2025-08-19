#!/usr/bin/env Rscript

# 提取 chDB 在 5G 和 0.5G 数据集上的查询执行时间
library(data.table)

# 加载数据
source("_report/report.R")
ld <- time_logs()

# 筛选 chDB 数据
chdb_data <- ld[solution == "chdb"]

if (nrow(chdb_data) == 0) {
  cat("没有找到 chDB 的测试数据\n")
  quit()
}

# 筛选 5G 和 0.5G 数据集 (1e8=5G, 1e7=0.5G)
chdb_target <- chdb_data[in_rows %in% c("1e7", "1e8")]

if (nrow(chdb_target) == 0) {
  cat("没有找到 0.5G 或 5G 的 chDB 测试数据\n")
  quit()
}

# 添加数据集大小标签
chdb_target[, size_label := ifelse(in_rows == "1e7", "0.5GB", "5GB")]

# 0.5GB 数据集
cat("=== 0.5GB 数据集 ===\n")

# Group By 查询 - 0.5GB
groupby_05gb <- chdb_target[size_label == "0.5GB" & task == "groupby"][order(question)]
if (nrow(groupby_05gb) > 0) {
  cat("\nGroup By 查询:\n")
  for (i in 1:nrow(groupby_05gb)) {
    row <- groupby_05gb[i]
    cat(sprintf("%s: %.4f秒, %.4f秒\n", row$question, row$time_sec_1, row$time_sec_2))
  }
}

# Join 查询 - 0.5GB
join_05gb <- chdb_target[size_label == "0.5GB" & task == "join"][order(question)]
if (nrow(join_05gb) > 0) {
  cat("\nJoin 查询:\n")
  for (i in 1:nrow(join_05gb)) {
    row <- join_05gb[i]
    cat(sprintf("%s: %.4f秒, %.4f秒\n", row$question, row$time_sec_1, row$time_sec_2))
  }
}

# 5GB 数据集
cat("\n=== 5GB 数据集 ===\n")

# Group By 查询 - 5GB
groupby_5gb <- chdb_target[size_label == "5GB" & task == "groupby"][order(question)]
if (nrow(groupby_5gb) > 0) {
  cat("\nGroup By 查询:\n")
  for (i in 1:nrow(groupby_5gb)) {
    row <- groupby_5gb[i]
    cat(sprintf("%s: %.4f秒, %.4f秒\n", row$question, row$time_sec_1, row$time_sec_2))
  }
}

# Join 查询 - 5GB
join_5gb <- chdb_target[size_label == "5GB" & task == "join"][order(question)]
if (nrow(join_5gb) > 0) {
  cat("\nJoin 查询:\n")
  for (i in 1:nrow(join_5gb)) {
    row <- join_5gb[i]
    cat(sprintf("%s: %.4f秒, %.4f秒\n", row$question, row$time_sec_1, row$time_sec_2))
  }
}
