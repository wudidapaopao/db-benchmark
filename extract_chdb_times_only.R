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

# 函数：打印查询时间
print_queries <- function(size, task_type) {
  cat(sprintf("\n=== %s %s 查询 ===\n", size, task_type))
  
  # 获取对应数据
  queries <- chdb_target[size_label == size & task == tolower(gsub(" ", "", task_type))][order(question)]
  
  if (nrow(queries) == 0) {
    cat(sprintf("没有找到 %s %s 查询数据\n", size, task_type))
    return()
  }
  
  for (i in 1:nrow(queries)) {
    row <- queries[i]
    cat(sprintf("[%s] %s: %.4f秒, %.4f秒\n", 
                row$data, row$question, row$time_sec_1, row$time_sec_2))
  }
}

# 检查数据可用性
cat("chDB 数据可用性:\n")
cat(sprintf("总记录数: %d (0.5GB: %d, 5GB: %d)\n", 
            nrow(chdb_target),
            nrow(chdb_target[size_label == "0.5GB"]),
            nrow(chdb_target[size_label == "5GB"])))

# 打印所有查询
print_queries("0.5GB", "Group By")
print_queries("0.5GB", "Join")
print_queries("5GB", "Group By")
print_queries("5GB", "Join")

# 总体性能汇总
cat("\n=== 总体性能汇总 ===\n")

for (size in c("0.5GB", "5GB")) {
  size_data <- chdb_target[size_label == size]
  
  if (nrow(size_data) > 0) {
    total_time <- sum(size_data$time_sec_1 + size_data$time_sec_2, na.rm = TRUE)
    avg_time <- mean((size_data$time_sec_1 + size_data$time_sec_2)/2, na.rm = TRUE)
    cat(sprintf("\n%s - 总查询数: %d, 总时间: %.4f秒, 平均时间: %.4f秒\n", 
                size, nrow(size_data), total_time, avg_time))
  }
}
