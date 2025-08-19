#!/usr/bin/env Rscript

# 提取 chDB 和 ClickHouse 在 5G 和 0.5G 数据集上的查询执行时间对比
library(data.table)

# 加载数据
source("_report/report.R")
ld <- time_logs()

# 筛选 chDB 和 ClickHouse 数据
chdb_data <- ld[solution == "chdb"]
clickhouse_data <- ld[solution == "clickhouse"]

if (nrow(chdb_data) == 0 && nrow(clickhouse_data) == 0) {
  cat("没有找到 chDB 或 ClickHouse 的测试数据\n")
  quit()
}

# 筛选 5G 和 0.5G 数据集 (1e8=5G, 1e7=0.5G)
chdb_target <- chdb_data[in_rows %in% c("1e7", "1e8")]
clickhouse_target <- clickhouse_data[in_rows %in% c("1e7", "1e8")]

# 添加数据集大小标签
if (nrow(chdb_target) > 0) {
  chdb_target[, size_label := ifelse(in_rows == "1e7", "0.5GB", "5GB")]
}
if (nrow(clickhouse_target) > 0) {
  clickhouse_target[, size_label := ifelse(in_rows == "1e7", "0.5GB", "5GB")]
}

# 函数：打印查询时间对比
print_query_comparison <- function(size, task_type) {
  cat(sprintf("\n=== %s %s 查询 ===\n", size, task_type))
  
  # 获取对应数据
  chdb_queries <- if (nrow(chdb_target) > 0) {
    chdb_target[size_label == size & task == tolower(gsub(" ", "", task_type))][order(question)]
  } else { data.table() }
  
  clickhouse_queries <- if (nrow(clickhouse_target) > 0) {
    clickhouse_target[size_label == size & task == tolower(gsub(" ", "", task_type))][order(question)]
  } else { data.table() }
  
  # 获取所有唯一查询
  all_questions <- unique(c(
    if (nrow(chdb_queries) > 0) chdb_queries$question else character(0),
    if (nrow(clickhouse_queries) > 0) clickhouse_queries$question else character(0)
  ))
  
  if (length(all_questions) == 0) {
    cat(sprintf("没有找到 %s %s 查询数据\n", size, task_type))
    return()
  }
  
  for (question in sort(all_questions)) {
    cat(sprintf("\n%s:\n", question))
    
    # chDB 数据
    chdb_row <- if (nrow(chdb_queries) > 0) {
      chdb_queries[question == question]
    } else { data.table() }
    
    if (nrow(chdb_row) > 0) {
      cat(sprintf("  [%s] chDB:       %.4f秒, %.4f秒\n", 
                  chdb_row$data[1], chdb_row$time_sec_1[1], chdb_row$time_sec_2[1]))
    } else {
      cat(sprintf("  [%s] chDB:       数据缺失\n", 
                  ifelse(size == "0.5GB", "G1_1e7_1e2_0_0", "G1_1e8_1e2_0_0")))
    }
    
    # ClickHouse 数据
    clickhouse_row <- if (nrow(clickhouse_queries) > 0) {
      clickhouse_queries[question == question]
    } else { data.table() }
    
    if (nrow(clickhouse_row) > 0) {
      cat(sprintf("  [%s] ClickHouse: %.4f秒, %.4f秒\n", 
                  clickhouse_row$data[1], clickhouse_row$time_sec_1[1], clickhouse_row$time_sec_2[1]))
    } else {
      cat(sprintf("  [%s] ClickHouse: 数据缺失\n", 
                  ifelse(size == "0.5GB", "G1_1e7_1e2_0_0", "G1_1e8_1e2_0_0")))
    }
    
    # 性能对比
    if (nrow(chdb_row) > 0 && nrow(clickhouse_row) > 0) {
      chdb_avg <- (chdb_row$time_sec_1[1] + chdb_row$time_sec_2[1]) / 2
      clickhouse_avg <- (clickhouse_row$time_sec_1[1] + clickhouse_row$time_sec_2[1]) / 2
      ratio <- chdb_avg / clickhouse_avg
      faster <- if (ratio < 1) "chDB 更快" else "ClickHouse 更快"
      cat(sprintf("  性能比率: %.2fx (%s)\n", abs(ratio), faster))
    }
  }
}

# 检查数据可用性
cat("数据可用性检查:\n")
cat(sprintf("chDB 记录数: %d (0.5GB: %d, 5GB: %d)\n", 
            nrow(chdb_target),
            if (nrow(chdb_target) > 0) nrow(chdb_target[size_label == "0.5GB"]) else 0,
            if (nrow(chdb_target) > 0) nrow(chdb_target[size_label == "5GB"]) else 0))
cat(sprintf("ClickHouse 记录数: %d (0.5GB: %d, 5GB: %d)\n", 
            nrow(clickhouse_target),
            if (nrow(clickhouse_target) > 0) nrow(clickhouse_target[size_label == "0.5GB"]) else 0,
            if (nrow(clickhouse_target) > 0) nrow(clickhouse_target[size_label == "5GB"]) else 0))

# 打印所有对比
print_query_comparison("0.5GB", "Group By")
print_query_comparison("0.5GB", "Join")
print_query_comparison("5GB", "Group By")
print_query_comparison("5GB", "Join")

# 总体性能汇总
cat("\n" %+% "=" %+% " 总体性能汇总 " %+% "=" %+% "\n")

for (size in c("0.5GB", "5GB")) {
  cat(sprintf("\n--- %s 数据集汇总 ---\n", size))
  
  # chDB 汇总
  chdb_size_data <- if (nrow(chdb_target) > 0) {
    chdb_target[size_label == size]
  } else { data.table() }
  
  if (nrow(chdb_size_data) > 0) {
    chdb_total_time <- sum(chdb_size_data$time_sec_1 + chdb_size_data$time_sec_2, na.rm = TRUE)
    chdb_avg_time <- mean((chdb_size_data$time_sec_1 + chdb_size_data$time_sec_2)/2, na.rm = TRUE)
    cat(sprintf("chDB - 总查询数: %d, 总时间: %.4f秒, 平均时间: %.4f秒\n", 
                nrow(chdb_size_data), chdb_total_time, chdb_avg_time))
  } else {
    cat("chDB - 无数据\n")
  }
  
  # ClickHouse 汇总
  clickhouse_size_data <- if (nrow(clickhouse_target) > 0) {
    clickhouse_target[size_label == size]
  } else { data.table() }
  
  if (nrow(clickhouse_size_data) > 0) {
    clickhouse_total_time <- sum(clickhouse_size_data$time_sec_1 + clickhouse_size_data$time_sec_2, na.rm = TRUE)
    clickhouse_avg_time <- mean((clickhouse_size_data$time_sec_1 + clickhouse_size_data$time_sec_2)/2, na.rm = TRUE)
    cat(sprintf("ClickHouse - 总查询数: %d, 总时间: %.4f秒, 平均时间: %.4f秒\n", 
                nrow(clickhouse_size_data), clickhouse_total_time, clickhouse_avg_time))
  } else {
    cat("ClickHouse - 无数据\n")
  }
}
