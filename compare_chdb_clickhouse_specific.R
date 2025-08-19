#!/usr/bin/env Rscript

# chDB vs ClickHouse 性能对比 - c6id.metal 机器上 0.5G 和 5G 数据集
library(data.table)
library(ggplot2)

# 加载数据
source("_report/report.R")
ld <- time_logs()

# 筛选 chDB 和 ClickHouse 数据
chdb_data <- ld[solution == "chdb"]
clickhouse_data <- ld[solution == "clickhouse"]

cat("Available solutions in dataset:\n")
print(unique(ld$solution))

cat("\nchDB records:", nrow(chdb_data))
cat("\nClickHouse records:", nrow(clickhouse_data))

# 合并数据进行对比
comparison_data <- rbind(chdb_data, clickhouse_data)

# 筛选 c6id.metal 机器和指定数据集大小
target_data <- comparison_data[
  machine_type == "c6id.metal" & 
  in_rows %in% c("1e7", "1e8")  # 1e7=0.5G, 1e8=5G
]

cat("\nFiltered data for c6id.metal with 0.5G and 5G datasets:", nrow(target_data))

if (nrow(target_data) == 0) {
  cat("\nNo data found for the specified criteria. Available combinations:\n")
  print(unique(comparison_data[, .(solution, machine_type, in_rows)]))
  stop("Please check if the benchmarks have been run for the specified configurations.")
}

# 数据集大小映射
size_mapping <- data.table(
  in_rows = c("1e7", "1e8", "1e9"),
  size_label = c("0.5GB", "5GB", "50GB")
)

target_data <- merge(target_data, size_mapping, by = "in_rows")

# 1. 总体性能对比
cat("\n=== 总体性能对比 (c6id.metal) ===\n")
overall_stats <- target_data[, .(
  avg_time_run1 = mean(time_sec_1, na.rm = TRUE),
  avg_time_run2 = mean(time_sec_2, na.rm = TRUE),
  total_queries = .N,
  avg_memory = mean((mem_gb_1 + mem_gb_2)/2, na.rm = TRUE)
), by = .(solution, size_label)]

overall_stats[, avg_time_total := (avg_time_run1 + avg_time_run2) / 2]
print(overall_stats[, .(solution, size_label, avg_time_total, total_queries, avg_memory)])

# 2. 按数据集大小详细对比
cat("\n=== 按数据集大小详细对比 ===\n")
for (size in c("0.5GB", "5GB")) {
  cat(sprintf("\n--- %s 数据集 ---\n", size))
  
  size_data <- target_data[size_label == size]
  
  if (nrow(size_data) > 0) {
    # 按查询类型汇总
    query_stats <- size_data[, .(
      avg_time = mean((time_sec_1 + time_sec_2)/2, na.rm = TRUE),
      min_time = min(c(time_sec_1, time_sec_2), na.rm = TRUE),
      max_time = max(c(time_sec_1, time_sec_2), na.rm = TRUE),
      avg_memory = mean((mem_gb_1 + mem_gb_2)/2, na.rm = TRUE)
    ), by = .(solution, question)]
    
    # 转换为宽格式便于对比
    time_wide <- dcast(query_stats, question ~ solution, value.var = "avg_time")
    
    if ("chdb" %in% names(time_wide) && "clickhouse" %in% names(time_wide)) {
      time_wide[, speedup_ratio := clickhouse / chdb]
      time_wide[, performance_diff := chdb - clickhouse]
      
      print(time_wide[, .(
        query = question, 
        chdb_time = round(chdb, 4), 
        clickhouse_time = round(clickhouse, 4),
        speedup_ratio = round(speedup_ratio, 2),
        diff_seconds = round(performance_diff, 4)
      )])
      
      # 汇总统计
      cat(sprintf("\n%s 数据集汇总:\n", size))
      cat(sprintf("chDB 平均时间: %.4f 秒\n", mean(time_wide$chdb, na.rm = TRUE)))
      cat(sprintf("ClickHouse 平均时间: %.4f 秒\n", mean(time_wide$clickhouse, na.rm = TRUE)))
      cat(sprintf("平均加速比 (ClickHouse/chDB): %.2fx\n", mean(time_wide$speedup_ratio, na.rm = TRUE)))
    }
  } else {
    cat("No data available for this dataset size.\n")
  }
}

# 3. 内存使用对比
cat("\n=== 内存使用对比 ===\n")
memory_stats <- target_data[, .(
  avg_memory_usage = mean((mem_gb_1 + mem_gb_2)/2, na.rm = TRUE),
  max_memory_usage = max(c(mem_gb_1, mem_gb_2), na.rm = TRUE)
), by = .(solution, size_label)]

print(memory_stats)

# 4. 生成可视化图表
create_performance_plots <- function(data) {
  if (nrow(data) == 0) return()
  
  # 准备绘图数据
  plot_data <- data[, .(
    solution = solution,
    size_label = size_label,
    question = question,
    avg_time = (time_sec_1 + time_sec_2) / 2,
    avg_memory = (mem_gb_1 + mem_gb_2) / 2
  )]
  
  # 执行时间对比图
  if (length(unique(plot_data$solution)) >= 2) {
    p1 <- ggplot(plot_data, aes(x = question, y = avg_time, fill = solution)) +
      geom_bar(stat = "identity", position = "dodge") +
      facet_wrap(~size_label, scales = "free_y") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      labs(title = "chDB vs ClickHouse - 执行时间对比 (c6id.metal)",
           subtitle = "按数据集大小分组",
           x = "查询类型", y = "平均执行时间 (秒)",
           fill = "数据库引擎") +
      scale_fill_manual(values = c("chdb" = "#FF6B6B", "clickhouse" = "#4ECDC4"))
    
    ggsave("chdb_vs_clickhouse_time_c6id_metal.png", p1, width = 14, height = 8, dpi = 300)
    cat("已保存执行时间对比图: chdb_vs_clickhouse_time_c6id_metal.png\n")
    
    # 内存使用对比图
    p2 <- ggplot(plot_data, aes(x = question, y = avg_memory, fill = solution)) +
      geom_bar(stat = "identity", position = "dodge") +
      facet_wrap(~size_label, scales = "free_y") +
      theme_minimal() +
      theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
      labs(title = "chDB vs ClickHouse - 内存使用对比 (c6id.metal)",
           subtitle = "按数据集大小分组",
           x = "查询类型", y = "平均内存使用 (GB)",
           fill = "数据库引擎") +
      scale_fill_manual(values = c("chdb" = "#FF6B6B", "clickhouse" = "#4ECDC4"))
    
    ggsave("chdb_vs_clickhouse_memory_c6id_metal.png", p2, width = 14, height = 8, dpi = 300)
    cat("已保存内存使用对比图: chdb_vs_clickhouse_memory_c6id_metal.png\n")
  }
}

# 生成图表
create_performance_plots(target_data)

# 5. 导出详细数据
detailed_export <- target_data[, .(
  solution, size_label, task, question, data,
  time_run1 = time_sec_1,
  time_run2 = time_sec_2,
  avg_time = (time_sec_1 + time_sec_2) / 2,
  memory_run1 = mem_gb_1,
  memory_run2 = mem_gb_2,
  avg_memory = (mem_gb_1 + mem_gb_2) / 2,
  version, batch
)]

fwrite(detailed_export, "chdb_clickhouse_c6id_metal_detailed.csv")
cat("已导出详细数据: chdb_clickhouse_c6id_metal_detailed.csv\n")

# 6. 性能摘要报告
cat("\n=== 性能摘要报告 ===\n")
summary_report <- target_data[, .(
  total_time = sum(time_sec_1 + time_sec_2, na.rm = TRUE),
  query_count = .N,
  avg_query_time = mean((time_sec_1 + time_sec_2)/2, na.rm = TRUE),
  total_memory = mean((mem_gb_1 + mem_gb_2)/2, na.rm = TRUE)
), by = .(solution, size_label)]

print(summary_report)

cat("\n=== 分析完成 ===\n")
cat("生成的文件:\n")
cat("- chdb_vs_clickhouse_time_c6id_metal.png\n")
cat("- chdb_vs_clickhouse_memory_c6id_metal.png\n")
cat("- chdb_clickhouse_c6id_metal_detailed.csv\n")
