loop_benchplot = function(dt_task, report_name, syntax.dict, exceptions, solution.dict, question.txt.fun = NULL, title.txt.fun = NULL, data_namev, q_groupv, cutoff=NULL, pending=NULL, machine_types) {
  for (data_name in data_namev) {
    for (m_type in machine_types) {
      for (q_group in q_groupv) {
        message(sprintf("benchplot %s %s %s %s", report_name, data_name, q_group, m_type))
        benchplot(
          x = dt_task[data==data_name & question_group==q_group & machine_type==m_type],
          filename = file.path("public", report_name, sprintf("%s_%s_%s.png", data_name, q_group, m_type)),
          solution.dict = solution.dict,
          syntax.dict = syntax.dict,
          exceptions = exceptions,
          question.txt.fun = question.txt.fun,
          title.txt.fun = title.txt.fun,
          cutoff = cutoff,
          pending = pending,
          url.footer = "https://duckdblabs.github.io/db-benchmark",
          interactive = FALSE
        )
      }
    }
  }
}

source("./_report/report.R", chdir=TRUE)
source("./_helpers/helpers.R", chdir=TRUE)
source("./_benchplot/benchplot.R", chdir=TRUE)
source("./_benchplot/benchplot-dict.R", chdir=TRUE)
ld = time_logs()

lld = ld[script_recent==TRUE]
# lld_nodename = as.character(unique(lld$nodename))
lld_nodename = "c6id.metal"
if (length(lld_nodename)>1L)
  stop(sprintf("There are multiple different 'nodename' to be presented on single report '%s'", report_name))
lld_unfinished = lld[is.na(script_time_sec)]
if (nrow(lld_unfinished)) {
  warning(sprintf("Missing solution finish timestamp in logs.csv for '%s' (still running or launcher script killed): %s", paste(unique(lld_unfinished$task), collapse=","), paste(unique(lld_unfinished$solution), collapse=", ")))
}