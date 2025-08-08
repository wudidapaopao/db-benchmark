#!/usr/bin/env python3

print("# join-chdb.py", flush=True)

import os
import gc
import timeit

import chdb

exec(open("./_helpers/helpers.py").read())

ver = '.'.join(chdb.chdb_version)
task = "join"
git = ""
solution = "chdb"
fun = ".join"
cache = "TRUE"
on_disk = "FALSE"


data_name = os.environ["SRC_DATANAME"]
#machine_type = os.environ["MACHINE_TYPE"]
machine_type = 'local'
src_jn_x = os.path.join("data", data_name + ".csv")
y_data_name = join_to_tbls(data_name)
src_jn_y = [os.path.join("data", y_data_name[0] + ".csv"), os.path.join("data", y_data_name[1] + ".csv"), os.path.join("data", y_data_name[2] + ".csv")]
if len(src_jn_y) != 3:
  raise Exception("Something went wrong in preparing files used for join")


chdb_join_db = f'{solution}_{task}_{data_name}.chdb'
scale_factor = data_name.replace("J1_","")[:4].replace("_", "")
on_disk = 'TRUE' if (machine_type == "c6id.4xlarge" and float(scale_factor) >= 1e9) else 'FALSE'

print("loading datasets " + data_name + ", " + y_data_name[0] + ", " + y_data_name[2] + ", " + y_data_name[2], flush=True)

if on_disk:
  print("using disk memory-mapped data storage")
  conn = chdb.session.Session(chdb_join_db) # TODO: check if the database should be created first
else:
  print("using in-memory data storage")
  conn = chdb.session.Session("")

# TODO: add logic for is_sorted, has_na

# reading data
engine_type = 'MergeTree()'
conn.query("CREATE DATABASE IF NOT EXISTS db_benchmark ENGINE = Atomic")
conn.query("DROP TABLE IF EXISTS db_benchmark.x")
conn.query("DROP TABLE IF EXISTS db_benchmark.small")
conn.query("DROP TABLE IF EXISTS db_benchmark.medium")
conn.query("DROP TABLE IF EXISTS db_benchmark.big")
conn.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.x (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v1 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")
conn.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.small (id1 Nullable(Int32), id4 Nullable(String), v2 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")
conn.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.medium (id1 Nullable(Int32), id2 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), v2 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")
conn.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.big (id1 Nullable(Int32), id2 Nullable(Int32), id3 Nullable(Int32), id4 Nullable(String), id5 Nullable(String), id6 Nullable(String), v2 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")

conn.query(f"INSERT INTO db_benchmark.x FROM INFILE '{src_jn_x}'")
conn.query(f"INSERT INTO db_benchmark.small FROM INFILE '{src_jn_y[0]}'")
conn.query(f"INSERT INTO db_benchmark.medium FROM INFILE '{src_jn_y[1]}'")
conn.query(f"INSERT INTO db_benchmark.big FROM INFILE '{src_jn_y[2]}'")

print(conn.query("SELECT count(*) from db_benchmark.x"))
print(conn.query("SELECT count(*) from db_benchmark.small"))
print(conn.query("SELECT count(*) from db_benchmark.medium"))
print(conn.query("SELECT count(*) from db_benchmark.big"))

in_rows = conn.query("SELECT count(*) from db_benchmark.x")

task_init = timeit.default_timer()
print("joining...", flush=True)

query_engine = 'ENGINE = Memory'
question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, small.id4 AS small_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.small AS small USING (id1)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, small.id4 AS small_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.small AS small USING (id1)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id2)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id2)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x LEFT JOIN db_benchmark.medium AS medium USING (id2)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM db_benchmark.x AS x LEFT JOIN db_benchmark.medium AS medium USING (id2)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 as medium_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id5)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 as medium_id4, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.medium AS medium USING (id5)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 as big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.big AS big USING (id3)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
conn.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans {query_engine} AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 as big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM db_benchmark.x AS x INNER JOIN db_benchmark.big AS big USING (id3)"
conn.query(QUERY)
nr=str(conn.query("SELECT count(*) AS cnt FROM ans"))
nc=str(conn.query("SELECT * FROM ans LIMIT 0"))
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = [conn.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(conn.query("SELECT * FROM ans LIMIT 3"), flush=True)
print(conn.query(f"SELECT * FROM ans LIMIT {int(nr) - 3}, 3"), flush=True)
conn.query("DROP TABLE IF EXISTS ans")

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

conn.close()
exit(0)
