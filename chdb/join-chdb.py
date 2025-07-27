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
machine_type = os.environ["MACHINE_TYPE"]
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
  conn = chdb.connect(chdb_join_db) # TODO: check if the database should be created first
else:
  print("using in-memory data storage")
  conn = chdb.connect(":memory:")

# reading data
cur = conn.cursor()
engine_type = 'LOG'
cur.query("CREATE DATABASE IF NOT EXISTS db_benchmark ENGINE = Atomic")
cur.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.x id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 Nullable(String), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(Int32), v1 Nullable(Int32), v2 Nullable(Int32), v3 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")
cur.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.small id1 LowCardinality(Nullable(String)), id4 Nullable(Int32), v2 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")
cur.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.medium id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id4 Nullable(Int32), id5 Nullable(Int32), v2 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")
cur.query(f"CREATE TABLE IF NOT EXISTS db_benchmark.big id1 LowCardinality(Nullable(String)), id2 LowCardinality(Nullable(String)), id3 LowCardinality(Nullable(String)), id4 Nullable(Int32), id5 Nullable(Int32), id6 Nullable(String), v2 Nullable(Float64)) ENGINE = {engine_type} ORDER BY tuple()")

cur.query(f"INSERT INTO db_benchmark.x FROM INFILE '{src_jn_x}'")
cur.query(f"INSERT INTO db_benchmark.small FROM INFILE '{src_jn_y[0]}'")
cur.query(f"INSERT INTO db_benchmark.medium FROM INFILE '{src_jn_y[1]}'")
cur.query(f"INSERT INTO db_benchmark.big FROM INFILE '{src_jn_y[2]}'")

print(cur.query("SELECT count(*) from db_benchmark.x"))
print(cur.query("SELECT count(*) from db_benchmark.small"))
print(cur.query("SELECT count(*) from db_benchmark.medium"))
print(cur.query("SELECT count(*) from db_benchmark.big"))

in_rows = cur.query("SELECT count(*) from db_benchmark.x")

task_init = timeit.default_timer()
print("joining...", flush=True)

question = "small inner on int" # q1
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x AS x INNER JOIN small AS y USING (id1)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
cur.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, small.id4 AS small_id4, v2 FROM x AS x INNER JOIN small AS y USING (id1)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(cur.query("SELECT * FROM ans LIMIT 3"))
print(cur.query("SELECT * FROM ans ORDER BY _part_offset DESC LIMIT 3"))
cur.query("DROP TABLE IF EXISTS ans")

question = "medium inner on int" # q2
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM x AS x INNER JOIN medium AS y USING (id2)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
cur.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM x AS x INNER JOIN medium AS y USING (id2)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(cur.query("SELECT * FROM ans LIMIT 3"))
print(cur.query("SELECT * FROM ans ORDER BY _part_offset DESC LIMIT 3"))
cur.query("DROP TABLE IF EXISTS ans")

question = "medium outer on int" # q3
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM x AS x LEFT JOIN medium AS y USING (id2)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
cur.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id4 AS medium_id4, medium.id5 as medium_id5, v2 FROM x AS x LEFT JOIN medium AS y USING (id2)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(cur.query("SELECT * FROM ans LIMIT 3"))
print(cur.query("SELECT * FROM ans ORDER BY _part_offset DESC LIMIT 3"))
cur.query("DROP TABLE IF EXISTS ans")

question = "medium inner on factor" # q4
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 as medium_id4, v2 FROM x AS x INNER JOIN medium AS y USING (id5)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
cur.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, medium.id1 AS medium_id1, medium.id2 AS medium_id2, medium.id4 as medium_id4, v2 FROM x AS x INNER JOIN medium AS y USING (id5)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(cur.query("SELECT * FROM ans LIMIT 3"))
print(cur.query("SELECT * FROM ans ORDER BY _part_offset DESC LIMIT 3"))
cur.query("DROP TABLE IF EXISTS ans")

question = "big inner on int" # q5
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 as big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x AS x INNER JOIN big AS y USING (id3)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
cur.query("DROP TABLE IF EXISTS ans")
gc.collect()
t_start = timeit.default_timer()
QUERY=f"CREATE TABLE ans AS SELECT x.*, big.id1 AS big_id1, big.id2 AS big_id2, big.id4 as big_id4, big.id5 AS big_id5, big.id6 AS big_id6, v2 FROM x AS x INNER JOIN big AS y USING (id3)"
cur.query(QUERY)
nr=cur.query("SELECT count(*) AS cnt FROM ans")
nc=cur.query("SELECT * FROM ans LIMIT 0")
print(nr,nc, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
chk = cur.query("SELECT SUM(v1) AS v1, SUM(v2) as v2 FROM ans")
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=nr, out_cols=nc, solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk(chk), chk_time_sec=chkt, on_disk=on_disk, machine_type=machine_type)
print(cur.query("SELECT * FROM ans LIMIT 3"))
print(cur.query("SELECT * FROM ans ORDER BY _part_offset DESC LIMIT 3"))
cur.query("DROP TABLE IF EXISTS ans")

print("joining finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
