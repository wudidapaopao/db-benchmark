#!/usr/bin/env python3

import os
import sys
import timeit
import pandas as pd
import dask as dk
import dask.dataframe as dd
import logging
from typing import Any

# Import needed utilities
THIS_DIR = os.path.abspath(
    os.path.basename(__file__)
)
HELPERS_DIR = os.path.abspath(
    os.path.join(
        THIS_DIR, '../_helpers'
    )
)
sys.path.extend((THIS_DIR, HELPERS_DIR))
from helpers import *
from common import Query, QueryRunner, dask_client

logging.basicConfig(
    level=logging.INFO,
    format='{ %(name)s:%(lineno)d @ %(asctime)s } - %(message)s'
)
logger = logging.getLogger(__name__)

# TODO: Case
ver = dk.__version__
git = dk.__git_revision__
task = "groupby"
solution = "dask"
fun = ".groupby"
cache = "TRUE"

def load_dataset(src_grp: str) -> dd.DataFrame:
    logger.info("Loading dataset %s" % data_name)
    x = dd.read_csv(
        src_grp,
        dtype={"id1":"category","id2":"category","id3":"category","id4":"Int32","id5":"Int32","id6":"Int32","v1":"Int32","v2":"Int32","v3":"float64"},
        engine="pyarrow"
    )
    x = x.persist()
    return x

class QueryOne(Query):
    question = "sum v1 by id1"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id1', dropna=False, observed=True).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum()]

class QueryTwo(Query):
    question = "sum v1 by id1:id2"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby(['id1','id2'], dropna=False, observed=True).agg({'v1':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum()]

class QueryThree(Query):
    question = "sum v1 mean v3 by id3"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'sum', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum(), ans.v3.sum()]

class QueryFour(Query):
    question = "mean v1:v3 by id4"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id4', dropna=False, observed=True).agg({'v1':'mean', 'v2':'mean', 'v3':'mean'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]

class QueryFive(Query):
    question = "sum v1:v3 by id6"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id6', dropna=False, observed=True).agg({'v1':'sum', 'v2':'sum', 'v3':'sum'}).compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v1.sum(), ans.v2.sum(), ans.v3.sum()]

class QuerySix(Query):
    question = "median v3 sd v3 by id4 id5"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby(['id4','id5'], dropna=False, observed=True).agg({'v3': ['median','std']}, shuffle='p2p').compute()
        ans.reset_index(inplace=True) # #68
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v3']['median'].sum(), ans['v3']['std'].sum()]

class QuerySeven(Query):
    question = "max v1 - min v2 by id3"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x.groupby('id3', dropna=False, observed=True).agg({'v1':'max', 'v2':'min'}).assign(range_v1_v2=lambda x: x['v1']-x['v2'])[['range_v1_v2']].compute()
        ans.reset_index(inplace=True) # #68
        return ans


    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['range_v1_v2'].sum()]

class QueryEight(Query):
    question = "largest two v3 by id6"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x[~x['v3'].isna()][['id6','v3']].groupby('id6', dropna=False, observed=True).apply(lambda x: x.nlargest(2, columns='v3'), meta={'id6':'Int64', 'v3':'float64'})[['v3']].compute()
        ans.reset_index(level='id6', inplace=True)
        ans.reset_index(drop=True, inplace=True) # drop because nlargest creates some extra new index field
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['v3'].sum()]

class QueryNine(Query):
    question = "regression v1 v2 by id2 id4"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = x[['id2','id4','v1','v2']].groupby(['id2','id4'], dropna=False, observed=True).apply(lambda x: pd.Series({'r2': x.corr()['v1']['v2']**2}), meta={'r2':'float64'}).compute()
        ans.reset_index(inplace=True)
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans['r2'].sum()]

class QueryTen(Query):
    question = "sum v3 count by id1:id6"

    @staticmethod
    def query(x: dd.DataFrame) -> dd.DataFrame:
        ans = (
            x.groupby(
                ['id1', 'id2', 'id3', 'id4', 'id5', 'id6'],
                dropna=False,
                observed=True,
            )
            .agg({'v3': 'sum', 'v1': 'size'}, split_out=x.npartitions)
            .rename(columns={"v1": "count"})
            .compute()
        )
        ans.reset_index(inplace=True)
        return ans

    @staticmethod
    def check(ans: dd.DataFrame) -> Any:
        return [ans.v3.sum(), ans["count"].sum()]

def run_task(
    data_name: str,
    src_grp: str,
    machine_type: str
):
    runner = QueryRunner(
        task=task,
        solution=solution,
        solution_version=ver,
        solution_revision=git,
        fun=fun,
        cache=cache,
        on_disk=on_disk
    )

    client = dask_client()
    x = load_dataset(src_grp)
    in_rows = len(x)
    logger.info("Input dataset rows: %s" % in_rows)

    task_init = timeit.default_timer()
    logger.info("Grouping...")

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryOne,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryTwo,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryThree,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryFour,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryFive,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QuerySix,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QuerySeven,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryEight,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryNine,
        machine_type=machine_type,
    )

    runner.run_query(
        data_name=data_name,
        in_rows=in_rows,
        x=x,
        query=QueryTen,
        machine_type=machine_type,
    )

    logger.info("Grouping finished, took %0.fs" % (timeit.default_timer()-task_init))

if __name__ == '__main__':
    logger.info("# groupby-dask.py")
    data_name = os.environ['SRC_DATANAME']
    machine_type = os.environ['MACHINE_TYPE']
    on_disk = False #data_name.split("_")[1] == "1e9" # on-disk data storage #126
    on_disk = data_name.split("_")[1] == "1e9" and os.environ["MACHINE_TYPE"] == "c6id.4xlarge"
    fext = "parquet" if on_disk else "csv"
    src_grp = os.path.join("data", data_name+"."+fext)

    na_flag = int(data_name.split("_")[3])
    if na_flag > 0:
        logger.error("Skip due to na_flag>0: #171")
        exit(0) # not yet implemented #171, currently groupby's dropna=False argument is ignored

    run_task(
        data_name=data_name,
        src_grp=src_grp,
        machine_type=machine_type
    )