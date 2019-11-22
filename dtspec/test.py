import os
import yaml
import json
import logging
import math

import dbt
import dbt.main
import sqlalchemy as sa
import pandas as pd

import dtspec

logging.basicConfig()
logging.getLogger("dbt").setLevel(logging.INFO)

LOG = logging.getLogger('sqlalchemy.engine')
LOG.setLevel(logging.ERROR)


with open(os.path.join(os.getenv('HOME'), '.dbt', 'profiles.yml')) as f:
    DBT_PROFILE = yaml.safe_load(f)['jaffle_shop']['outputs']['dev']

SA_ENGINE = sa.create_engine(
    'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}'.format(
        user=DBT_PROFILE['user'],
        password=DBT_PROFILE['pass'],
        host=DBT_PROFILE['host'],
        port=DBT_PROFILE['port'],
        dbname=DBT_PROFILE['dbname'],
    )
)

def clean_test_data(api):
    sqls = [
        f'TRUNCATE {source}' for source in api.spec['sources'].keys()
    ] + [
        f'TRUNCATE {target}' for target in api.spec['targets'].keys()
    ]

    with SA_ENGINE.connect() as conn:
        conn.execute(';\n'.join(sqls))

def load_sources(api):
    metadata = sa.MetaData()

    for source, data in api.spec['sources'].items():
        sa_table = sa.Table(
            source,
            metadata,
            autoload=True,
            autoload_with=SA_ENGINE,
            schema=DBT_PROFILE['schema']
        )

        with SA_ENGINE.connect() as conn:
            serialized_data = data.serialize()
            if len(serialized_data) == 0:
                continue
            sa_insert = sa_table.insert().values(serialized_data)
            conn.execute(sa_insert)

class DbtRunError(Exception): pass
def run_dbt():
    dbt_args = ['run', '--project-dir', os.path.join(os.path.dirname(__file__), '..')]
    _, success = dbt.main.handle_and_check(dbt_args)
    if not success:
        raise DbtRunError('dbt failed to run successfully, please see log for details')

def _is_nan(value):
    try:
        return math.isnan(value)
    except TypeError:
        return False


def _is_null(value):
    return value in [None, pd.NaT] or _is_nan(value)


def _stringify_sa(df, sa_table):
    for col in sa_table.columns:
        if col.type.python_type is int:
            df[col.name] = df[col.name].apply(lambda v: str(int(v)) if not pd.isna(v) else None)

    nulls_df = df.applymap(_is_null)
    str_df = df.astype({column: str for column in df.columns})

    def _replace_nulls(series1, series2):
        return series1.combine(series2, lambda value1, value2: value1 if not value2 else '{NULL}')

    return str_df.combine(nulls_df, _replace_nulls)


def load_actuals(api):
    metadata = sa.MetaData()
    serialized_actuals = {}
    with SA_ENGINE.connect() as conn:
        for target, _data in api.spec['targets'].items():
            sa_table = sa.Table(
                target,
                metadata,
                autoload=True,
                autoload_with=SA_ENGINE,
                schema=DBT_PROFILE['schema']
            )
            df = _stringify_sa(
                pd.read_sql_table(target, conn, schema=DBT_PROFILE['schema']),
                sa_table
            )

            serialized_actuals[target] = {
                "records": json.loads(df.to_json(orient="records")),
                "columns": list(df.columns),
            }
    api.load_actuals(serialized_actuals)



def test_dtspec():
    with open(os.path.join(os.path.dirname(__file__), 'spec.yml')) as f:
        api = dtspec.api.Api(yaml.safe_load(f))

    api.generate_sources()
    clean_test_data(api)
    load_sources(api)
    run_dbt()
    load_actuals(api)
    api.assert_expectations()

if __name__ == '__main__':
    test_dtspec()
