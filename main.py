import os
import io
import json
import psycopg2
import requests
import argparse
import pandas as pd

from urllib.parse import urljoin


def get_api_data(endpoint, filters_list=None):
    base_url = 'https://ghoapi.azureedge.net/api/'
    
    if not filters_list:
        url = urljoin(base_url, endpoint)
    else:
        filters = ' and '.join(filters_list)
        url = f"{urljoin(base_url, endpoint)}?$filter={filters}"

    print(url)

    try:
        resp_json = requests.get(url).json()
        data = resp_json['value']
    except KeyError as e:
        print('API get failed, response:', json.dumps(resp_json, indent=4, default=str), sep='\n')
        raise(e)

    return data


def run_pg_query(query, **kwargs):
    creds = eval(os.environ.get('PG_CREDS'))
    conn_string = f"host={creds['host']} port={creds['port']} dbname={creds['dbname']} user={creds['user']} password={creds['password']} connect_timeout=10"

    with psycopg2.connect(conn_string) as conn:
        with conn.cursor as cur:
            cur.execute(query, kwargs)

            return cur.fetchall()


def write_df_to_db(df, schema, table):
    sio = io.StringIO()
    df.to_csv(sio, sep='|', header=False, index=False)
    sio.seek(0)
    query = f"COPY {schema}.{table} FROM STDIN WITH CSV DELIMITER '|'"
    run_pg_query(query, file=sio)
            

def get_max_date(schema, table, date_column='year'):
    query = f"select coalesce(max({date_column}, '1900') from {schema}.{table}"
    max_date = run_pg_query(query)[0]

    return f'{max_date}-01-01'


def process_data(schema, table, delta=True):
    countries = get_api_data('DIMENSION/COUNTRY/DimensionValues', ["ParentCode eq 'AFR'"])
    genders = get_api_data('DIMENSION/SEX/DimensionValues')

    inds = get_api_data('Indicator', ["contains(IndicatorName,'Under-five')", "IndicatorCode ne 'MALARIA004'", "IndicatorCode ne 'u5mr'"])

    join_cols = ['country_code', 'Dim1', 'year']
    columns = ['SpatialDim', 'Dim1', 'NumericValue', 'TimeDimensionValue']

    codes = {'MDG_0000000007': 'mortality_per_1000', 'CM_01': 'num_deaths'}
    joined_mr_df = pd.DataFrame(columns=join_cols)

    indicator_filters = ["ParentLocationCode eq 'AFR'", "TimeDimType eq 'YEAR'", "Dim2 eq 'AGEGROUP_YEARSUNDER5'"]

    if delta:
        max_year = get_max_date(schema, table)
        delta_filter = f"date(TimeDimensionBegin) gt '{max_year}'"
        indicator_filters.append(delta_filter)

    for d in inds:
        code = d['IndicatorCode']
        name = d['IndicatorName']

        rename_dict = {
            'NumericValue': codes[code],
            'SpatialDim': 'country_code',
            'TimeDimensionValue': 'year'
            }
        tmp_df = pd.DataFrame(get_api_data(code, indicator_filters))
        tmp_df = tmp_df[columns]
        tmp_df.rename(columns=rename_dict, inplace=True)
        joined_mr_df = pd.merge(joined_mr_df, tmp_df, how='outer', on=join_cols)

    dim_cols = ['Code', 'Title']

    gender_df = pd.DataFrame(genders)[dim_cols]
    countries_df = pd.DataFrame(countries)[dim_cols]

    print(countries_df.head())
    joined_mr_df = pd.merge(joined_mr_df, gender_df, how='inner', left_on='Dim1', right_on='Code')
    joined_mr_df.rename(columns={'Title': 'sex'}, inplace=True)
    joined_mr_df.drop(columns=['Code', 'Dim1'], inplace=True)

    joined_mr_df = pd.merge(joined_mr_df, countries_df, how='inner', left_on='country_code', right_on='Code')
    joined_mr_df.rename(columns={'Title': 'country'}, inplace=True)
    joined_mr_df.drop(columns=['Code'], inplace=True)

    # change probablity of dying by age 5 per 100 to percentage
    joined_mr_df['mortality_perc'] = joined_mr_df['mortality_per_1000'].apply(lambda x: x/1000)

    # write final df to postgres db
    # pulls creds from environment variable called PG_CREDS; stored as json
    write_df_to_db(joined_mr_df, schema=schema, table=table)
    print('Writing to database complete...')


if __name__ == 'main':
    parser = argparse.ArgumentParser()
    parser.add_argument('--delta', default=True, help="Specifies whether to fetch the latest data only. Default True")
    parser.add_argument('--schema', default='mortality_data', help="destination schema for the data")
    parser.add_argument('--table', default='mortality_rates_africa', help="destination table for the data")
    args = parser.parse_args()

    process_data(args.schema, args.table, delta=args.delta)
