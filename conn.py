import oracledb
import snowflake.connector
import pandas as pd
import configparser
import os
import sys
import importlib.util


def get_config(config_file):
    # checks if the config file path exist or not
    if not os.path.isfile(config_file):
        raise FileNotFoundError(f"The config file {config_file} not exist")
    # Reads the config if file exist
    config = configparser.ConfigParser()
    config.read(config_file, encoding='utf-8')
    print("Config file Parsed Successfully")
    return config


# This function is used to import constants dynamically
def dynamic_import(file_name):
    # Adding extension to file name
    file_name_with_extension = f"{file_name}.py"

    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(current_dir, file_name_with_extension)

    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"The constants file {config_file} not exist")

    # Load module from specific file path
    try:
        spec = importlib.util.spec_from_file_location(file_name, file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules[file_name] = module
        print(f"Module '{file_name}' imported Successfully")
        return module
    except Exception as e:
        print(f"Error importing module '{file_path}': {e}")
        return None


def get_oracle_connection(database_config):
    try:
        print("Establishing Oracle connection")

        dsn_string = "{host}:{port}/{service_name}".format(host=database_config['Oracle']['host'],
                                                           port=database_config['Oracle']['port'],
                                                           service_name=database_config['Oracle']['service_name'])
        connection = oracledb.connect(user=database_config['Oracle']['user'],
                                      password=database_config['Oracle']['password'],
                                      dsn=dsn_string
                                      )

        cursor = connection.cursor()
        print("Oracle connection established")
        return connection, cursor

    except Exception as e:
        print("Error in establishing oracle connection: ", e)


def get_snowflake_connection(database_config):
    try:
        print("Establishing snowflake connection")
        snow_flake_connection = snowflake.connector.connect(
            user=database_config['Snowflake']['user'],
            password=database_config['Snowflake']['password'],
            account=database_config['Snowflake']['account'],
            warehouse=database_config['Snowflake']['warehouse'],
            database=database_config['Snowflake']['database'],
            schema=database_config['Snowflake']['schema'],
            role=database_config['Snowflake']['role']
        )
        snowflake_cursor = snow_flake_connection.cursor()
        print("Snowflake connection Established")
        return snow_flake_connection, snowflake_cursor
    except Exception as e:
        print("Error occured while connecting snowflake db:", e)


def get_df_from_query_result(connection, cursor, query, flag):
    """  """
    try:
        print("Executing query")
        cursor.execute(query)
        rows = cursor.fetchall()
        # get column names
        print("Converting to dataframe")
        columns = [i[0] for i in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

        if constants_module.ORACLE_IGNORE_COLUMNS and flag == 'oracle':
            ignore_columns = constants_module.ORACLE_IGNORE_COLUMNS
            df = df.drop(columns=ignore_columns)
        if constants_module.SNOWFLAKE_IGNORE_COLUMNS and flag == "snowflake":
            ignore_columns = constants_module.SNOWFLAKE_IGNORE_COLUMNS
            df = df.drop(columns=ignore_columns)

        # identify string, integer and float columns separately to handle null values
        string_columns = df.select_dtypes(include=['object']).columns
        df[string_columns] = df[string_columns].fillna("")
        float_columns = df.select_dtypes(include=['float64']).columns
        df[float_columns] = df[float_columns]
        df[constants_module.FLOAT_TO_INT_COLUMNS] = df[constants_module.FLOAT_TO_INT_COLUMNS].round()
        integer_columns = df.select_dtypes(include="int64").columns
        df[integer_columns] = df[integer_columns].fillna(0)

        # Formatting timestamp columns
        if constants_module.TTIMESTAMP_COLUMNS:
            df[constants_module.TIMESTAMP_COLUMNS] = df[constants_module.TIMESTAMP_COLUMNS].apply(
                lambda col: pd.to_datetime(col, errors="coerce").dt.strftime('%Y-%m-%d %H:%M:%S'))
        if constants_module.DATE_COLUMNS:
            df[constants_module.DATE_COLUMNS] = df[constants_module.DATE_COLUMNS].apply(
                lambda col: pd.to_datetime(col, errors="coerce").dt.strftime('%Y-%m-%d'))
        print("Closing Db connection")
        cursor.close()
        connection.close()
        return df

    except Exception as e:
        print("Error in Query execution: ", e)


# as snowflake float columns appear as string values,
# this function is used to convert those columns into float data type
def convert_to_float_data_type(dataframe):
    try:
        if constants_module.SNOWFLAKE_FLOAT_COLUMNS:
            for column in constants_module.SNOWFLAKE_FLOAT_COLUMNS:
                dataframe[column] = pd.to_numeric(dataframe[column], errors="coerce")
            dataframe[constants_module.SNOWFLAKE_FLOAT_COLUMNS] = dataframe[constants_module.SNOWFLAKE_FLOAT_COLUMNS]
        return dataframe
    except Exception as e:
        print("Error while converting to float type: ", e)


def compare_data(config):
    oracle_conn, cursor = get_oracle_connection(config)
    snowflake_conn, snowflake_cursor = get_snowflake_connection(config)
    oracle_data = get_df_from_query_result(oracle_conn, cursor, constants_module.ORACLE_QUERY, "oracle")
    snowflake_data = get_df_from_query_result(snowflake_conn, snowflake_cursor,
                                              constants_module.SNOWFLAKE_QUERY, "snowflake")
    snowflake_corrected_data = convert_to_float_data_type(snowflake_data)
    snowflake_corrected_data = snowflake_corrected_data[oracle_data.columns]

    # comparing both the data sets
    comparison = oracle_data.compare(snowflake_corrected_data, keep_equal=True)
    mismatch_rows = comparison[comparison.notna().any(axis=1)]
    mismatch_rows = mismatch_rows.reset_index()
    mismatch_rows_with_id = oracle_data.loc[mismatch_rows['index'], ["HEADER_ID", "ENTITY_ID"]].reset_index(drop=True)
    final_result = pd.concat([mismatch_rows_with_id, mismatch_rows.drop(columns=['index'])], axis=1)
    # Enabling this print mismatch columns from both the data sets
    final_result.columns = [f"{col[0]}{'_table_1'}" if col[1] == 'self' else f"{col[0]}{'_table_2'}"
                            for col in final_result.columns]

    if not final_result.empty:
        print("Rows with non-matching values")
        pd.set_option("display.max_columns", None)
        print(final_result)
    else:
        print("The data matched")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("please execute the script with file_name and config name")
    else:
        file_name = sys.argv[1]
        config_file = sys.argv[2]
        constants_module = dynamic_import(file_name)
        config = get_config(config_file + ".cfg")
        compare_data(config)


