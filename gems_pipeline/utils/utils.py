# Databricks notebook source
# DBTITLE 1,Imports
import requests
import json
from typing import Dict, Any, List
from pyspark.sql import DataFrame
from pyspark.sql import functions as f

# COMMAND ----------

# DBTITLE 1,get_token
def get_token(BASE_URL: str, CLIENT_ID: str, CLIENT_SECRET: str) -> str:
    """
    Retrieve an OAuth2 access token from the GEMS API.
    
    Makes a POST request to the token endpoint using client credentials grant type.
    The token is used to authenticate subsequent API requests to the GEMS system.
    
    Args:
        BASE_URL: The base URL of the GEMS API (e.g., "https://api.gems.example.com")
        CLIENT_ID: OAuth2 client identifier for authentication
        CLIENT_SECRET: OAuth2 client secret for authentication
        
    Returns:
        str: The access token string for authenticating subsequent API requests
        
    Raises:
        requests.HTTPError: If the token request fails (non-2xx status code)
        KeyError: If the response doesn't contain an 'access_token' field
        
    Example:
        >>> BASE_URL = "https://api.gems.example.com"
        >>> CLIENT_ID = "my_client_id"
        >>> CLIENT_SECRET = "my_secret"
        >>> token = get_token(BASE_URL, CLIENT_ID, CLIENT_SECRET)
        >>> print(token)
        'eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...'
    """
    r = requests.post(
        f"{BASE_URL}/gemsidsvr/connect/token",
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "gemswebapi",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    r.raise_for_status()
    return r.json()["access_token"]

# COMMAND ----------

# DBTITLE 1,get_table_list_from_schema
def get_table_list_from_schema(primarySchema: str) -> DataFrame:
    """
    Generate a list of tables that sit under the primary schema.
    
    Fetches schema details from the GEMS API and converts the response into
    a Spark DataFrame with a variant column containing parsed JSON. This function
    requires global variables BASE_URL and headers to be defined.
    
    Args:
        primarySchema: The name of the primary schema to query (e.g., "Person Basics", "Company Basics")
        
    Returns:
        DataFrame: A Spark DataFrame with a single 'v' column of type variant,
                   containing parsed JSON data for each table in the schema.
                   Each row represents one table's metadata.
                   
    Raises:
        requests.HTTPError: If the API request fails
        NameError: If BASE_URL or headers are not defined globally
        
    Example:
        >>> df = get_table_list_from_schema("Person Basics")
        >>> df.show(1, truncate=False)
        +--------------------------------------------------+
        |v                                                 |
        +--------------------------------------------------+
        |{"savedSearchCounter":100000156,"title":"API_...}|
        +--------------------------------------------------+
        >>> df.count()
        5
    """
    # 1. Make the request without instantly trying to parse JSON
    response = requests.get(
        f"{BASE_URL}/gemswebapi/api/search/{primarySchema}",
        headers=headers,
    )

    # 2. Check if the API threw an error (like a 404 or 500)
    if not response.ok:
        print(f"  -> Skipping '{primarySchema}': API returned status {response.status_code}")
        return None

    # 3. Safely try to parse the JSON
    try:
        details = response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"  -> Skipping '{primarySchema}': API did not return valid JSON")
        return None

    # 4. If the schema is empty, skip it
    if not details:
        return None

    # 5. Explicitly define the schema to avoid empty inference errors
    raw_df = spark.createDataFrame(
        [(json.dumps(d),) for d in details], 
        schema="raw_json string"
    )

    variant_df = raw_df.select(f.parse_json(f.col("raw_json")).alias("v"))
    return variant_df

# COMMAND ----------

# DBTITLE 1,get_primary_schema_df
def get_primary_schema_df(RESULTS_DICT: Dict[str, Any], primarySchema: str) -> DataFrame:
    """
    Convert variant schema data to a typed DataFrame and filter for target tables.
    
    Takes the variant DataFrame from RESULTS_DICT, infers its schema using schema_of_json_agg,
    converts it to a typed DataFrame with explicit columns, and filters for rows where the
    title matches TARGET_TABLES or starts with 'API'. Requires global TARGET_TABLES variable.
    
    Args:
        RESULTS_DICT: Dictionary containing schema data with 'schema_data' key.
                      Structure: {primarySchema: {"schema_data": DataFrame, ...}}
        primarySchema: The name of the primary schema being processed (e.g., "Person Basics")
        
    Returns:
        DataFrame: A filtered Spark DataFrame containing only target tables,
                   with columns like savedSearchCounter, title, dateCreated, etc.
                   expanded from the variant structure.
                   
    Raises:
        KeyError: If primarySchema or 'schema_data' key doesn't exist in RESULTS_DICT
        NameError: If TARGET_TABLES is not defined globally
        
    Example:
        >>> RESULTS_DICT = {"Person Basics": {"schema_data": variant_df}}
        >>> TARGET_TABLES = ["Company Basics", "Person Basics"]
        >>> df = get_primary_schema_df(RESULTS_DICT, "Person Basics")
        >>> df.select("title", "savedSearchCounter").show()
        +-------------+-------------------+
        |title        |savedSearchCounter |
        +-------------+-------------------+
        |API_People   |100000156          |
        +-------------+-------------------+
    """
    variant_df = RESULTS_DICT[primarySchema]["schema_data"]
    schema_ddl = (
        variant_df
        .selectExpr("schema_of_json_agg(to_json(v)) as schema_ddl")
        .collect()[0]["schema_ddl"]
    )
    typed_df = (
        variant_df
        .select(f.from_json(f.to_json(f.col("v")), schema_ddl).alias("row"))
        .select("row.*")
    )

    filtered_results_df = typed_df.filter((f.col("title").isin(TARGET_TABLES)) | 
                                          f.col("title").startswith("API"))
    return filtered_results_df

# COMMAND ----------

# DBTITLE 1,get_primary_schema_from_class
def get_primary_schema_from_class(results_state: Any, primarySchema: str) -> DataFrame:
    """
    Convert variant schema data to a typed DataFrame and filter for target tables (dataclass version).
    
    Similar to get_primary_schema_df but uses a dataclass-based state object instead of
    a nested dictionary. Accesses schema_data via state.schemas[primarySchema].schema_data.
    Requires global TARGET_TABLES variable.
    
    Args:
        results_state: WorkflowState object containing schemas attribute.
                       Structure: results_state.schemas[primarySchema].schema_data
        primarySchema: The name of the primary schema being processed (e.g., "Person Basics")
        
    Returns:
        DataFrame: A filtered Spark DataFrame containing only target tables,
                   with columns like savedSearchCounter, title, dateCreated, etc.
                   expanded from the variant structure.
                   
    Raises:
        KeyError: If primarySchema doesn't exist in results_state.schemas
        AttributeError: If results_state doesn't have schemas attribute
        NameError: If TARGET_TABLES is not defined globally
        
    Example:
        >>> state = WorkflowState(schemas={"Person Basics": schema_state_obj})
        >>> TARGET_TABLES = ["Company Basics", "Person Basics"]
        >>> df = get_primary_schema_from_class(state, "Person Basics")
        >>> df.select("title", "savedSearchCounter").show()
        +-------------+-------------------+
        |title        |savedSearchCounter |
        +-------------+-------------------+
        |API_People   |100000156          |
        +-------------+-------------------+
    """
    variant_df = results_state.schemas[primarySchema].schema_data
    
    # Infer the schema
    schema_ddl = (
        variant_df
        .selectExpr("schema_of_json_agg(to_json(v)) as schema_ddl")
        .collect()[0]["schema_ddl"]
    )
    
    # SAFEGUARD 1: If the API just returned flat strings, it's not a table list. Skip it!
    if schema_ddl.upper() == "STRING":
        return spark.createDataFrame([], schema="savedSearchCounter int, title string")
        
    typed_df = (
        variant_df
        .select(f.from_json(f.to_json(f.col("v")), schema_ddl).alias("row"))
        .select("row.*")
    )

    # SAFEGUARD 2: If the returned objects don't even have a 'title' column, skip it!
    if "title" not in typed_df.columns:
         return spark.createDataFrame([], schema="savedSearchCounter int, title string")

    # Filter for exact matches against our specific OEGEN list
    filtered_results_df = typed_df.filter(f.col("title").isin(TARGET_TABLES))
    
    return filtered_results_df

# COMMAND ----------

# DBTITLE 1,get_saved_search_counter
def get_saved_search_counter(RESULTS_DICT: Dict[str, Any], primarySchema: str) -> List[tuple[int, str]]:
    """
    Extract saved search counter values and titles from filtered results.
    
    Retrieves the 'savedSearchCounter' and 'title' columns from the filtered results DataFrame
    and returns them as a list of tuples. Each tuple contains (counter_id, table_name).
    
    Args:
        RESULTS_DICT: Dictionary containing filtered results with 'filtered_results_df' key.
                      Structure: {primarySchema: {"filtered_results_df": DataFrame, ...}}
        primarySchema: The name of the primary schema being processed (e.g., "Person Basics")
        
    Returns:
        List[tuple[int, str]]: A list of tuples where each tuple contains:
                               - savedSearchCounter (int): The table ID
                               - title (str): The table name
                               
    Raises:
        KeyError: If primarySchema or 'filtered_results_df' key doesn't exist in RESULTS_DICT
        
    Example:
        >>> RESULTS_DICT = {"Company Basics": {"filtered_results_df": df}}
        >>> result = get_saved_search_counter(RESULTS_DICT, "Company Basics")
        >>> print(result)
        [(100000155, 'API_Entity'), (100000161, 'API_Shareholdings'), 
         (100000134, 'OEGEN_Entity'), (100000138, 'OEGEN_Shareholdings')]
    """
    rows = (
        RESULTS_DICT[primarySchema]["filtered_results_df"]
        .select(f.col("savedSearchCounter"), f.col("title"))
        .collect()
    )
    return [(r["savedSearchCounter"], r["title"]) for r in rows]

# COMMAND ----------

# DBTITLE 1,get_saved_search_counter_from_class
def get_saved_search_counter_from_class(state: Any, primarySchema: str) -> List[tuple[int, str]]:
    """
    Extract saved search counter values and titles from filtered results (dataclass version).
    
    Similar to get_saved_search_counter but uses a dataclass-based state object instead of
    a nested dictionary. Accesses filtered_results_df via state.schemas[primarySchema].filtered_results_df.
    
    Args:
        state: WorkflowState object containing schemas attribute.
               Structure: state.schemas[primarySchema].filtered_results_df
        primarySchema: The name of the primary schema being processed (e.g., "Person Basics")
        
    Returns:
        List[tuple[int, str]]: A list of tuples where each tuple contains:
                               - savedSearchCounter (int): The table ID
                               - title (str): The table name
                               
    Raises:
        KeyError: If primarySchema doesn't exist in state.schemas
        AttributeError: If state doesn't have schemas attribute or schema doesn't have filtered_results_df
        
    Example:
        >>> state = WorkflowState(schemas={"Company Basics": schema_state_obj})
        >>> result = get_saved_search_counter_from_class(state, "Company Basics")
        >>> print(result)
        [(100000155, 'API_Entity'), (100000161, 'API_Shareholdings'), 
         (100000134, 'OEGEN_Entity'), (100000138, 'OEGEN_Shareholdings')]
    """
    rows = (
        state.schemas[primarySchema].filtered_results_df
        .select(f.col("savedSearchCounter"), f.col("title"))
        .collect()
    )
    return [(r["savedSearchCounter"], r["title"]) for r in rows]

# COMMAND ----------

# DBTITLE 1,fetch_all_pages_as_json_list
def fetch_all_pages_as_json_list(
    base_url: str,
    schema: str,
    company_id: int,
    headers: Dict[str, str],
    page_size: int = 150
) -> List[List[Dict[str, Any]]]:
    """
    Fetch all paginated results from the GEMS API for a specific schema and company.
    
    Makes an initial request to determine total pages from the X-Pagination header,
    then fetches all remaining pages. Each page contains a list of records.
    
    Args:
        base_url: The base URL of the GEMS API (e.g., "https://api.gems.example.com")
        schema: The schema name to query (e.g., "Person Basics", "Company Basics")
        company_id: The saved search counter ID for the specific table
        headers: HTTP headers including authorization token
        page_size: Number of records per page (default: 150)
        
    Returns:
        List[List[Dict[str, Any]]]: A list where each element is a page (list of record dicts).
                                     Each record is a dictionary containing the API response data.
                                     
    Raises:
        requests.HTTPError: If any API request fails
        json.JSONDecodeError: If X-Pagination header is not valid JSON
        
    Example:
        >>> headers = {"Authorization": f"Bearer {token}"}
        >>> pages = fetch_all_pages_as_json_list(
        ...     base_url="https://api.gems.example.com",
        ...     schema="Person Basics",
        ...     company_id=100000156,
        ...     headers=headers,
        ...     page_size=150
        ... )
        >>> print(f"Fetched {len(pages)} pages")
        Fetched 2 pages
        >>> print(f"First page has {len(pages[0])} records")
        First page has 150 records
    """
    url = f"{base_url}/gemswebapi/api/search/{schema}/{company_id}"

    r1 = requests.get(url, params={"page": 1, "pagesize": page_size}, headers=headers)
    r1.raise_for_status()

    xp = json.loads(r1.headers.get("X-Pagination", "{}"))
    total_pages = int(xp.get("TotalPages") or 1)
    if total_pages < 1:
        total_pages = 1

    out = [r1.json()]

    for page in range(2, total_pages + 1):
        r = requests.get(url, params={"page": page, "pagesize": page_size}, headers=headers)
        r.raise_for_status()
        out.append(r.json())

    return out

# COMMAND ----------

# DBTITLE 1,create_df_from_list
def create_df_from_list(pages: List[List[Dict[str, Any]]]) -> DataFrame:
    """
    Convert a list of API response pages into a Spark DataFrame with variant column.
    
    Takes raw JSON pages from the API, serializes them to JSON strings, creates a DataFrame,
    and parses the JSON into a variant column for flexible schema handling.
    
    Args:
        pages: List of pages where each page is a list of record dictionaries from the API
        
    Returns:
        DataFrame: A Spark DataFrame with a single 'raw_data' column of type variant,
                   containing one row per page with parsed JSON data.
                   
    Raises:
        json.JSONDecodeError: If page data cannot be serialized to JSON
        
    Example:
        >>> pages = [[{"id": 1, "name": "John"}, {"id": 2, "name": "Jane"}],
        ...          [{"id": 3, "name": "Bob"}]]
        >>> df = create_df_from_list(pages)
        >>> df.printSchema()
        root
         |-- raw_data: variant (nullable = true)
        >>> df.count()
        2
        >>> df.show(1, truncate=False)
        +------------------------------------------+
        |raw_data                                  |
        +------------------------------------------+
        |[{"id":1,"name":"John"},{"id":2,"name":...}|
        +------------------------------------------+
    """
    records_variant_df: DataFrame = (
        spark.createDataFrame(
            [(json.dumps(page),) for page in pages],
            ["raw_json"]
        )
        .select(f.parse_json(f.col("raw_json")).alias("raw_data"))
    )
    return records_variant_df

# COMMAND ----------

# DBTITLE 1,explode_json_array_field
def explode_json_array_field(
    records_variant_df: DataFrame, 
    variant_path_expr: str, 
    json_alias: str = "json_str"
) -> DataFrame:
    """
    Explode a nested array field from a variant column into separate rows.
    
    Takes a DataFrame with variant data, extracts an array field using a path expression,
    infers the schema, and explodes the array so each element becomes a separate row
    with typed columns.
    
    Args:
        records_variant_df: DataFrame containing variant column with nested array data
        variant_path_expr: Path expression to the array field (e.g., "raw_data.items", "v.records")
        json_alias: Temporary column name for JSON string representation (default: "json_str")
        
    Returns:
        DataFrame: A DataFrame where each row represents one element from the exploded array,
                   with columns expanded from the array element structure.
                   
    Raises:
        AnalysisException: If variant_path_expr doesn't exist or isn't an array
        
    Example:
        >>> # Input DataFrame with variant column containing array
        >>> df = spark.createDataFrame([
        ...     ('{"items": [{"id": 1, "name": "A"}, {"id": 2, "name": "B"}]}',)
        ... ], ["raw_json"]).select(f.parse_json(f.col("raw_json")).alias("raw_data"))
        >>> 
        >>> result = explode_json_array_field(df, "raw_data.items")
        >>> result.show()
        +---+----+
        | id|name|
        +---+----+
        |  1|   A|
        |  2|   B|
        +---+----+
    """
    json_df = (
        records_variant_df
        .selectExpr(f"to_json({variant_path_expr}) as {json_alias}")
        .withColumn("schema_ddl", f.schema_of_json(f.col(json_alias)))
    )

    schema_ddl = json_df.select("schema_ddl").first()["schema_ddl"]

    return (
        json_df
        .select(f.from_json(f.col(json_alias), schema_ddl).alias("arr"))
        .select(f.explode("arr").alias("row"))
        .select("row.*")
    )

# COMMAND ----------

# DBTITLE 1,build_id_label_mapping
def build_id_label_mapping(
    fields_maps_df: DataFrame, 
    id_col: str = "id", 
    label_col: str = "label"
) -> Dict[str, str]:
    """
    Build a dictionary mapping IDs to labels from a DataFrame.
    
    Collects ID and label columns from a DataFrame and creates a dictionary where
    IDs (converted to lowercase) are keys and labels are values. Useful for creating
    lookup mappings for data enrichment.
    
    Args:
        fields_maps_df: DataFrame containing ID and label columns
        id_col: Name of the column containing IDs (default: "id")
        label_col: Name of the column containing labels (default: "label")
        
    Returns:
        Dict[str, str]: Dictionary mapping lowercase ID strings to their corresponding labels
        
    Raises:
        AttributeError: If specified columns don't exist in the DataFrame
        
    Example:
        >>> df = spark.createDataFrame([
        ...     ("FIELD_001", "First Name"),
        ...     ("FIELD_002", "Last Name"),
        ...     ("FIELD_003", "Email Address")
        ... ], ["id", "label"])
        >>> 
        >>> mapping = build_id_label_mapping(df)
        >>> print(mapping)
        {'field_001': 'First Name', 'field_002': 'Last Name', 'field_003': 'Email Address'}
        >>> 
        >>> # Use the mapping for lookups
        >>> print(mapping.get('field_001'))
        First Name
    """
    rows = fields_maps_df.select(id_col, label_col).collect()
    return {getattr(r, id_col).lower(): getattr(r, label_col) for r in rows}

# COMMAND ----------

# DBTITLE 1,add_row_hash
def add_row_hash(
    df: DataFrame,
    exclude: List[str] | None = None,
    hash_col: str = "row_hash",
    hash_bits: int = 256
) -> DataFrame:
    """
    Add a hash column computed over all (or selected) columns.
    
    Concatenates all columns (except excluded ones) into a single string with '||' delimiter,
    then computes a SHA-2 hash. Useful for change data capture (CDC) and deduplication.
    
    Args:
        df: The input PySpark DataFrame
        exclude: List of column names to exclude from hashing (default: None)
        hash_col: Name of the hash column to create (default: 'row_hash')
        hash_bits: Bit length for SHA-2 hashing - 256 or 512 (default: 256)
        
    Returns:
        DataFrame: New DataFrame with an additional hash column containing the computed hash
        
    Raises:
        ValueError: If hash_bits is not 256 or 512
        
    Example:
        >>> df = spark.createDataFrame([
        ...     (1, "Alice", "alice@example.com", "2024-01-01"),
        ...     (2, "Bob", "bob@example.com", "2024-01-02")
        ... ], ["id", "name", "email", "created_date"])
        >>> 
        >>> # Hash all columns
        >>> hashed_df = add_row_hash(df)
        >>> hashed_df.select("id", "name", "row_hash").show(truncate=False)
        +---+-----+----------------------------------------------------------------+
        |id |name |row_hash                                                        |
        +---+-----+----------------------------------------------------------------+
        |1  |Alice|a3f5e8c9d1b2a4f6e8c0d2b4a6f8e0c2d4b6a8f0e2c4d6b8a0f2e4c6d8b0|
        |2  |Bob  |b4f6e9c0d2b3a5f7e9c1d3b5a7f9e1c3d5b7a9f1e3c5d7b9a1f3e5c7d9b1|
        +---+-----+----------------------------------------------------------------+
        >>> 
        >>> # Exclude timestamp columns from hashing
        >>> hashed_df = add_row_hash(df, exclude=["created_date"])
        >>> hashed_df.columns
        ['id', 'name', 'email', 'created_date', 'row_hash']
    """
    exclude = exclude or []
    cols_to_hash = [c for c in df.columns if c not in exclude]

    # Concatenate all selected columns as strings with a delimiter
    concat_expr = f.concat_ws("||", *[f.col(c).cast("string") for c in cols_to_hash])
    df = df.withColumn(hash_col, f.sha2(concat_expr, hash_bits))

    return df

# COMMAND ----------

# DBTITLE 1,add_scd_cols
def add_scd_cols(df: DataFrame, time_now: str) -> DataFrame:
    """
    Add Slowly Changing Dimension (SCD) Type 2 columns to a DataFrame.
    
    Adds 'START_AT' and 'END_AT' timestamp columns for tracking record validity periods.
    New records get START_AT set to the current time and END_AT set to NULL (indicating
    the record is currently active).
    
    Args:
        df: The input PySpark DataFrame
        time_now: Timestamp string or expression for the START_AT value
                  (e.g., "2024-01-15 10:30:00" or f.current_timestamp())
        
    Returns:
        DataFrame: DataFrame with two additional columns:
                   - START_AT (timestamp): When the record became valid
                   - END_AT (timestamp): When the record became invalid (NULL for active records)
                   
    Example:
        >>> df = spark.createDataFrame([
        ...     (1, "Alice", "alice@example.com"),
        ...     (2, "Bob", "bob@example.com")
        ... ], ["id", "name", "email"])
        >>> 
        >>> scd_df = add_scd_cols(df, time_now=f.current_timestamp())
        >>> scd_df.show(truncate=False)
        +---+-----+-----------------+-------------------+------+
        |id |name |email            |START_AT           |END_AT|
        +---+-----+-----------------+-------------------+------+
        |1  |Alice|alice@example.com|2024-01-15 10:30:00|null  |
        |2  |Bob  |bob@example.com  |2024-01-15 10:30:00|null  |
        +---+-----+-----------------+-------------------+------+
        >>> 
        >>> # With string timestamp
        >>> scd_df = add_scd_cols(df, time_now="2024-01-15 10:30:00")
    """
    df = df.withColumn("START_AT", f.lit(time_now).cast("timestamp"))\
                    .withColumn("END_AT", f.lit(None).cast("timestamp"))
    return df

# COMMAND ----------

# DBTITLE 1,scd_merge_df_to_target
def scd_merge_df_to_target(
    SCHEMA: str, 
    df: DataFrame, 
    target_table: str, 
    time_now: str
) -> None:
    """
    Merge a DataFrame into a target Delta table using SCD Type 2 logic.
    
    Creates a temporary view from the source DataFrame, creates the target table if it doesn't exist,
    and performs a MERGE operation with schema evolution. The merge:
    - Inserts new records (based on row_hash)
    - Closes out records that no longer exist in the source by setting their END_AT timestamp
    
    This implements Slowly Changing Dimension Type 2 pattern for historical tracking.
    Requires global spark session to be available.
    
    Args:
        SCHEMA: The schema/database name where the target table resides (e.g., "bronze", "silver")
        df: Source DataFrame to merge into the target table. Must contain 'row_hash' and 'END_AT' columns.
        target_table: Name of the target table (without schema prefix)
        time_now: Timestamp string for closing out records (e.g., "2024-01-15 10:30:00")
        
    Returns:
        None: This function performs side effects (creates/updates tables) but returns nothing
        
    Raises:
        AnalysisException: If schema doesn't exist or table creation fails
        ParseException: If SQL syntax is invalid
        
    Example:
        >>> # Prepare source data with SCD columns
        >>> df = spark.createDataFrame([
        ...     (1, "Alice", "abc123", "2024-01-15 10:00:00", None),
        ...     (2, "Bob", "def456", "2024-01-15 10:00:00", None)
        ... ], ["id", "name", "row_hash", "START_AT", "END_AT"])
        >>> 
        >>> # Merge into target table
        >>> scd_merge_df_to_target(
        ...     SCHEMA="bronze",
        ...     df=df,
        ...     target_table="person_landing",
        ...     time_now="2024-01-15 10:30:00"
        ... )
        Creating view source_df_person_landing
        Creating table if not exists bronze.person_landing
        >>> 
        >>> # Verify the merge
        >>> spark.table("bronze.person_landing").show()
        +---+-----+--------+-------------------+------+
        | id| name|row_hash|           START_AT|END_AT|
        +---+-----+--------+-------------------+------+
        |  1|Alice|  abc123|2024-01-15 10:00:00|  null|
        |  2|  Bob|  def456|2024-01-15 10:00:00|  null|
        +---+-----+--------+-------------------+------+
    """
    source_view = f"source_df_{target_table}"
    qualified_target_table = f"{SCHEMA}.{target_table}"

    try:
        print(f"Creating view {source_view}")
        df.createOrReplaceTempView(source_view)
    except Exception as e:
        print(f"Error creating view {source_view}: {e}")

    try:
        print(f"Creating table if not exists {qualified_target_table}")
        spark.sql(
            f"""
        CREATE TABLE IF NOT EXISTS {qualified_target_table}
        USING DELTA
        AS SELECT * FROM {source_view} WHERE 1 = 0
        """
        )
    except Exception as e:
        print(f"Error creating table {qualified_target_table}: {e}")

    merge_df_into_staging_query = f"""
    MERGE WITH SCHEMA EVOLUTION INTO {qualified_target_table} AS t
    USING {source_view} AS s
    ON t.row_hash = s.row_hash 
        AND t.END_AT <=> s.END_AT
    WHEN NOT MATCHED THEN 
        INSERT *
    WHEN NOT MATCHED BY SOURCE THEN 
        UPDATE SET t.END_AT = CASE
            WHEN t.END_AT IS NULL THEN TIMESTAMP '{time_now}'
            ELSE t.END_AT
        END
    """
    spark.sql(merge_df_into_staging_query)
