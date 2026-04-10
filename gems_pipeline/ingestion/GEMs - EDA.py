# Databricks notebook source
# DBTITLE 1,Import libraries / params
import requests
import pandas as pd
import pyspark.sql.functions as f
import pprint, json

# dev
# BASE_URL = "https://gemsuat.gemsemea.com"
# CLIENT_ID = "3b85aff8-b13c-41b0-b5a8-5fd800e05b57"
# CLIENT_SECRET = "g$m$U@T0ct0PU$"

# prod
BASE_URL =  "https://gems.gemsemea.com"
CLIENT_ID =  "52390f0d-2ec0-4966-ba0c-f665cce6aeff"
CLIENT_SECRET = "g$mPr0d0ct0PU$!!!"



SCHEMA_LIST = [
    'Person Basics',
                'Company Basics',
                ]

RESULTS_DICT = {}

TARGET_TABLES = {
    "OEGEN_Entity",
    "OEGEN_Appointment1",
    "OEGEN_Shareholdings",
    "OEGEN_Documents",
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Get token

# COMMAND ----------

def get_token() -> str:
    """
    Retrieve an OAuth2 access token from the GEMS API.
    
    Makes a POST request to the token endpoint using client credentials grant type.
    Uses the global BASE_URL, CLIENT_ID, and CLIENT_SECRET variables.
    
    Returns:
        str: The access token for authenticating subsequent API requests.
        
    Raises:
        requests.HTTPError: If the token request fails.
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

token = get_token()
headers = {"Authorization": f"Bearer {token}"}

# COMMAND ----------

# MAGIC %md
# MAGIC # List out all schemas

# COMMAND ----------

schemas = requests.get(
    f"{BASE_URL}/gemswebapi/api/search",
    headers=headers,
).json()

names = []
for item in schemas:
    if item["primarySchema"] in SCHEMA_LIST:
        names.append(item)

names_to_process = {item["primarySchema"] for item in names}
names_to_process

# COMMAND ----------

# MAGIC %md
# MAGIC # Get list of all tables for the chosen schemas

# COMMAND ----------

from pyspark.sql import DataFrame
from typing import Dict, Any

def get_table_list_from_schema(primarySchema: str) -> DataFrame:
    """
    Generate a list of tables that sit under the primary schema.
    
    Fetches schema details from the GEMS API and converts the response into
    a Spark DataFrame with a variant column containing parsed JSON.
    
    Args:
        primarySchema: The name of the primary schema to query.
        
    Returns:
        DataFrame: A Spark DataFrame with a single 'v' column containing
                   parsed JSON variant data for each table in the schema.
                   
    Raises:
        requests.HTTPError: If the API request fails.
    """
    details = requests.get(
        f"{BASE_URL}/gemswebapi/api/search/{primarySchema}",
        headers=headers,
    ).json()

    raw_df = spark.createDataFrame([(json.dumps(d),) for d in details], ["raw_json"])

    variant_df = raw_df.select(f.parse_json(f.col("raw_json")).alias("v"))
    return variant_df

for primarySchema in names_to_process:
    RESULTS_DICT[primarySchema] = {}
    print(f"Processing {primarySchema}")
    df = get_table_list_from_schema(primarySchema)
    df.display()
    RESULTS_DICT[primarySchema]["schema_data"] = df

# COMMAND ----------

# DBTITLE 1,explode variant of tables to full table
from pyspark.sql import DataFrame
from typing import Dict, Any

def get_primary_schema_df(RESULTS_DICT: Dict[str, Any], primarySchema: str) -> DataFrame:
    """
    Convert variant schema data to a typed DataFrame and filter for target tables.
    
    Takes the variant DataFrame from RESULTS_DICT, infers its schema, converts it
    to a typed DataFrame, and filters for rows where the title matches TARGET_TABLES
    or starts with 'API'.
    
    Args:
        RESULTS_DICT: Dictionary containing schema data with 'schema_data' key.
        primarySchema: The name of the primary schema being processed.
        
    Returns:
        DataFrame: A filtered Spark DataFrame containing only target tables,
                   with columns expanded from the variant structure.
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

for primarySchema in names_to_process:
    print(f"Processing {primarySchema}")
    df = get_primary_schema_df(RESULTS_DICT, primarySchema)
    df.display()
    RESULTS_DICT[primarySchema]["filtered_results_df"] = df

# COMMAND ----------

# DBTITLE 1,get_saved_search_counter
from typing import Dict, Any, List

def get_saved_search_counter(RESULTS_DICT: Dict[str, Any], primarySchema: str) -> List[int]:
    """
    Extract saved search counter values from filtered results.
    
    Retrieves the 'savedSearchCounter' column from the filtered results DataFrame
    and returns the values as a list of integers.
    
    Args:
        RESULTS_DICT: Dictionary containing filtered results with 'filtered_results_df' key.
        primarySchema: The name of the primary schema being processed.
        
    Returns:
        List[int]: A list of saved search counter values for the schema.
    """
    rows = (
        RESULTS_DICT[primarySchema]["filtered_results_df"]
        .select(f.col("savedSearchCounter"))
        .collect()
    )
    return [r["savedSearchCounter"] for r in rows]

for primarySchema in names_to_process:
    print(f"Processing {primarySchema}")
    saved_search = get_saved_search_counter(RESULTS_DICT, primarySchema)
    print(f"Saved Search Count: {saved_search}")
    RESULTS_DICT[primarySchema]["saved_search"] = saved_search

# COMMAND ----------

# MAGIC %md
# MAGIC # Get data from the actual tables

# COMMAND ----------

primarySchema = SCHEMA_LIST[0]
primarySchema

# COMMAND ----------

# DBTITLE 1,Get company data payload

primarySchema = SCHEMA_LIST[0]
company_id = RESULTS_DICT[primarySchema]["saved_search"][0]
records = requests.get(
    f"{BASE_URL}/gemswebapi/api/search/{primarySchema}/{company_id}",
    headers=headers,
)



# COMMAND ----------

# DBTITLE 1,check headers
response_headers = dict(records.headers)
# print(response_headers.keys())
print(response_headers['X-Pagination'])

# COMMAND ----------

# DBTITLE 1,check body
records = records.json()
records_variant_df = spark.createDataFrame([(json.dumps(records),)], ["raw_json"]).select(
    f.parse_json(f.col("raw_json")).alias("v")
)
display(records_variant_df)

# COMMAND ----------

# DBTITLE 1,check a different page
records = requests.get(
    f"{BASE_URL}/gemswebapi/api/search/{primarySchema}/{company_id}?page=4&pageSize=100",
    headers=headers,
)
print(records.headers["X-Pagination"])

# This has 1923 records - when I set page size as 100, I have 20 pages, which makes sense, and I can see a PrevPage and NextPage link

# COMMAND ----------

# DBTITLE 1,create a func that pulls in all pages
import json
import requests

def fetch_all_pages_as_json_list(
    base_url: str,
    schema: str,
    company_id: str,
    headers: dict,
) -> list:
    url = f"{base_url}/gemswebapi/api/search/{schema}/{company_id}"

    r1 = requests.get(url, params={"page": 1}, headers=headers)
    r1.raise_for_status()

    xp = json.loads(r1.headers.get("X-Pagination", "{}"))
    total_pages = int(xp.get("TotalPages") or 1)

    if total_pages < 1:
        total_pages = 1

    out = [r1.json()]

    for page in range(2, total_pages + 1):
        r = requests.get(url, params={"page": page}, headers=headers)
        r.raise_for_status()
        out.append(r.json())

    return out


primarySchema = SCHEMA_LIST[1]
company_id = RESULTS_DICT[primarySchema]["saved_search"][0]

pages = fetch_all_pages_as_json_list(
    base_url=BASE_URL,
    schema=primarySchema,
    company_id=company_id,
    headers=headers,
)

len(pages)

# COMMAND ----------

# DBTITLE 1,get the data out of the variant
from pyspark.sql import functions as f

results_df = (
    records_variant_df
    .selectExpr("to_json(v:result) as results")
    .withColumn("results_schema_ddl", f.schema_of_json(f.col("results")))
)

results_array_schema_ddl = results_df.select("results_schema_ddl").first()["results_schema_ddl"]

results_maps_df = (
    results_df
    .select(f.from_json(f.col("results"), results_array_schema_ddl).alias("arr"))
    .select(f.explode("arr").alias("row"))
    .select("row.*")
)

display(results_maps_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Apply correct Column Headers

# COMMAND ----------

# DBTITLE 1,get mappings
from pyspark.sql import functions as f

fields_df = (
    records_variant_df
    .selectExpr("to_json(v:fields) as fields_json")
    .withColumn("fields_schema_ddl", f.schema_of_json(f.col("fields_json")))
)

display(fields_df)


fields_array_schema_ddl = fields_df.select("fields_schema_ddl").first()["fields_schema_ddl"]

fields_maps_df = (
    fields_df
    .select(f.from_json(f.col("fields_json"), fields_array_schema_ddl).alias("arr"))
    .select(f.explode("arr").alias("row"))
    .select("row.*")
)

mapping = fields_maps_df.select("id", "label").collect()
mapping = {item.id.lower(): item.label for item in mapping}
mapping

# COMMAND ----------

# DBTITLE 1,apply mappings
renamed_df = results_maps_df.select(
    *[
        f.col(c).alias(mapping[c.lower()])
        if c.lower() in mapping
        else f.col(c)
        for c in results_maps_df.columns
    ]
)

display(renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Looking for documents
# MAGIC  {{baseUrl}}/gemswebapi/api/File/58

# COMMAND ----------

# DBTITLE 1,List documents
import requests
from typing import List, Dict

def list_all_file_ids(base_url: str, headers: Dict[str, str], saved_search_counter: int = 0, pagesize: int = 500) -> List[requests.Response]:
    """
    Retrieve all file IDs from the Document Basics schema using pagination.
    
    Makes repeated GET requests to the GEMS API, incrementing the page number
    until no more results are returned. Collects all Response objects.
    
    Args:
        base_url: The base URL for the GEMS API.
        headers: Dictionary of HTTP headers including authorization token.
        saved_search_counter: The saved search counter for Document Basics (default: 0).
        pagesize: Number of results per page (default: 500).
        
    Returns:
        List[requests.Response]: A list of Response objects, one per page retrieved.
        
    Raises:
        requests.HTTPError: If any API request fails.
    """
    raw = []
    page = 1

    while True:
        r = requests.get(
            f"{base_url}/gemswebapi/api/search/Document Basics/{saved_search_counter}",
            headers=headers,
            params={"page": page, "pagesize": pagesize},
        )
        r.raise_for_status()

        raw.append(r)

        payload = r.json()
        rows = payload.get("result") or []
        if not rows:
            break

        page += 1
    print(f"Found {len(raw)} pages")
    return raw

responses = list_all_file_ids(BASE_URL, headers, pagesize=1000)
responses[0].json()

# COMMAND ----------

# DBTITLE 1,get df
raw_df = spark.createDataFrame([(json.dumps(r.json()),) for r in responses], ["raw_json"])
variant_df = raw_df.select(f.parse_json(f.col("raw_json")).alias("v"))
variant_df.display()

# COMMAND ----------

# DBTITLE 1,explode df to get list of entities
from pyspark.sql import functions as f

results_df = (
    variant_df
    .selectExpr("to_json(v:result) as results")
    .withColumn("results_schema_ddl", f.schema_of_json(f.col("results")))
)

results_array_schema_ddl = results_df.select("results_schema_ddl").first()["results_schema_ddl"]

results_maps_df = (
    results_df
    .select(f.from_json(f.col("results"), results_array_schema_ddl).alias("arr"))
    .select(f.explode("arr").alias("row"))
    .select("row.*")
)

display(results_maps_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get single document

# COMMAND ----------

records = requests.get(
    f"{BASE_URL}/gemswebapi/api/file/30",
    headers=headers,
).json()
records.keys()


# COMMAND ----------

records["result"]

# COMMAND ----------

# DBTITLE 1,Decode doc
import base64
import os

row = records["result"][0]

b64 = row["contentsEncoded"]
filename = row["filename"] + row["extension"]  
data = base64.b64decode(b64)

out_path = f"./tmp/{filename}"

os.makedirs(os.path.dirname(out_path), exist_ok=True)

with open(out_path, "wb") as f:
    f.write(data)

out_path

# COMMAND ----------

# MAGIC %md
# MAGIC # Pushing data to GEMS

# COMMAND ----------

resp = requests.get(
    f"{BASE_URL}/gemswebapi/api/ScreenData/company/279/4139",
    headers=headers,
).json()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM oegen_data_prod_prod.gems.bronze_company_basics_oegen_entity
# MAGIC where companybasics_0_entity_entitycounter = 4139;

# COMMAND ----------

pprint.pprint(resp.keys())

# COMMAND ----------

pprint.pprint(resp["root"])