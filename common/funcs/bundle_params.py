from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Databricks Pipeline").getOrCreate()

try:
    from pyspark.dbutils import DBUtils

    dbutils = DBUtils(spark)
except ImportError:
    dbutils = None

_UNSET = "__xio_param_unset__"


def get_bundle_param(widget_name, spark_conf_key, default):
    """
    Reads a bundle-driven value that may arrive either via a notebook widget
    (set from a job's base_parameters) or via a DLT pipeline's spark
    configuration (set from a pipeline's configuration block), depending on
    which execution context this is called from. `default` may be a
    callable, evaluated lazily only if neither source was ever explicitly
    set (an empty string, e.g. staging/prod's development_prefix, still
    counts as set and is returned as-is, distinct from "not set at all").
    """
    if dbutils is not None:
        try:
            dbutils.widgets.text(widget_name, _UNSET)
            value = dbutils.widgets.get(widget_name)
            if value != _UNSET:
                return value
        except Exception:
            pass
    try:
        value = spark.conf.get(spark_conf_key, _UNSET)
        if value != _UNSET:
            return value
    except Exception:
        pass
    return default() if callable(default) else default
