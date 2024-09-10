from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslator

from ..project import dbt_project
from ..partitions import daily_partition

import json

INCREMENTAL_SELECTOR = "config.materialized:incremental"

class CustomizedDagsterDbtTranslator(DagsterDbtTranslator):
    # dbt_resource_props refers to a JSON object for the dbt model's properties (based on the manifest file)
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]

        # Only rename the asset if the dbt resource type is source
        # Otherwise, use get_asset_key from DagsterDbtTranslator
        if resource_type == "source":
            return AssetKey(f"taxi_{name}")
        else:
            return super().get_asset_key(dbt_resource_props)
        
    def get_group_name(self, dbt_resource_props) -> str:
        return dbt_resource_props["fqn"][1]

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    exclude=INCREMENTAL_SELECTOR
)
def dbt_analytics(context: AssetExecutionContext, dbt: DbtCliResource):
    # context indicates which dbt models to run and any other configurations
    # .stream() fetches the events and results of this dbt execution
    # Results of the stream are a Python generator of what Dagster events happened
    # yield from is used to have Dagster track asset materializations
    yield from dbt.cli(["build"], context=context).stream()

@dbt_assets(
    manifest=dbt_project.manifest_path,
    dagster_dbt_translator=CustomizedDagsterDbtTranslator(),
    select=INCREMENTAL_SELECTOR,
    partitions_def=daily_partition
)
def incremental_dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    time_window = context.partition_time_window
    dbt_vars = {
        "min_date": time_window.start.strftime('%Y-%m-%d'),
        "max_date": time_window.end.strftime('%Y-%m-%d')
    }
    yield from dbt.cli(["build", "--vars", json.dumps(dbt_vars)], context=context).stream()
