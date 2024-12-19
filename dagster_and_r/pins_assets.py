import dagster as dg 
import shutil
import pandas as pd
from .resources import PinsResource

@dg.asset
def simple_pins_asset(
    context: dg.AssetExecutionContext,
    pins_resource: PinsResource):
    df = pd.DataFrame({"a": [1, 2, 3, 4], "b": [4, 5, 6, 7]})
    pins_resource.write(df, "simple_pins_asset")
    return df


@dg.asset(deps = ["simple_pins_asset"])
def read_pins_asset(
    context: dg.AssetExecutionContext,
    pins_resource: PinsResource):
    df = pins_resource.read("simple_pins_asset") 
    context.log.info(f"Read pins asset: {df}")
    return df

# TODO add a way to pass the environment to use the correct board
@dg.asset(
        description="Reads a pin asset from within R",
        deps=["simple_pins_asset"])
def read_pins_asset_r(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    cmd = [shutil.which("Rscript"), dg.file_relative_path(__file__, "./R/read_pin.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_materialize_result()