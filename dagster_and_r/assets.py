import shutil
import pandas as pd
import dagster as dg 

# example that runs an R script without modification. R script runs but does not report anything in Dagster other than succes.
@dg.asset
def hello_world_r(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    cmd = [shutil.which("Rscript"), dg.file_relative_path(__file__, "./R/hello_world.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
    ).get_materialize_result()



#
@dg.asset(
    config_schema={"output_dir": dg.Field(dg.String, default_value="./data")},
    check_specs=[
        dg.AssetCheckSpec(name="no_missing_sepal_length_check_r", asset="iris_r"),
        dg.AssetCheckSpec(name="no_missing_sepal_width_check_r", asset="iris_r"),
        dg.AssetCheckSpec(name="no_missing_petal_length_check_r", asset="iris_r"),
        dg.AssetCheckSpec(name="no_missing_petal_width_check_r", asset="iris_r"),
        dg.AssetCheckSpec(name="species_name_check_r", asset="iris_r"),
        ],
    )
def iris_r(
    context: dg.AssetExecutionContext,
    pipes_subprocess_client: dg.PipesSubprocessClient,
) -> dg.MaterializeResult:
    output_dir = context.op_config["output_dir"]
    cmd = [shutil.which("Rscript"), dg.file_relative_path(__file__, "./R/iris.R")]
    return pipes_subprocess_client.run(
        command=cmd,
        context=context,
        env={
            "MY_ENV_VAR_IN_SUBPROCESS": "This is an environment variable passed from Dagster to R!",
            "OUTPUT_DIR": output_dir,
        },
    ).get_materialize_result()


@dg.asset(deps=[iris_r])
def iris_py(context):
    # TODO replace hardcoded output_dir with resource key
    iris = pd.read_csv(f"data/iris.csv")
    context.log.info(type(iris))
    context.log.info(iris.head())
    return iris
