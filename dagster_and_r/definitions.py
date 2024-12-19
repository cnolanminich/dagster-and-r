import os
import dagster as dg
from . jobs import docker_container_op_r
from . asset_checks import (
    # no_missing_sepal_length_check_r,
    no_missing_sepal_length_check_py,
    )

from .pins_assets import (
    simple_pins_asset,
    read_pins_asset,
    read_pins_asset_r,
)

from . assets import (
    hello_world_r,
    iris_r,
    iris_py,
    )
from .resources import LocalPinsResource, S3PinsResource, ConnectPinsResource


resource_defs = {
        "DEV":{
            "pins_resource": LocalPinsResource(pins_directory=dg.EnvVar("local_pins_directory")),
            "pipes_subprocess_client": dg.PipesSubprocessClient(),},
        "TEST": {
            "pins_resource": S3PinsResource(
                region_name=dg.EnvVar("AWS_REGION"),
                s3_bucket=dg.EnvVar("AWS_S3_BUCKET"),
                aws_access_key_id=dg.EnvVar("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=dg.EnvVar("AWS_SECRET_ACCESS_KEY"),
                pins_directory="dagster-r-pins",
            ),
            "pipes_subprocess_client": dg.PipesSubprocessClient(),},
        "PROD": {
            "pins_resource":  ConnectPinsResource(
                connect_api_key=dg.EnvVar("connect_api_key"),
                connect_server=dg.EnvVar("connect_server")
            ),
            "pipes_subprocess_client": dg.PipesSubprocessClient(),},
    }

def get_env():
    # defined in .env file, which dagster dev automatically loads
    if os.getenv("DAGSTER_PROD_DEPLOY", "") == "1":
        return "PROD"
    elif os.getenv("DAGSTER_TEST_DEPLOY", "") == "1":
        return "TEST"
    elif os.getenv("DAGSTER_IS_DEV_CLI"):
        return "DEV"
    else:
        return "UNDEFINED"


defs = dg.Definitions(
    assets=[
        hello_world_r,
        iris_r,
        iris_py,
        simple_pins_asset,
        read_pins_asset,
        read_pins_asset_r,
        ],
    asset_checks=[
        # no_missing_sepal_length_check_r,
        no_missing_sepal_length_check_py,
        ],
    jobs=[docker_container_op_r],
    resources=resource_defs[get_env()],
)
