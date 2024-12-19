import os
import dagster as dg
from pins import board_connect, board_folder, board_s3
from pydantic import PrivateAttr
from typing import Any

class PinsResource(dg.ConfigurableResource):
    
    def read(self, name):
        raise NotImplementedError()
    
    def write(self, df, name):
        raise NotImplementedError()
    
    def list_pins(self):
        raise NotImplementedError()

class LocalPinsResource(PinsResource):
    pins_directory: str = "/tmp/pins"
    _board: Any = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._board = board_folder(path=self.pins_directory)
                                  
    def read(self, name):
        return self._board.pin_read(name)
    
    def write(self, df, name):
        return self._board.pin_write(df, name, type="csv")
    
    def list_pins(self):
        return self._board.pin_list()


class S3PinsResource(PinsResource):
    region_name: str
    s3_bucket: str
    aws_access_key_id: str
    aws_secret_access_key: str
    pins_directory: str
    _board: Any = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._board = board_s3(
        f"{self.s3_bucket}/{self.pins_directory}",
        )

    def read(self, name):
        return self._board.pin_read(name)
    
    def write(self, df, name):
        return self._board.pin_write(df, name, type="csv")
    
    def list_pins(self):
        return self._board.pin_list()


class ConnectPinsResource(PinsResource):
    connect_api_key: str
    connect_server: str
    _board: Any = PrivateAttr()

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        self._board = board_connect(server_url=self.server, api_key=self.connect_api_key)

    def read(self, name):
        return self._board.pin_read(name)
    
    def write(self, df, name):
        return self._board.pin_write(df, name, type="csv")
    
    def list_pins(self):
        return self._board.pin_list()