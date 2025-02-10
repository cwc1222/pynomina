import logging
import pathlib
import tomllib
from dataclasses import dataclass

from src.python.logger import Logger


@dataclass
class PGConf:
    host: str
    port: int
    dbname: str
    schema: str
    user: str
    password: str
    connect_timeout: int
    application_name: str


@dataclass
class NominasConf:
    resource: str
    # st_month_year: str
    force_download: bool


@dataclass
class Config:
    pg: PGConf
    nominas: NominasConf


class AppConfig:

    config_file_path = "config.toml"
    default_resource = "https://datos.hacienda.gov.py/odmh-core/rest/nomina/datos/"
    log: logging.Logger

    def __init__(self, log4py: Logger, config_file_path: str) -> None:
        self.log = log4py.getLogger("AppConfig")
        self.config_file_path = config_file_path

    def read_config(self) -> Config:
        config_file = pathlib.Path(self.config_file_path)
        read = tomllib.loads(config_file.read_text())
        read_app = read.get("app", {})
        read_pg = read.get("postgres", {})
        pgconf = PGConf(
            host=read_pg.get("HOST", "localhost"),
            port=read_pg.get("PORT", 5432),
            dbname=read_pg.get("DB", "opendata"),
            schema=read_pg.get("SCHEMA", "pynomina"),
            user=read_pg.get("USER", "postgres"),
            password=read_pg.get("PASS", "postgres"),
            connect_timeout=read_pg.get("CONN_TIMEOUT", 10),
            application_name=read_app.get("APP_NAME", "nominas-py"),
        )
        read_nomina = read.get("nominas", {})
        nomina_conf = NominasConf(
            resource=read_nomina.get("RESOURCE", self.default_resource),
            # st_month_year=read_nomina.get("ST_MONTH_YEAR", "2013-01"),
            force_download=read_nomina.get("FORCE_DOWNLOAD", False),
        )
        conf: Config = Config(pg=pgconf, nominas=nomina_conf)
        self.log.debug(f"read config: {conf}")
        return conf
