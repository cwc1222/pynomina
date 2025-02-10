import csv
import hashlib
import io
import logging
import zipfile
from dataclasses import asdict
from datetime import datetime as dt
from typing import IO, List

import psycopg
import pydantic
from psycopg.rows import dict_row
from pydantic.dataclasses import dataclass
from tqdm import tqdm

from src.python.config import AppConfig, Config, NominasConf
from src.python.httpclient import HttpClient
from src.python.logger import Logger
from src.python.pipeline import AvailableData, NominaPipeline, RawCsvItem
from src.python.postgres import NominaPgPool


@dataclass
class DownloadHistory:
    download_id: str | None
    resource_url: str | None
    check_sum: str | None
    entries: int | None
    download_at_utc: dt | None
    was_succeed: bool | None


class CsvHandler:
    hash: str
    num_entries: int
    data: List[RawCsvItem]
    log: logging.Logger

    def __init__(self, csv_file: IO[bytes], encoding: str, log4py: Logger) -> None:
        self.log = log4py.getLogger("CsvHandler")
        md5sum = hashlib.md5()
        self.num_entries = 0
        self.data = []
        with io.TextIOWrapper(csv_file, encoding) as text_file:
            csv_reader = csv.DictReader(text_file)
            for row in csv_reader:
                self.num_entries += 1
                md5sum.update(",".join(row.values()).encode(encoding))
                try:
                    self.data.append(RawCsvItem(**row))
                except pydantic.ValidationError as e:
                    self.log.error(e)

        self.hash = md5sum.hexdigest()


class PyNomina:

    nominas_conf: NominasConf
    pgpool_mgr: NominaPgPool
    client: HttpClient
    log: logging.Logger

    def __init__(self, log4py: Logger, config: Config) -> None:
        self.log = log4py.getLogger("PyNomina")
        self.client = HttpClient(
            default_headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
            },
            default_timeout=5,
            max_tries=50,
            log4py=log4py,
        )
        self.pgpool_mgr = NominaPgPool(conf=config.pg, log4py=log4py)
        self.nominas_conf = config.nominas

    def get_histories(self):
        query = """
        SELECT
        	h.resource_url
        FROM
        	public.download_history h
        WHERE
        	h.stat = 'SUCCEED'::public.download_stat
        GROUP BY
        	resource_url
        """
        with self.pgpool_mgr.get_conn() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                rs = cur.execute(query).fetchall()
                return [str(r["resource_url"]) for r in rs]

    def insert_download_history(self, dh: DownloadHistory):
        query = """
        INSERT INTO public.download_history (
        	download_id,
        	resource_url,
        	check_sum,
        	entries,
	    	stat
        )
        SELECT
        	CONCAT(
        		'D',
        		LPAD(
        			COALESCE(
                        MAX(REGEXP_REPLACE(download_id, '\\D','','g')::NUMERIC) + 1,
                        1
                    )::TEXT,
        			7,
        			'0'
        		)
        	),
        	%(resource_url)s,
        	%(check_sum)s,
        	%(entries)s,
	    	'SUCCEED'::public.download_stat
        FROM
        	public.download_history
        """
        with self.pgpool_mgr.get_conn() as conn, conn.cursor() as cur:
            cur.execute(query, asdict(dh))

    def sync_data(self):
        try:
            resp = self.client.get(self.nominas_conf.resource).json()
            try:
                available_data = [
                    AvailableData(
                        **(
                            r
                            | {
                                "resource_url": f"{self.nominas_conf.resource}/nomina_{r["periodo"]}.zip"
                            }
                        )
                    )
                    for r in resp
                ]
            except pydantic.ValidationError as e:
                self.log.error(f"Error upon parsing: {resp}")
                raise e
            self.log.debug(f"available_data: {available_data}")
            downloaded = self.get_histories()
            self.log.debug(f"downloaded: {downloaded}")
            pending = [ad for ad in available_data if ad.resource_url not in downloaded]
            for item in (pbar := tqdm(pending)):
                anio_mes = item.periodo
                pbar.set_description(f"downloading nomina_{anio_mes}.zip")
                try:
                    resp = self.client.get(item.resource_url)
                except Exception as e:
                    self.log.error(e)
                    continue
                pbar.set_description(f"[{anio_mes}] reading zip file")
                with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                    pbar.set_description(f"[{anio_mes}] reading csv file")
                    with zf.open(f"nomina_{anio_mes}.csv", "r") as csv_file:
                        csvHandler = CsvHandler(
                            csv_file=csv_file, encoding="iso-8859-1", log4py=log4py
                        )
                        pbar.set_description(f"[{anio_mes}] saving csv to pg")
                        pipeline = NominaPipeline(csvHandler.data, anio_mes, log4py)
                        with (
                            self.pgpool_mgr.get_conn() as conn,
                            psycopg.ClientCursor(conn) as cur,
                        ):
                            pipeline.persist_to_pg(cur)
                        download_history = DownloadHistory(
                            download_id=None,
                            resource_url=item.resource_url,
                            check_sum=csvHandler.hash,
                            entries=csvHandler.num_entries,
                            download_at_utc=None,
                            was_succeed=True,
                        )
                        self.insert_download_history(download_history)
                # break
        except Exception as e:
            self.log.error(e)

    def teardown(self):
        self.pgpool_mgr.teardown()


if __name__ == '__main__':
    log4py = Logger()
    log = log4py.getLogger("Main")
    config = AppConfig(log4py=log4py, config_file_path="config.toml").read_config()
    pynomina = PyNomina(log4py=log4py, config=config)
    try:
        log.info("Welcome to pynomina")
        pynomina.sync_data()
    finally:
        pynomina.teardown()
        log.info("Left pynomina")
