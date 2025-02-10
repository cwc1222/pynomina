import io
import unittest
import zipfile
from pathlib import Path

import psycopg

from nomina import CsvHandler, DownloadHistory, PyNomina
from src.python.config import AppConfig
from src.python.logger import Logger
from src.python.pipeline import NominaPipeline


class TestNomina(unittest.TestCase):

    pynomina: PyNomina
    csv_file: Path
    resource_url: str
    log4py: Logger
    anio_mes: str

    def setUp(self):
        log4py = Logger()
        log = log4py.getLogger("TestNomina")
        config = AppConfig(log4py=log4py, config_file_path="config.toml").read_config()
        self.pynomina = PyNomina(log4py=log4py, config=config)
        self.log4py = log4py

        self.anio_mes = "2017-05"
        self.resource_url = f"{config.nominas.resource}/nomina_{self.anio_mes}.zip"

        csv_output_path = Path(f"nomina_{self.anio_mes}.csv")
        if not csv_output_path.exists():
            resp = self.pynomina.client.get(self.resource_url)
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                with zf.open(f"nomina_{self.anio_mes}.csv", "r") as csv_file:
                    with csv_output_path.open("wb") as output_file:
                        output_file.write(csv_file.read())
        self.csv_file = csv_output_path

        log.info("setUpClass finished")

    def tearDown(self):
        self.pynomina.teardown()

    def test_saving_from_csv(self):
        with self.csv_file.open(
            "rb",
        ) as csv_file:
            csvHandler = CsvHandler(
                csv_file=csv_file, encoding="iso-8859-1", log4py=self.log4py
            )
            pipeline = NominaPipeline(csvHandler.data, self.anio_mes, self.log4py)
            # The mogrify method is only available on the ClientCursor class.
            with (
                self.pynomina.pgpool_mgr.get_conn() as conn,
                psycopg.ClientCursor(conn) as cur,
            ):
                pipeline.persist_to_pg(cur)
            download_history = DownloadHistory(
                download_id=None,
                resource_url=self.resource_url,
                check_sum=csvHandler.hash,
                entries=csvHandler.num_entries,
                download_at_utc=None,
                was_succeed=True,
            )
            self.pynomina.insert_download_history(download_history)


if __name__ == '__main__':
    unittest.main()
