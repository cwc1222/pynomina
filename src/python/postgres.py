import logging
from multiprocessing import cpu_count

from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg_pool import ConnectionPool

from src.python.config import PGConf
from src.python.logger import Logger


class NominaPgPool:
    _pool: ConnectionPool
    _started: bool
    _conf: PGConf
    log: logging.Logger

    def __init__(self, conf: PGConf, log4py: Logger) -> None:
        self._conf = conf
        self.log = log4py.getLogger("NominaPgPool")
        self._started = False

    def _get_conn_str(self):
        return (
            f"host={self._conf.host} port={self._conf.port} dbname={self._conf.dbname} "
            f"user={self._conf.user} password={self._conf.password} connect_timeout={self._conf.connect_timeout} "
            f"application_name={self._conf.application_name}"
        )

    def get_conn(self):
        if not self._started:
            self._start()
            self._started = True
        return self._pool.connection()

    def _start(self):
        self.log.info("Starting postgres connection pool...")
        if self._started:
            self.log.info("Pool had started already, do nothing")
            return
        conninfo = self._get_conn_str()
        self._pool = ConnectionPool(
            conninfo=conninfo,
            min_size=10 // cpu_count(),
            max_size=30 // cpu_count(),
            max_waiting=10,
            open=True,
            kwargs={"row_factory": dict_row},
        )
        self.log.info("Connection pool started")
        with self._pool.connection() as conn:
            self.log.info(f"SET search_path TO {self._conf.schema}")
            conn.execute(
                SQL("SET search_path TO {}, public;").format(
                    Identifier(self._conf.schema)
                )
            )
            conn.commit()
        self._started = True

    def teardown(self):
        try:
            if not self._started:
                self.log.info("Pool had been stopped already, do nothing")
                return
            self._pool.close()
            self.log.info("Connection pool shutted down")
        except Exception as e:
            self.log.error(e)
