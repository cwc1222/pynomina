from multiprocessing import cpu_count

from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier
from psycopg_pool import ConnectionPool

from python.config import PGConf
from python.logger import log4py

log = log4py.getLogger("NominaPgPool")


class NominaPgPool:
    _pool: ConnectionPool
    _started: bool
    _conf: PGConf

    def __init__(self, conf: PGConf) -> None:
        self._conf = conf
        self._started = False

    def get_conn_str(self):
        return (
            f"host={self._conf.host} port={self._conf.port} dbname={self._conf.dbname} "
            f"user={self._conf.user} password={self._conf.password} connect_timeout={self._conf.connect_timeout} "
            f"application_name={self._conf.application_name}"
        )

    def get_conn(self):
        if not self._started:
            self.start()
            self._started = True
        return self._pool.connection()

    def start(self):
        log.info("Starting postgres connection pool...")
        if self._started:
            log.info("Pool had started already, do nothing")
            return
        conninfo = self.get_conn_str()
        self._pool = ConnectionPool(
            conninfo=conninfo,
            min_size=10 // cpu_count(),
            max_size=30 // cpu_count(),
            max_waiting=10,
            open=True,
            kwargs={"row_factory": dict_row},
        )
        log.info("Connection pool started")
        with self._pool.connection() as conn:
            log.info(f"SET search_path TO {self._conf.schema}")
            conn.execute(
                SQL("SET search_path TO {}").format(Identifier(self._conf.schema))
            )
            conn.commit()

    def teardown(self):
        if not self._started:
            log.info("Pool had been stopped already, do nothing")
            return
        self._pool.close()
        log.info("Connection pool shutted down")
