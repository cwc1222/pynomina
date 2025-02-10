import functools
import logging
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import datetime
from datetime import datetime as dt
from typing import List, Set

from psycopg import ClientCursor
from pydantic.dataclasses import dataclass
from tqdm import tqdm

from src.python.logger import Logger


@dataclass
class AvailableData:
    dataset: str
    periodo: str
    fechaCreacion: str
    resource_url: str


@dataclass
class RawCsvItem:
    anio: str
    mes: str
    codigoNivel: str
    nivelAbr: str
    descripcionNivel: str
    codigoEntidad: str
    entidadAbr: str
    descripcionEntidad: str
    codigoPrograma: str
    programaAbr: str
    descripcionPrograma: str
    codigoSubprograma: str
    subprogramaAbr: str
    descripcionSubprograma: str
    codigoProyecto: str
    proyectoAbr: str
    descripcionProyecto: str
    codigoUnidadResponsable: str
    unidadAbr: str
    descripcionUnidadResponsable: str
    codigoObjetoGasto: str
    conceptoGasto: str
    fuenteFinanciamiento: str
    linea: str
    codigoPersona: str
    nombres: str
    apellidos: str
    sexo: str
    discapacidad: str
    codigoCategoria: str
    cargo: str
    horasCatedra: str
    fechaIngreso: str
    tipoPersonal: str
    lugar: str
    montoPresupuestado: str
    montoDevengado: str
    mesCorte: str
    anioCorte: str
    fechaCorte: str


@dataclass(frozen=True)
class Persona:
    codigo_persona: str
    nombres: str
    apellidos: str
    fecha_nacimiento: datetime | None
    sexo: str


@dataclass(frozen=True)
class Nivel:
    nivel_key: str
    codigo_nivel: str
    nivel_abr: str
    desc_nivel: str


@dataclass(frozen=True)
class Entidad:
    entidad_key: str
    codigo_entidad: str
    entidad_abr: str
    desc_entidad: str


@dataclass(frozen=True)
class Programa:
    programa_key: str
    codigo_programa: str
    codigo_sub_programa: str
    programa_abr: str
    sub_programa_abr: str
    desc_programa: str
    desc_sub_programa: str


@dataclass(frozen=True)
class Proyecto:
    proyecto_key: str
    codigo_proyecto: str
    proyecto_abr: str
    desc_proyecto: str


@dataclass(frozen=True)
class UnidadResponsable:
    unidad_responsable_key: str
    codigo_unidad_responsable: str
    unidad_responsable_abr: str
    desc_unidad_responsable: str


@dataclass(frozen=True)
class ObjectoGasto:
    codigo_objecto_gasto: str
    concepto_gasto: str


@dataclass(frozen=True)
class PubOfficer:
    codigo_evento: str
    anio: int
    mes: int
    codigo_persona: str
    discapacidad: bool | None
    nivel_key: str | None
    entidad_key: str | None
    programa_key: str | None
    proyecto_key: str | None
    unidad_responsable_key: str | None
    codigo_objecto_gasto: int | None
    fuente_financiamiento: str | None
    linea: str | None
    codigo_categoria: str | None
    cargo: str | None
    horas_catedra: int | None
    fecha_ingreso: datetime | None
    tipo_personal: str | None
    lugar: str | None
    monto_presupuestado: int | None
    monto_devengado: int | None
    anio_corte: int | None
    mes_corte: int | None
    fecha_corte: datetime | None


@dataclass
class ProcessedCsvItems:
    personas: Set[Persona]
    niveles: Set[Nivel]
    entidades: Set[Entidad]
    programas: Set[Programa]
    proyectos: Set[Proyecto]
    unidades: Set[UnidadResponsable]
    objecto_gastos: Set[ObjectoGasto]
    pub_officers: Set[PubOfficer]


def parse_raw_item(raw: RawCsvItem, log: logging.Logger):
    """Parses a single RawCsvItem into processed entities."""
    try:
        codigo_evento = f"{raw.anio}{str(raw.mes).zfill(2)}-{raw.codigoPersona}"

        sexo = "Otros"
        if raw.sexo == "F":
            sexo = "Femenino"
        if raw.sexo == "M":
            sexo = "Masculino"
        persona = Persona(
            codigo_persona=raw.codigoPersona.strip(),
            nombres=raw.nombres.strip(),
            apellidos=raw.apellidos.strip(),
            fecha_nacimiento=None,
            sexo=sexo,
        )
        nivel = Nivel(
            nivel_key=f"{raw.codigoNivel.strip()}-{raw.nivelAbr.strip()}",
            codigo_nivel=raw.codigoNivel.strip(),
            nivel_abr=raw.nivelAbr.strip(),
            desc_nivel=raw.descripcionNivel.strip(),
        )
        entidad = Entidad(
            entidad_key=f"{raw.codigoEntidad.strip()}-{raw.entidadAbr.strip()}",
            codigo_entidad=raw.codigoEntidad.strip(),
            entidad_abr=raw.entidadAbr.strip(),
            desc_entidad=raw.descripcionEntidad.strip(),
        )
        programa = Programa(
            programa_key=f"{raw.codigoPrograma.strip()}-{raw.codigoSubprograma.strip()}-{raw.programaAbr.strip()}-{raw.subprogramaAbr.strip()}",
            codigo_programa=raw.codigoPrograma.strip(),
            codigo_sub_programa=raw.codigoSubprograma.strip(),
            programa_abr=raw.programaAbr.strip(),
            sub_programa_abr=raw.subprogramaAbr.strip(),
            desc_programa=raw.descripcionPrograma.strip(),
            desc_sub_programa=raw.descripcionSubprograma.strip(),
        )
        proyecto = Proyecto(
            proyecto_key=f"{raw.codigoProyecto.strip()}-{raw.proyectoAbr.strip()}",
            codigo_proyecto=raw.codigoProyecto.strip(),
            proyecto_abr=raw.proyectoAbr.strip(),
            desc_proyecto=raw.descripcionProyecto.strip(),
        )
        unidad = UnidadResponsable(
            unidad_responsable_key=f"{raw.codigoUnidadResponsable.strip()}-{raw.unidadAbr.strip()}",
            codigo_unidad_responsable=raw.codigoUnidadResponsable.strip(),
            unidad_responsable_abr=raw.unidadAbr.strip(),
            desc_unidad_responsable=raw.descripcionUnidadResponsable.strip(),
        )
        objecto_gasto = ObjectoGasto(
            codigo_objecto_gasto=raw.codigoObjetoGasto.strip(),
            concepto_gasto=raw.conceptoGasto.strip(),
        )

        fecha_ingreso = None
        fecha_corte = None
        try:
            fecha_ingreso = dt.strptime(raw.fechaIngreso, "%Y-%m-%d")
            fecha_corte = dt.strptime(raw.fechaCorte, "%Y-%m-%d")
        except ValueError:
            pass  # Ignore invalid dates

        pub_officer = PubOfficer(
            codigo_evento=codigo_evento,
            anio=int(raw.anio),
            mes=int(raw.mes),
            codigo_persona=raw.codigoPersona.strip(),
            discapacidad=True if raw.discapacidad == "Y" else False,
            nivel_key=f"{raw.codigoNivel.strip()}-{raw.nivelAbr.strip()}",
            entidad_key=f"{raw.codigoEntidad.strip()}-{raw.entidadAbr.strip()}",
            programa_key=f"{raw.codigoPrograma.strip()}-{raw.codigoSubprograma.strip()}-{raw.programaAbr.strip()}-{raw.subprogramaAbr.strip()}",
            proyecto_key=f"{raw.codigoProyecto.strip()}-{raw.proyectoAbr.strip()}",
            unidad_responsable_key=f"{raw.codigoUnidadResponsable.strip()}-{raw.unidadAbr.strip()}",
            codigo_objecto_gasto=int(raw.codigoObjetoGasto.strip()),
            fuente_financiamiento=raw.fuenteFinanciamiento.strip(),
            linea=raw.linea.strip(),
            codigo_categoria=raw.codigoCategoria.strip(),
            cargo=raw.cargo.strip(),
            horas_catedra=int(raw.horasCatedra.strip() or 0),
            fecha_ingreso=fecha_ingreso,
            tipo_personal=raw.tipoPersonal.strip(),
            lugar=raw.lugar.strip(),
            monto_presupuestado=int(raw.montoPresupuestado),
            monto_devengado=int(raw.montoDevengado),
            anio_corte=int(raw.anioCorte),
            mes_corte=int(raw.mesCorte),
            fecha_corte=fecha_corte,
        )

        return (
            persona,
            nivel,
            entidad,
            programa,
            proyecto,
            unidad,
            objecto_gasto,
            pub_officer,
        )

    except Exception as e:
        log.error(e)
        log.error(raw)
        return None  # Ignore errors


class NominaPipeline:

    _parsed_data: ProcessedCsvItems
    anio_mes: str
    log: logging.Logger

    def __init__(
        self,
        data: List[RawCsvItem],
        anio_mes: str,
        log4py: Logger,
    ) -> None:
        self.log = log4py.getLogger("NominaPipeline")
        self.anio_mes = anio_mes
        num_workers = multiprocessing.cpu_count()  # Use all CPU cores
        parse_with_log = functools.partial(parse_raw_item, log=self.log)

        # Use tqdm for progress tracking
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            results = list(
                tqdm(
                    executor.map(parse_with_log, data),  # No chunksize needed
                    total=len(data),
                    desc=f"[{anio_mes}] Processing records",
                    unit="item",
                )
            )

        # Remove None values (in case of errors)
        results = [r for r in results if r is not None]

        # Unpack parsed entities into separate sets
        (
            personas,
            niveles,
            entidades,
            programas,
            proyectos,
            unidades,
            objecto_gastos,
            pub_officers,
        ) = zip(*results)

        self._parsed_data = ProcessedCsvItems(
            personas=set(personas),
            niveles=set(niveles),
            entidades=set(entidades),
            programas=set(programas),
            proyectos=set(proyectos),
            unidades=set(unidades),
            objecto_gastos=set(objecto_gastos),
            pub_officers=set(pub_officers),
        )

        self.log.info(f"Finished processing {len(data)} records.")

    def persist_to_pg(self, cur: ClientCursor):
        insert_persona = """
        INSERT INTO public.py_personas (
            codigo_persona,
            nombres,
            apellidos,
            fecha_nacimiento,
            sexo
        )
        VALUES (
            %(codigo_persona)s, %(nombres)s, %(apellidos)s,
            %(fecha_nacimiento)s, %(sexo)s::public.gender
        )
        ON CONFLICT (codigo_persona)
        DO UPDATE SET
            nombres = EXCLUDED.nombres,
            apellidos = EXCLUDED.apellidos,
            fecha_nacimiento = EXCLUDED.fecha_nacimiento,
            sexo = EXCLUDED.sexo
        """
        cur.executemany(insert_persona, [asdict(p) for p in self._parsed_data.personas])

        insert_nivels = """
        INSERT INTO pynomina.hacienda_pub_officers_niveles (
            nivel_key,
            codigo_nivel,
            nivel_abr,
            desc_nivel
        )
        VALUES (
            %(nivel_key)s, %(codigo_nivel)s,
            %(nivel_abr)s, %(desc_nivel)s
        )
        ON CONFLICT (nivel_key)
        DO UPDATE SET
            codigo_nivel = EXCLUDED.codigo_nivel,
            nivel_abr = EXCLUDED.nivel_abr,
            desc_nivel = EXCLUDED.desc_nivel
        """
        cur.executemany(insert_nivels, [asdict(n) for n in self._parsed_data.niveles])

        insert_entidades = """
        INSERT INTO pynomina.hacienda_pub_officers_entidades (
            entidad_key,
            codigo_entidad,
            entidad_abr,
            desc_entidad
        )
        VALUES (
            %(entidad_key)s, %(codigo_entidad)s,
            %(entidad_abr)s, %(desc_entidad)s
        )
        ON CONFLICT (entidad_key)
        DO UPDATE SET
            codigo_entidad = EXCLUDED.codigo_entidad,
            entidad_abr = EXCLUDED.entidad_abr,
            desc_entidad = EXCLUDED.desc_entidad
        """
        cur.executemany(
            insert_entidades, [asdict(e) for e in self._parsed_data.entidades]
        )

        insert_programa = """
        INSERT INTO pynomina.hacienda_pub_officers_programas (
            programa_key,
            codigo_programa,
            codigo_sub_programa,
            programa_abr,
            sub_programa_abr,
            desc_programa,
            desc_sub_programa
        )
        VALUES (
            %(programa_key)s, %(codigo_programa)s,
            %(codigo_sub_programa)s, %(programa_abr)s,
            %(sub_programa_abr)s, %(desc_programa)s,
            %(desc_sub_programa)s
        )
        ON CONFLICT (programa_key)
        DO UPDATE SET
            codigo_programa = EXCLUDED.codigo_programa,
            codigo_sub_programa = EXCLUDED.codigo_sub_programa,
            programa_abr = EXCLUDED.programa_abr,
            sub_programa_abr = EXCLUDED.sub_programa_abr,
            desc_programa = EXCLUDED.desc_programa,
            desc_sub_programa = EXCLUDED.desc_sub_programa
        """
        cur.executemany(
            insert_programa, [asdict(p) for p in self._parsed_data.programas]
        )

        insert_proyecto = """
        INSERT INTO pynomina.hacienda_pub_officers_proyectos (
            proyecto_key,
            codigo_proyecto,
            proyecto_abr,
            desc_proyecto
        )
        VALUES (
            %(proyecto_key)s, %(codigo_proyecto)s,
            %(proyecto_abr)s, %(desc_proyecto)s
        )
        ON CONFLICT (proyecto_key)
        DO UPDATE SET
            codigo_proyecto = EXCLUDED.codigo_proyecto,
            proyecto_abr = EXCLUDED.proyecto_abr,
            desc_proyecto = EXCLUDED.desc_proyecto
        """
        cur.executemany(
            insert_proyecto, [asdict(p) for p in self._parsed_data.proyectos]
        )

        insert_unidad_resp = """
        INSERT INTO pynomina.hacienda_pub_officers_responsables (
            unidad_responsable_key,
            codigo_unidad_responsable,
            unidad_responsable_abr,
            desc_unidad_responsable
        )
        VALUES (
            %(unidad_responsable_key)s, %(codigo_unidad_responsable)s,
            %(unidad_responsable_abr)s, %(desc_unidad_responsable)s
        )
        ON CONFLICT (unidad_responsable_key)
        DO UPDATE SET
            codigo_unidad_responsable = EXCLUDED.codigo_unidad_responsable,
            unidad_responsable_abr = EXCLUDED.unidad_responsable_abr,
            desc_unidad_responsable = EXCLUDED.desc_unidad_responsable
        """
        cur.executemany(
            insert_unidad_resp, [asdict(u) for u in self._parsed_data.unidades]
        )

        insert_objecto_gasto = """
        INSERT INTO pynomina.hacienda_pub_officers_objecto_gasto (
            codigo_objecto_gasto,
            concepto_gasto
        )
        VALUES (
            %(codigo_objecto_gasto)s, %(concepto_gasto)s
        )
        ON CONFLICT (codigo_objecto_gasto)
        DO UPDATE SET
            concepto_gasto = EXCLUDED.concepto_gasto
        """
        cur.executemany(
            insert_objecto_gasto, [asdict(o) for o in self._parsed_data.objecto_gastos]
        )

        clean_current_month = """
        DELETE FROM pynomina.hacienda_pub_officers WHERE anio = %(anio)s AND mes = %(mes)s
        """
        insert_pub_officer = """
        INSERT INTO pynomina.hacienda_pub_officers (
            codigo_evento,
            orden,
            anio,
            mes,
            codigo_persona,
            discapacidad,
            nivel_key,
            entidad_key,
            programa_key,
            proyecto_key,
            unidad_responsable_key,
            codigo_objecto_gasto,
            fuente_financiamiento,
            linea,
            codigo_categoria,
            cargo,
            horas_catedra,
            fecha_ingreso,
            tipo_personal,
            lugar,
            monto_presupuestado,
            monto_devengado,
            anio_corte,
            mes_corte,
            fecha_corte
        )
        SELECT
            %(codigo_evento)s,
            COALESCE(MAX(orden) + 1, 1),
            %(anio)s, %(mes)s, %(codigo_persona)s, %(discapacidad)s,
            %(nivel_key)s, %(entidad_key)s, %(programa_key)s, %(proyecto_key)s,
            %(unidad_responsable_key)s, %(codigo_objecto_gasto)s, %(fuente_financiamiento)s, %(linea)s,
            %(codigo_categoria)s, %(cargo)s, %(horas_catedra)s, %(fecha_ingreso)s,
            %(tipo_personal)s, %(lugar)s, %(monto_presupuestado)s, %(monto_devengado)s,
            %(anio_corte)s, %(mes_corte)s, %(fecha_corte)s
        FROM
            pynomina.hacienda_pub_officers
        WHERE
            codigo_evento = %(codigo_evento)s
        """
        for p in self._parsed_data.pub_officers:
            # query = cur.mogrify(insert_pub_officer, asdict(p))
            # print(query)
            cur.execute(clean_current_month, {"anio": p.anio, "mes": p.mes})
            break
        cur.executemany(
            insert_pub_officer, [asdict(p) for p in self._parsed_data.pub_officers]
        )
