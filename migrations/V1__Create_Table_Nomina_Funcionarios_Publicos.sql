CREATE SCHEMA IF NOT EXISTS pynomina;

DO $$ BEGIN
    CREATE TYPE public.download_stat AS ENUM ('SUCCEED', 'FAILED');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

CREATE TABLE IF NOT EXISTS public.download_history (
    download_id TEXT,
    resource_url TEXT,
    check_sum TEXT,
    entries INT4,
    download_at_utc TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'utc'),
    stat download_stat DEFAULT NULL,
    PRIMARY KEY (download_id)
);

DO $$ BEGIN
    CREATE TYPE public.gender AS ENUM ('Femenino', 'Masculino', 'Otros');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;
CREATE TABLE IF NOT EXISTS public.py_personas (
    codigo_persona TEXT NULL,
    nombres TEXT NULL,
    apellidos TEXT NULL,
    fecha_nacimiento DATE NULL,
    sexo gender DEFAULT NULL,
    PRIMARY KEY (codigo_persona)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_errors (
    error_id SERIAL PRIMARY KEY,
    download_id TEXT,
    raw_data JSONB,
    CONSTRAINT fk_download_stat FOREIGN KEY (download_id) REFERENCES public.download_history(download_id)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_niveles (
    nivel_key TEXT, -- {codigo_nivel}-{nivel_abr}
    codigo_nivel TEXT,
    nivel_abr TEXT,
    desc_nivel TEXT NULL,
    PRIMARY KEY (nivel_key)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_entidades (
    entidad_key TEXT, -- {codigo_entidad}-{entidad_abr}
    codigo_entidad TEXT,
    entidad_abr TEXT,
    desc_entidad TEXT NULL,
    PRIMARY KEY (entidad_key)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_programas (
    programa_key TEXT, -- {codigo_programa}-{codigo_sub_programa}-{programa_abr}-{sub_programa_abr}
    codigo_programa TEXT NULL,
    codigo_sub_programa TEXT NULL,
    programa_abr TEXT NULL,
    sub_programa_abr TEXT NULL,
    desc_programa TEXT NULL,
    desc_sub_programa TEXT NULL,
    PRIMARY KEY (programa_key)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_proyectos (
    proyecto_key TEXT, -- {codigo_proyecto}-{proyecto_abr}
    codigo_proyecto TEXT NULL,
    proyecto_abr TEXT NULL,
    desc_proyecto TEXT NULL,
    PRIMARY KEY (proyecto_key)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_responsables (
    unidad_responsable_key TEXT, -- {codigo_unidad_responsable}-{unidad_responsable_abr}
    codigo_unidad_responsable TEXT NULL,
    unidad_responsable_abr TEXT NULL,
    desc_unidad_responsable TEXT NULL,
    PRIMARY KEY (unidad_responsable_key)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers_objecto_gasto (
    codigo_objecto_gasto TEXT,
    concepto_gasto TEXT NULL,
    PRIMARY KEY (codigo_objecto_gasto)
);

CREATE TABLE IF NOT EXISTS pynomina.hacienda_pub_officers (
    codigo_evento TEXT, -- {anio}{mes}-{codigo_persona}
    orden INT4,
    anio INT2,
    mes INT2,
    codigo_persona TEXT,
    discapacidad Boolean NULL,
    nivel_key TEXT NULL,
    entidad_key TEXT NULL,
    programa_key TEXT NULL,
    proyecto_key TEXT NULL,
    unidad_responsable_key TEXT NULL,
    codigo_objecto_gasto TEXT NULL,
    fuente_financiamiento TEXT NULL,
    linea TEXT NULL,
    codigo_categoria TEXT NULL,
    cargo TEXT NULL,
    horas_catedra INT4 NULL,
    fecha_ingreso DATE NULL,
    tipo_personal TEXT NULL,
    lugar TEXT NULL,
    monto_presupuestado INT8 NULL,
    monto_devengado INT8 NULL,
    anio_corte INT2 NULL,
    mes_corte INT2 NULL,
    fecha_corte DATE NULL,
    PRIMARY KEY (codigo_evento, orden),
    CONSTRAINT fk_persona FOREIGN KEY (codigo_persona) REFERENCES public.py_personas(codigo_persona),
    CONSTRAINT fk_nivel FOREIGN KEY (nivel_key) REFERENCES pynomina.hacienda_pub_officers_niveles(nivel_key),
    CONSTRAINT fk_entidad FOREIGN KEY (entidad_key) REFERENCES pynomina.hacienda_pub_officers_entidades(entidad_key),
    CONSTRAINT fk_programa FOREIGN KEY (programa_key) REFERENCES pynomina.hacienda_pub_officers_programas(programa_key),
    CONSTRAINT fk_proyecto FOREIGN KEY (proyecto_key) REFERENCES pynomina.hacienda_pub_officers_proyectos(proyecto_key),
    CONSTRAINT fk_unidad_responsable FOREIGN KEY (unidad_responsable_key) REFERENCES pynomina.hacienda_pub_officers_responsables(unidad_responsable_key),
    CONSTRAINT fk_objecto_gasto FOREIGN KEY (codigo_objecto_gasto) REFERENCES pynomina.hacienda_pub_officers_objecto_gasto(codigo_objecto_gasto)
);
