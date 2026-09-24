# Pruebas Data Engineer

Repositorio de prácticas y desarrollo de un pipeline de datos inmobiliarios utilizando **Databricks, Apache Spark SQL/PySpark, Delta Lake y una arquitectura Medallion**. El proyecto transforma publicaciones de propiedades desde una zona de aterrizaje hasta tablas analíticas y vistas semánticas para consultar precios, superficies, operaciones y métricas por zona.

## Objetivos

- Practicar consultas SQL avanzadas: `JOIN`, CTEs, funciones de ventana y agregaciones.
- Implementar un pipeline de datos por capas: **Bronze, Silver y Gold**.
- Aplicar limpieza, tipado, deduplicación, normalización geográfica y controles de calidad.
- Construir un modelo dimensional tipo **Star Schema**.
- Exponer vistas semánticas para análisis de propiedades y comparación entre zonas.

## Stack tecnológico

- **Databricks Notebooks** para ejecutar SQL y PySpark.
- **Apache Spark SQL / PySpark** para ingesta y transformación.
- **Delta Lake** para almacenar tablas transaccionales y confiables.
- **Unity Catalog** con el catálogo `bootcamp` y los esquemas `bronze`, `silver`, `gold` y `semantica`.
- **Jupyter Notebooks** (`.ipynb`) como formato de los entregables y procesos.

## Estructura del repositorio

```text
.
├── README.md
├── SRC/
│   ├── DDL/
│   │   ├── bronze/       Definición de tablas de entrada
│   │   ├── silver/       Definición de tablas limpias y tipadas
│   │   └── gold/         Dimensiones y tabla de hechos
│   ├── ETL/
│   │   ├── bronze/       Carga desde archivos de landing
│   │   ├── silver/       Limpieza, estandarización y deduplicación
│   │   └── gold/         Carga del modelo dimensional
│   ├── EDA/              Exploración y validación durante la transformación
│   ├── DQ/               Validaciones de calidad de datos
│   └── views/             Vistas analíticas del esquema semántico
├── Semana 1/             Joins y ejercicios iniciales de SQL
├── Semana 2/             EDA, CTEs y Window Functions
├── Semana 3/             Implementación de la arquitectura Medallion
├── Semana 4/             Ejercicios de transformación
├── Semana 5/             Ejercicios de procesamiento
├── Semana 6/             Ejercicios adicionales de módulos
├── Semana 8/             Consumo de APIs y escritura en Bronze Delta
└── Semana 9/             Validaciones de calidad e integridad
```

La carpeta principal para implementar el proyecto es `SRC/`. Las carpetas `Semana 1` a `Semana 9` contienen ejercicios y ejemplos que sirven como material de aprendizaje y antecedentes de las transformaciones implementadas en `SRC/`.

## Arquitectura Medallion

El pipeline utiliza el siguiente flujo:

```text
Landing / APIs
     │
     ▼
  Bronze ──► EDA y validaciones iniciales
     │
     ▼
  Silver ──► Tipado, limpieza, normalización y deduplicación
     │
     ▼
   Gold ──► Modelo dimensional y métricas de negocio
     │
     ▼
 Semantica ──► Vistas para consumo analítico
```

### Bronze

Bronze conserva los datos cercanos a la fuente original, con transformaciones mínimas.

- Tabla principal: `bootcamp.bronze.properties_bronze`.
- Origen actual: `/Volumes/bootcamp/landing/archivos/properties_raw.csv`.
- El notebook `SRC/ETL/bronze/ETL_properties_bronze.ipynb` lee el CSV con `read_files`.
- Se conserva el esquema como texto (`inferSchema => false`) para permitir que la capa Silver controle explícitamente los tipos.
- Se filtran registros que no tienen una URL válida (`url LIKE 'https%'`).

La carpeta `Semana 8` también muestra cómo cargar APIs externas a Bronze Delta, incluyendo DolarAPI y Open-Meteo, mediante `requests`, `spark.createDataFrame` y `saveAsTable`.

### Silver

Silver contiene datos limpios, tipados, estandarizados y listos para modelado.

El flujo de `SRC/ETL/silver/ETL_properties_silver.ipynb` incluye:

1. Conversión segura de strings numéricos a `double`, `decimal` e `int`.
2. Conversión de valores inválidos o `NaN` a `NULL`.
3. Normalización de monedas a `USD` y `ARS`.
4. Filtrado de outliers por percentiles de precio.
5. Validación de URLs y zonas geográficas.
6. Normalización de texto con `LOWER` y `TRIM`.
7. Conversión de `cochera` a booleano.
8. Reemplazo controlado de valores inválidos de antigüedad por `999`.
9. Deduplicación mediante `ROW_NUMBER() OVER (PARTITION BY precio, url)`.
10. Derivación de `partido`, `region` y `precio_por_m2`.

La tabla resultante es `bootcamp.silver.propiedades`, definida en `SRC/DDL/silver/DDL_properties_silver.ipynb` como una tabla Delta con columnas de negocio y metadatos como `_source_table` y `_processing_timestamp`.

### Gold

Gold modela los datos para el análisis de negocio mediante un esquema estrella.

#### Tabla de hechos

- `bootcamp.gold.fact_propiedades`
- Clave técnica: `row_hash`, calculada con `MD5(CONCAT_WS('|', url, precio))`.
- Medidas: `precio`, `expensas`, `precio_por_m2`, metros cuadrados y ambientes.
- Claves foráneas: `zona_id`, `tipo_operacion_id`, `fecha_id`, `caracteristicas_id` y `orientacion_id`.

#### Dimensiones

- `dim_zona`: partido y región.
- `dim_tipo_operacion`: tipo de operación y moneda.
- `dim_tiempo`: fecha de publicación.
- `dim_caracteristicas`: estado y disponibilidad de cochera.
- `dim_orientacion`: orientación del inmueble.

El notebook `SRC/ETL/gold/ETL_fact_propiedades.sql.ipynb` carga la tabla de hechos desde Silver y realiza `LEFT JOIN` contra las dimensiones. Antes de ejecutar el hecho, deben crearse y cargarse todas las dimensiones mediante los notebooks correspondientes de `SRC/DDL/gold/` y `SRC/ETL/gold/`.

### Semántica

La carpeta `SRC/views/` crea vistas para los consumidores analíticos en `bootcamp.semantica`:

- `v_metricas_zona`: cantidad de propiedades, precios promedio y mediana, precio por metro cuadrado, superficie y ambientes por zona y operación.
- `v_comparativa_zonas`: comparación de propiedades en venta en USD por partido.
- `v_propiedades_completa`: vista desnormalizada combinando la tabla de hechos con sus dimensiones.
- `v_propiedades_venta_usd`: propiedades en venta expresadas en USD.

## Cómo implementar `SRC/`

### 1. Preparar el entorno

Ejecuta los notebooks en un workspace de Databricks con acceso a Unity Catalog, Spark SQL, PySpark y Delta Lake.

Crea o verifica el catálogo y los esquemas:

```sql
CREATE CATALOG IF NOT EXISTS bootcamp;

CREATE SCHEMA IF NOT EXISTS bootcamp.bronze;
CREATE SCHEMA IF NOT EXISTS bootcamp.silver;
CREATE SCHEMA IF NOT EXISTS bootcamp.gold;
CREATE SCHEMA IF NOT EXISTS bootcamp.semantica;
```

Carga el archivo fuente en el volumen esperado:

```text
/Volumes/bootcamp/landing/archivos/properties_raw.csv
```

El CSV debe incluir, entre otras, las columnas utilizadas por el pipeline: `precio`, `moneda`, `tipo_de_operacion`, `ambientes`, `metros_cuadrados_totales`, `metros_cuadrados_cubiertos`, `antiguedad`, `cochera`, `estado`, `orientacion_inmueble`, `zona`, `fecha` y `url`.

### 2. Ejecutar los DDL de Bronze y Silver

En Databricks, importa y ejecuta:

```text
SRC/DDL/bronze/DDL_properties_bronze.ipynb
SRC/DDL/silver/DDL_properties_silver.ipynb
```

Después verifica las tablas:

```sql
SHOW TABLES IN bootcamp.bronze;
SHOW TABLES IN bootcamp.silver;
```

### 3. Cargar Bronze

Ejecuta:

```text
SRC/ETL/bronze/ETL_properties_bronze.ipynb
```

El proceso lee `properties_raw.csv`, mantiene inicialmente los campos como texto y sobrescribe `bootcamp.bronze.properties_bronze` con los registros que tienen URLs válidas.

Valida la carga:

```sql
SELECT COUNT(*) AS registros_bronze
FROM bootcamp.bronze.properties_bronze;

SELECT *
FROM bootcamp.bronze.properties_bronze
LIMIT 10;
```

### 4. Ejecutar EDA y limpieza Silver

Usa los notebooks en este orden:

```text
SRC/EDA/EDA_quality_bronze.ipynb
SRC/ETL/silver/ETL_properties_silver.ipynb
SRC/EDA/EDA_validation_silver.ipynb
```

`EDA_quality_bronze.ipynb` crea una vista temporal `bronze_EDA` y aplica controles sobre números, monedas, superficies, pisos y URLs. Luego Silver tipa, filtra, estandariza y deduplica la información antes de escribir en `bootcamp.silver.propiedades`.

Valida Silver:

```sql
SELECT
    COUNT(*) AS total_silver,
    COUNT(CASE WHEN precio <= 0 THEN 1 END) AS precio_invalido,
    COUNT(CASE WHEN metros_cuadrados_totales <= 0 THEN 1 END) AS m2_invalido,
    COUNT(CASE WHEN precio_por_m2 IS NULL THEN 1 END) AS precio_m2_nulo
FROM bootcamp.silver.propiedades;
```

### 5. Crear y cargar Gold

Ejecuta primero los DDL de las dimensiones y la tabla de hechos:

```text
SRC/DDL/gold/DDL_dim_zona.sql.ipynb
SRC/DDL/gold/DDL_dim_tipo_operacion.sql.ipynb
SRC/DDL/gold/DDL_dim_tiempo.sql.ipynb
SRC/DDL/gold/DDL_dim_caracteristicas.sql.ipynb
SRC/DDL/gold/DDL_dim_orientacion.ipynb
SRC/DDL/gold/DDL_fact_propiedades.sql.ipynb
```

Después ejecuta los ETL en este orden:

```text
SRC/ETL/gold/ETL_dim_zona.sql.ipynb
SRC/ETL/gold/ETL_dim_tipo_operacion.sql.ipynb
SRC/ETL/gold/ETL_dim_tiempo.sql.ipynb
SRC/ETL/gold/ETL_dim_caracteristicas.sql.ipynb
SRC/ETL/gold/ETL_dim_orientacion.ipynb
SRC/ETL/gold/ETL_fact_propiedades.sql.ipynb
```

El hecho se construye desde `bootcamp.silver.propiedades`, genera el `row_hash`, obtiene las claves de las dimensiones y conserva las métricas necesarias para el análisis.

### 6. Crear las vistas semánticas

Ejecuta los notebooks de `SRC/views/`:

```text
SRC/views/v_metricas_zona.ipynb
SRC/views/v_comparativa_zonas.ipynb
SRC/views/v_propiedades_completa.ipynb
SRC/views/v_propiedades_venta_usd.ipynb
```

Luego consulta, por ejemplo:

```sql
SELECT *
FROM bootcamp.semantica.v_metricas_zona
ORDER BY total_propiedades DESC;
```

## Workflow recomendado

El orden completo de ejecución es:

```text
1. Crear catálogo y schemas
        │
        ▼
2. Cargar properties_raw.csv en landing
        │
        ▼
3. Ejecutar DDL Bronze y Silver
        │
        ▼
4. ETL Bronze: properties_raw.csv → properties_bronze
        │
        ▼
5. EDA Bronze y controles iniciales
        │
        ▼
6. ETL Silver: limpieza → tipado → deduplicación → propiedades
        │
        ▼
7. Validar Silver
        │
        ▼
8. Ejecutar DDL de dimensiones y fact table Gold
        │
        ▼
9. Cargar dimensiones Gold
        │
        ▼
10. Cargar fact_propiedades
        │
        ▼
11. Ejecutar Data Quality
        │
        ▼
12. Crear vistas semánticas
        │
        ▼
13. Consultar métricas y productos analíticos
```

## Controles de calidad

`SRC/DQ/DQ_validaciones.ipynb` contiene validaciones para:

- Comparar el volumen de registros entre Bronze, Silver y Gold.
- Detectar claves foráneas huérfanas mediante `LEFT JOIN ... IS NULL`.
- Validar rangos de precio, metros cuadrados y cantidad de ambientes.
- Confirmar que Gold conserve la relación esperada con Silver.

Ejemplo de validación de integridad referencial:

```sql
SELECT COUNT(*) AS huerfanos
FROM bootcamp.gold.fact_propiedades fp
LEFT JOIN bootcamp.gold.dim_zona dz
    ON fp.zona_id = dz.zona_id
WHERE dz.zona_id IS NULL;
```

El resultado esperado es `0` para cada dimensión.

## Consideraciones importantes

- Los notebooks deben ejecutarse en Databricks; no son scripts Python independientes.
- Las rutas `/Volumes/bootcamp/...` y las tablas `bootcamp.*` dependen de la configuración del workspace.
- El pipeline usa `INSERT OVERWRITE` en varios puntos, por lo que las ejecuciones completas reemplazan el contenido de la tabla destino.
- `ETL_fechas.ipynb` contiene una variante de carga incremental/full con Delta Lake y widgets `fecha_carga` y `modo`. Antes de usarla en este pipeline, hay que conectar explícitamente el DataFrame `df_silver_source` y confirmar que la clave `id_evento` corresponda al modelo de propiedades.
- `EDA_validation_silver.ipynb` está creado como notebook de validación, pero actualmente no contiene lógica ejecutable; las validaciones activas se encuentran en `SRC/DQ/DQ_validaciones.ipynb`.
- Es recomendable parametrizar el catálogo, el volumen de landing y el modo de carga antes de llevar el pipeline a un entorno productivo.

## Material de aprendizaje

Las carpetas semanales documentan la evolución del proyecto:

- **Semana 1:** joins y consultas SQL.
- **Semana 2:** análisis exploratorio, CTEs y window functions.
- **Semana 3:** creación de schemas Bronze/Silver/Gold y pipeline de propiedades.
- **Semana 4 y 5:** ejercicios de transformación y análisis.
- **Semana 6:** ejercicios organizados por módulos.
- **Semana 8:** consumo de APIs externas y escritura en tablas Delta Bronze.
- **Semana 9:** controles de volumen, integridad referencial y validaciones de rangos.

## Estado del proyecto

El repositorio funciona como un proyecto educativo y una base para un pipeline inmobiliario en Databricks. La arquitectura principal está implementada en `SRC/`, mientras que las carpetas semanales contienen ejercicios complementarios y ejemplos para extender el flujo.
