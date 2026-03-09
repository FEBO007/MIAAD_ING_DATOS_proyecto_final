# Fiction Weekly Radar

Pipeline ELT end-to-end para analizar la evolución semanal del ranking **New York Times Hardcover Fiction**, enriquecido con **metadata editorial de Google Books** y **señales de atención pública de Wikipedia Pageviews**.

## 1. Descripción del proyecto

**Fiction Weekly Radar** integra múltiples APIs heterogéneas, centraliza los datos en un warehouse analítico, aplica modelado con dbt y expone una capa final de consumo optimizada para BI.

La solución fue construida con el siguiente stack:

- **Airbyte** para extracción y carga
- **MotherDuck / DuckDB** como warehouse
- **dbt + dbt-expectations** para transformación, testing y documentación
- **Prefect + prefect-dbt** para orquestación
- **Metabase** para visualización

El proyecto sigue un enfoque **ELT** y un modelo **híbrido Kimball + One Big Table (OBT)**:
- **Kimball** aporta estructura analítica, gobernanza y reutilización
- **OBT** simplifica el consumo en BI y reduce joins en la capa de visualización

---

## 2. Objetivo analítico

Construir un radar semanal que permita analizar:

- desempeño editorial del ranking NYT
- permanencia y dinámica de los libros en el ranking
- contexto editorial y reputación desde Google Books
- atención pública digital desde Wikipedia Pageviews

---

## 3. Arquitectura general

```text
Fuentes externas
   ├── NYT Books API
   ├── Google Books API
   └── Wikipedia Pageviews API
          ↓
Ingesta (Airbyte)
          ↓
Raw (MotherDuck / schema main)
          ↓
Transformación y calidad (dbt)
          ↓
Capa semántica final (OBT)
          ↓
Orquestación (Prefect)
          ↓
BI / Dashboard (Metabase)

```
## Principios de diseño

- separación clara entre ingesta, almacenamiento, transformación, orquestación y visualización
- trazabilidad de datos desde raw hasta BI
- modularidad y mantenibilidad
- reproducibilidad de la ejecución

## 4. Fuentes de datos

### 4.1 New York Times Books API
Fuente principal del proyecto.

**Granularidad:** `Libro – Semana`

Aporta:
- ranking semanal
- título
- autor
- editorial
- semanas en lista
- ranking de la semana anterior
- URL de Amazon
- imagen del libro

### 4.2 Google Books API
Fuente de enriquecimiento editorial.

**Granularidad:** `Libro`

Aporta:
- publisher
- published date
- categories
- language
- average rating
- ratings count

### 4.3 Wikipedia Pageviews API
Fuente de atención pública digital.

**Granularidad:** `Autor – Día`

Aporta:
- views por artículo
- granularidad temporal diaria
- series temporales para agregación semanal

## 5. Estrategia de ingesta en Airbyte

Se utilizaron 2 conexiones finales:

- `nyt_parent_googlebooks`
- `wiki_pageviews`

### Sync mode adoptado
Se definió una estrategia **append-only** sobre la capa raw:

- `hardcover_fiction_by_date` (NYT) → `Full refresh | Append`
- `volumes_by_isbn` (Google Books) → `Full refresh | Append`
- `pageviews_by_article` (Wikipedia) → `Full refresh | Append`

### Justificación
La capa raw se diseñó como una landing histórica e inmutable, priorizando:
- historización
- trazabilidad
- reprocesamiento
- separación entre ingesta y transformación

La deduplicación lógica, la integración entre granularidades y la definición del grano analítico final se resuelven en dbt.

## 6. Warehouse y organización física

### Schema raw
- `airbyte_curso.main`

Contiene las tablas cargadas por Airbyte sin transformar.

### Schema analítico
- `airbyte_curso.dbt_final`

Contiene:
- staging
- intermediate
- marts
- OBT final

## 7. Modelado en dbt

### 7.1 Estructura del proyecto

```text
models/proyecto_final/
├── staging/
├── intermediate/
├── marts/
└── obt/
```

### Capas implementadas

- `stg_*` → limpieza, tipificación y normalización inicial
- `int_*` → construcción de llaves, desacople lógico y normalización
- `dim_*`, `fct_*`, `bridge_*` → modelo dimensional
- `obt_*` → tabla final optimizada para BI

### 7.2 Sources y dependencias

Las fuentes externas se declararon en:

```text
models/staging/_sources.yml
```

con:
- `database: airbyte_curso`
- `schema: main`

Se utilizó:
- `source()` para referenciar tablas raw
- `ref()` para dependencias entre modelos internos

### 7.3 Modelo híbrido Kimball + OBT

#### Modelo dimensional
Incluye:
- dimensiones de libro, autor, semana y fecha
- facts de ranking NYT, snapshots de Google Books y pageviews
- bridge table libro–autor

#### OBT final
Tabla de consumo analítico:

```text
airbyte_curso.dbt_final.obt_fiction_weekly_scorecard
```

Ventajas:
- elimina joins en la capa BI
- simplifica consultas
- mejora performance en Metabase
- conserva una narrativa analítica clara

### 7.4 Materializations

La estrategia de materialización quedó definida así:

- `staging` → `view`
- `intermediate` → `view`
- `marts` → `table`
- `obt` → `table`

#### Justificación
- `view` en capas tempranas para mantener flexibilidad y evitar persistencia innecesaria
- `table` en marts y OBT para optimizar consumo analítico y consultas frecuentes

## 8. Calidad de datos

La calidad se trató como parte estructural del pipeline.

### Dimensiones de calidad cubiertas
- integridad
- completitud
- validez
- cobertura
- consistencia referencial

### Tests nativos de dbt
Se aplicaron:
- `not_null`
- `unique`
- `relationships`

### Tests avanzados con dbt-expectations
Se implementaron validaciones sobre:
- unicidad compuesta
- volumen esperado
- obligatoriedad de campos
- rangos válidos
- regex
- dominios cerrados

### Resultado final
La corrida consolidada del proyecto finalizó con:

```text
PASS=75 WARN=0 ERROR=0 SKIP=0 NO-OP=0 TOTAL=75
```

## 9. Orquestación con Prefect

La orquestación se implementó en:

```text
prefect_flows/pipeline_proyecto_final.py
```

### Componentes principales
- `@flow` para el proceso coordinador principal
- `@task` para autenticación, disparo de syncs, espera de jobs y ejecución de dbt

### Secuencia del flow
1. obtener token de Airbyte
2. ejecutar sync de `nyt_parent_googlebooks`
3. esperar finalización del job
4. ejecutar sync de `wiki_pageviews`
5. esperar finalización del job
6. ejecutar `dbt deps`
7. ejecutar `dbt build`

### Integración con dbt
La ejecución de dbt se resolvió con:

- `prefect-dbt`
- `PrefectDbtRunner`

### Robustez operativa
El flow incorpora:
- retries en `trigger_sync()`
- polling con timeout en `wait_job()`
- refresh de token ante `401`
- validación temprana de variables de entorno
- logs centralizados en Prefect UI

### Scheduling
Deployment local:

```text
pf-semanal
```

Schedule:

```text
todos los lunes a las 06:00
timezone = America/Asuncion
```

## 10. Dashboard en Metabase

El dashboard final consume directamente la tabla:

```text
airbyte_curso.dbt_final.obt_fiction_weekly_scorecard
```

### Objetivo
Visualizar:
- desempeño semanal del ranking
- dinámica y permanencia
- metadata editorial
- atención pública digital

### Estructura narrativa
1. **This Week’s Winners**
2. **Who Are They?**
3. **Public Attention**

### Filtro global
Se implementó un filtro por semana basado en:

```text
published_date
```

### Visualizaciones principales
- KPI de control del ranking
- Top 15 por semana
- movers positivos
- movers negativos
- nuevos ingresos
- métricas de views 7d

## 11. Estructura sugerida del repositorio

```text
.
├── models/
│   └── proyecto_final/
│       ├── staging/
│       ├── intermediate/
│       ├── marts/
│       └── obt/
├── prefect_flows/
│   ├── pipeline_proyecto_final.py
│   └── .env
├── macros/
├── tests/
├── dbt_project.yml
├── packages.yml
├── profiles.yml   # no versionar si contiene credenciales reales
└── README.md
```

## 12. Variables de entorno requeridas

Crear el archivo:

```text
prefect_flows/.env
```

con al menos:

```env
AIRBYTE_PUBLIC_API_URL=http://localhost:8000/api/public/v1
AIRBYTE_CLIENT_ID=TU_CLIENT_ID
AIRBYTE_CLIENT_SECRET=TU_CLIENT_SECRET
AIRBYTE_CONN_ID_NYT_PARENT_GBOOKS=TU_CONNECTION_ID_NYT
AIRBYTE_CONN_ID_WIKI_PAGEVIEWS=TU_CONNECTION_ID_WIKI

DBT_PROJECT_DIR=/home/mti2_ubuntu/projects/dbt/mi_proyecto_dbt
DBT_PROFILES_DIR=/home/mti2_ubuntu/.dbt
DBT_TARGET=final
DBT_SELECT=path:models/proyecto_final
```

> **Importante:** no subir secretos reales al repositorio.  
> Se recomienda incluir un archivo `prefect_flows/.env.example`.

## 13. Instrucciones de ejecución reproducibles

Esta sección está pensada como runbook operativo para reproducir el proyecto de punta a punta.

### 13.1 Prerrequisitos

Antes de ejecutar, verificar:

- Airbyte local operativo
- MotherDuck accesible
- perfil dbt configurado correctamente
- credenciales válidas en `prefect_flows/.env`
- proyecto ubicado en `~/projects/dbt/mi_proyecto_dbt`

#### Entornos virtuales esperados
- `dbt-env` para ejecución manual de dbt
- `prefect-env` para ejecución del flow de Prefect

### 13.2 Activar entorno dbt (para validación manual)

```bash
cd ~/projects/dbt/mi_proyecto_dbt
source /home/mti2_ubuntu/dbt-env/bin/activate
```

Verificación opcional:

```bash
which python
which dbt
```

### 13.3 Ejecutar dbt manualmente

#### Instalar dependencias del proyecto dbt
```bash
dbt deps
```

#### Construir modelos y correr tests
```bash
dbt build --target final --select path:models/proyecto_final
```

#### Generar documentación
```bash
dbt docs generate --target final
dbt docs serve
```

#### Resultado esperado
La corrida final debería terminar sin errores y con evidencia similar a:

```text
PASS=75 WARN=0 ERROR=0
```

### 13.4 Activar entorno Prefect

Si ya estás dentro de otra venv:

```bash
deactivate
```

Luego:

```bash
cd ~/projects/dbt/mi_proyecto_dbt
source prefect_flows/prefect-env/bin/activate
```

### 13.5 Levantar Prefect UI

```bash
prefect server start
```

Abrir en navegador:

```text
http://127.0.0.1:4200
```

### 13.6 Exponer el deployment local con serve()

En otra terminal, con `prefect-env` activo:

```bash
cd ~/projects/dbt/mi_proyecto_dbt
source prefect_flows/prefect-env/bin/activate
python prefect_flows/pipeline_proyecto_final.py
```

Esto deja disponible el deployment:

```text
pf-semanal
```

### 13.7 Ejecutar el flujo completo manualmente

Para correr el pipeline completo ad hoc:

```bash
cd ~/projects/dbt/mi_proyecto_dbt
source prefect_flows/prefect-env/bin/activate
python - <<'PY'
from prefect_flows.pipeline_proyecto_final import airbyte_and_dbt_flow
airbyte_and_dbt_flow()
print("FLOW_OK")
PY
```

#### Resultado esperado
- sync exitoso de NYT/Google Books
- sync exitoso de Wikipedia
- ejecución satisfactoria de `dbt deps`
- ejecución satisfactoria de `dbt build`
- salida final:

```text
FLOW_OK
```

### 13.8 Verificación de éxito

La reproducción se considera correcta cuando se cumplen simultáneamente estas condiciones:

- ambas conexiones de Airbyte terminan exitosamente
- el flow aparece en Prefect UI
- el run finaliza en estado `Completed`
- `dbt build --target final --select path:models/proyecto_final` finaliza sin errores
- la tabla `airbyte_curso.dbt_final.obt_fiction_weekly_scorecard` queda disponible
- la evidencia final de tests muestra:

```text
PASS=75 WARN=0 ERROR=0
```

### 13.9 Troubleshooting básico

#### Error 401 en Airbyte
Verificar:
- `AIRBYTE_CLIENT_ID`
- `AIRBYTE_CLIENT_SECRET`
- `AIRBYTE_PUBLIC_API_URL`

#### Prefect no muestra el run
Verificar que `prefect server start` esté levantado y que el flow se ejecute con `prefect-env`.

#### dbt no encuentra adapter duckdb
Instalar en el entorno correspondiente:

```bash
pip install -U dbt-duckdb
```

#### dbt no encuentra dependencias de packages
Ejecutar:

```bash
dbt deps
```

## 14. Comandos útiles

### dbt
```bash
dbt deps
dbt build --target final --select path:models/proyecto_final
dbt docs generate --target final
dbt docs serve
```

### Prefect
```bash
prefect server start
python prefect_flows/pipeline_proyecto_final.py
python - <<'PY'
from prefect_flows.pipeline_proyecto_final import airbyte_and_dbt_flow
airbyte_and_dbt_flow()
print("FLOW_OK")
PY
```

## 15. Buenas prácticas para GitHub

No versionar:
- `.env`
- credenciales
- tokens
- archivos temporales de `target/`
- logs locales
- entornos virtuales

Agregar al `.gitignore`:

```gitignore
target/
logs/
dbt_packages/
.env
*.pyc
__pycache__/
prefect_flows/prefect-env/
```

## 16. Estado del proyecto

Estado actual:
- arquitectura implementada
- ingesta en Airbyte operativa
- modelo dbt construido
- tests ejecutados satisfactoriamente
- orquestación semanal implementada
- dashboard final en Metabase disponible

## 17. Autoras

- Fátima Barrios
- Graciela Vera
- Claudia Coronel