# MIAAD_ING_DATOS_proyecto_final

Proyecto final de la materia Ingeniería de Datos.

## Objetivo
Construir un pipeline ELT end-to-end utilizando Airbyte, MotherDuck, dbt y Prefect.

## Tecnologías utilizadas
- Airbyte
- MotherDuck / DuckDB
- dbt Core
- Prefect
- Metabase

## Estructura del proyecto
- `models/`: modelos dbt
- `prefect_flows/`: flujos de orquestación
- `tests/`: pruebas
- `macros/`: macros dbt
- `seeds/`: datos semilla, si aplica

## Ejecución general
1. Sincronizar fuentes con Airbyte
2. Ejecutar transformaciones con dbt
3. Ejecutar tests con dbt
4. Orquestar el flujo con Prefect

## Autoras
Fátima Barrios
Claudia Coronel
Graciela Vera