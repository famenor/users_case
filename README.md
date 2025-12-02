# FLUJO DE DATOS PARA CASO DE VISITA DE USUARIOS

## CONTENIDO

  1 - Propuesta de Solución
  2 - Perfilamiento de Datos
  3 - ETL
  4 - Estructura del Repositorio
  5 - Discusión
  
## 1.- PROPUESTA DE SOLUCIÓN

En este ejercicio se implementó un ETL que procesa una tabla con datos de visitas de usuarios y genera tablas para consumo final. Los datos se procesaron mediante la herramienta **Pyspark**, la cual se ejecutó mediante **Databricks**.

### Arquitectura

Se utilizó una arquitectura de **medallero**, donde cada nivel tiene sus propias capas:

| Nivel | Capa | Propósito |
|---|---|---|
| Bronze | Landing | Almacenar los datos originales, sin ningún cambio salvo que sea necesario para poderlo almacenar. |
| Bronze | Raw | Almacenar en formato Delta los datos originales, consolidando las versiones que llegan en cada ejecución, se incorporan algunos metadatos. |
| Silver | Audit | Almacenar los datos limpios y auditados, no se elimina ningún registro excepto aquellos que violen su llave primaria, los demás datos erroneos se anulan y se guardan en una bitácora de eventos de error. |
| Silver | Historic | Almacenar la versión histórica de la tabla auditada, se descartan registros que no hayan cumplido la auditoria. |
| Gold | Gold | Almacenar tablas que necesiten transformaciones para su consumo final. |

Los datos se organizaron con una estructura de tres niveles, siguiendo la convención CATALOGO.ESQUEMA.TABLA

Un **catálogo** se compone por el dominio y el ambiente de trabajo, se crearon tres catálogos:

![](https://github.com/famenor/users_case/blob/main/pictures/01_catalogos.jpg)

El primer dominio es para el **gobierno de datos** y el segundo dominio es general para todas las tablas a procesar, para el segundo dominio hay dos ambienes: DEV y PROD, en este ejercicio solo utilizaremos el ambiente DEV.

Un **esquema** se compone por el nivel (medalla) y subdominio, excepto en el catálogo de gobierno de datos, para este catálogo se crearon los esquemas de métricas y metadatos:

![](https://github.com/famenor/users_case/blob/main/pictures/02_esquemas.jpg)

Una **tabla** se compone por la capa y el nombre plano de la tabla, excepto en el catálogo de gobierno de datos y las tablas oro que de momento no tienen convención establecida:

![](https://github.com/famenor/users_case/blob/main/pictures/03_tablas.jpg)

También se generó una tabla de gobierno de datos que almacena las principales especificaciones de las columnas que serán ingestadas:

![](https://github.com/famenor/users_case/blob/main/pictures/04_tablas_detalle.jpg)

### Métricas de Ingesta de Datos

Para la ingesta de datos se crearon dos tablas, la primera almacena estadísticas de ingesta de datos:

![](https://github.com/famenor/users_case/blob/main/pictures/05_ingesta.jpg)

mientras que la segunda tabla almacena los **eventos de error** (bitácora):

![](https://github.com/famenor/users_case/blob/main/pictures/06_eventos_error.jpg)

Se aclara que con excepción del error de llave primeria, los demás errores fueron introducidos deliberadamente para fines del ejercicio.

La creación de las tablas de gobierno de datos (metadatos y métricas) se implementó en el cuaderno appendix_a_init_governance, a continuación se muestra una captura en la sección de catálogos de Databricks donde se muestran los catálogos, esquemas y tablas de gobierno creados:

![](https://github.com/famenor/users_case/blob/main/pictures/07_tablas_gobierno.jpg)

### Enfoque de metadatos

En la sección anterior se mostraron dos tablas de gobierno con datos referentes a las tablas ingestadas y sus columnas, esta información se capturó a partir de archivos **YAML** en los que describe el estado deseado de la tabla antes de ser procesada, si bien este enfoque puede ser complicado de implementar, es posible facilitar tareas repetitivas y mejorar la documentación de los activos de datos. 

Para este ejercicio se implementó un enfoque declarativo para algunas de las tareas más comunes y repetitivas, se propuso el siguiente formato:

~~~YAML
name: visitas
layers:
  raw:
    write_mode: overwrite_partition
    version: 1
    description: Metrics of user visits as in the original source
    owner: armando.n90@gmail.com
    retention_policy: permanent
    schema:
      email:
        data_type: string
        is_pii: True
        is_primary_key: True
        is_nullable: False
        is_partition: False
        comment: 'Email of the user'
      jyv:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'No description provided'
      Badmail:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Indicates if the email could not be validates'
      Baja:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'No description provided'
      Fechaenvio:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Date when the metrics were sent'
      Fechaopen:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Date when the page was open'
      Opens:
        data_type: integer
        is_nullable: True
        is_partition: False
        comment: 'Amount of opens'
      Opensvirales:
        data_type: integer
        is_nullable: True
        is_partition: False
        comment: 'Amount of massive opens'
      Fechaclick:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Date when the clicks were executed'
      Clicks:
        data_type: integer
        is_nullable: True
        is_partition: False
        comment: Amount of clicks'
      Clicksvirales:
        data_type: integer
        is_nullable: True
        is_partition: False
        comment: 'Amount of massive clicks'
      Links:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Links'
      IPs:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Logical Addresses'
      Navegadores:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Browsers'
      Plataformas:
        data_type: string
        is_nullable: True
        is_partition: False
        comment: 'Platforms'
  audit:
    write_mode: merge_incremental
    version: 1
    description: Metrics of user visits audited
    owner: armando.n90@gmail.com
    retention_policy: permanent
    schema:
      Email:
        rename_from: email
        validations:
          - validation: is_not_null
          - validation: is_email_format
      Jyv:
        rename_from: jyv
      Badmail:
        validations:
          - validation: is_in_list
            allowed: 'HARD'
      Baja:
        validations:
          - validation: is_in_list
            allowed: 'SI'
      FechaEnvio:
        rename_from: Fechaenvio
        data_type: timestamp
        validations:
          - validation: is_date_format
            format: 'dd/MM/yyyy HH:mm'
      FechaOpen:
        rename_from: Fechaopen
        data_type: timestamp
        validations:
          - validation: is_date_format
            format: 'dd/MM/yyyy HH:mm'
      Opens:
        validations:
          - validation: is_in_bounds
            min_allowed: 0
            max_allowed: 20
      OpensVirales:
        rename_from: Opensvirales
        validations:
          - validation: is_in_bounds
            min_allowed: 0
            max_allowed: 20
      FechaClick:
        rename_from: Fechaclick
        data_type: timestamp
        validations:
          - validation: is_date_format
            format: 'dd/MM/yyyy HH:mm'
      Clicks:
        validations:
          - validation: is_in_bounds
            min_allowed: 0
            max_allowed: 20
      ClicksVirales:
        rename_from: Clicksvirales
        validations:
          - validation: is_in_bounds
            min_allowed: 0
            max_allowed: 20
      Links:
        rename_from: Links
      IPs:
        rename_from: IPs
      Navegadores:
        rename_from: Navegadores
      Plataformas:
        rename_from: Plataformas
  historic:
    write_mode: historic
    version: 1
    description: Metrics of user visits with history
    owner: armando.n90@gmail.com
    retention_policy: permanent
    schema:
      Email:
        track_changes: True
      Jyv:
        track_changes: True
      Badmail:
        track_changes: True
      Baja:
        track_changes: True
      FechaEnvio:
        track_changes: True
      FechaOpen:
        track_changes: True
      Opens:
        track_changes: True
      OpensVirales:
        track_changes: True
      FechaClick:
        track_changes: True
      Clicks:
        track_changes: True
      ClicksVirales:
        track_changes: True
      Links:
        track_changes: True
      IPs:
        track_changes: True
      Navegadores:
        track_changes: True
      Plataformas:
        track_changes: True
~~~

Para la capa **raw** se capturan llaves primarias, si es dato personal, particiones, tipos de dato originales y el comentario de la columna; para la capa **audit** se especifican renombres, validaciones y tipos de dato finales; mientras que para la capa **historic** se especifican qué columnas serán seguidas para control de cambios. En algunos casos la información de una capa se replica a la capa siguiente cuando ésta sea necesaria y así evitar capturarla dos veces (pero se puede especificar si necesita cambiarse para la siguiente capa).

En el cuaderno appendix_b_metadata_manager se realizó la ingesta de los metadatos especificados en el archivo YAML hacia las tablas de gobierno de datos correspondientes.

![](https://github.com/famenor/users_case/blob/main/pictures/08_ingesta_metadatos.jpg)

### Almacenamiento

Las tablas intermedias serán almacenadas en el **lakehouse** de Databricks siguiendo las convensiones antes mencionadas, para la Base de Datos de entregables se utilizará el motor de **DuckDB** para emular la Base de Datos en **MySQL**, las sentencias SQL utilizadas son compatibles entre ambos motores. La creación de tablas se ejecutó en el cuaderno appendix_c_init_data_base:

- Tabla visitor con conteos de visitas por usuario.
- Tabla statistics con los registros recibidos en cada batch.
- Table event_errors con los registros de la bitacora de errores.

![](https://github.com/famenor/users_case/blob/main/pictures/09_tablas_duckdb.jpg)

### Orquestación 

El ETL se dividió en 4 etapas que más adelante se detallarán, cada etapa se ejecuta en un cuaderno en Databricks, para ejecutarlas se creó una **canalización**, la cual está representada en el siguiente grafo:

![](https://github.com/famenor/users_case/blob/main/pictures/10_tablas_duckdb.jpg)

Los notebooks necesitan un **rundate** para saber qué archivos o tablas van a procesar, el rundate 20130208_000000 procesa los archivos con batches 7 y 8; mientras que el rundate 20130214_000000 procesa el archivo con batch 9.

Se utilizó el orquestador **Airflow** para ejecutar bajo demanda cualquiera de las dos ejecuciones:

![](https://github.com/famenor/users_case/blob/main/pictures/11_airflow.jpg)

En un ambiente productivo solo sería necesario un **DAG** programado para ejecutar los rundates automáticamente, sin embargo para este ejercicio se crearon dos DAGs, los cuales envían los rundates 20130208_000000 y 20130214_000000 a Databricks bajo demanda respectivamente. 

![](https://github.com/famenor/users_case/blob/main/pictures/12_airflow.jpg)

## 2.- PERFILAMIENTO DE DATOS


~~~python
        #CHECK NULL VALUES


~~~
