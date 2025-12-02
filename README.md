# FLUJO DE DATOS PARA CASO DE VISITA DE USUARIOS

## CONTENIDO

  1 - Propuesta de Solución

  2 - Perfilamiento de Datos

  3 - ETL

  4 - Estructura del Repositorio
  
  5 - Discusión y Mejoras
  
## 1.- PROPUESTA DE SOLUCIÓN

En este ejercicio se implementó un ETL que procesa una tabla con datos de visitas de usuarios y genera tablas para consumo final. Los datos se procesaron mediante la herramienta **Pyspark**, la cual se ejecutó mediante **Databricks**.

### Arquitectura de Datos

Se utilizó una arquitectura de **medallero**, donde cada nivel tiene sus propias capas:

| Nivel | Capa | Propósito |
|---|---|---|
| Bronze | Landing | Almacenar los datos originales, sin ningún cambio salvo que sea necesario para poderlo almacenar. |
| Bronze | Raw | Almacenar en formato Delta los datos originales, consolidando las versiones que llegan en cada ejecución, se incorporan algunos metadatos. |
| Silver | Audit | Almacenar los datos limpios y auditados en formato Delta, no se elimina ningún registro excepto aquellos que violen su llave primaria, los demás datos erroneos se anulan y se guardan en una bitácora de eventos de error. |
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

La creación de las tablas de gobierno de datos (metadatos y métricas) se implementó en el cuaderno *appendix_a_init_governance*, a continuación se muestra una captura en la sección de catálogos de Databricks donde se muestran los catálogos, esquemas y tablas de gobierno creados:

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

En el cuaderno *appendix_b_metadata_manager* se realizó la ingesta de los metadatos especificados en el archivo YAML hacia las tablas de gobierno de datos correspondientes.

![](https://github.com/famenor/users_case/blob/main/pictures/08_ingesta_metadatos.jpg)

### Almacenamiento

Las tablas intermedias serán almacenadas en el **lakehouse** de Databricks siguiendo las convensiones antes mencionadas, para la Base de Datos de entregables se utilizará el motor de **DuckDB** para emular la Base de Datos en **MySQL**, las sentencias SQL utilizadas son compatibles entre ambos motores. La creación de tablas se ejecutó en el cuaderno *appendix_c_init_data_base*:

- Tabla *visitor* con conteos de visitas por usuario.
- Tabla *statistics* con los registros recibidos en cada *batch*.
- Table *event_errors* con los registros de la bitacora de errores.

![](https://github.com/famenor/users_case/blob/main/pictures/09_tablas_duckdb.jpg)

### Orquestación 

El ETL se dividió en 4 etapas que más adelante se detallarán, cada etapa se ejecuta en un cuaderno de Databricks, para ejecutarlas se creó una **canalización**, la cual está representada en el siguiente grafo:

![](https://github.com/famenor/users_case/blob/main/pictures/10_canalizacion.jpg)

Los cuadernos necesitan un **rundate** para saber qué archivos o tablas van a procesar, el *rundate* 20130208_000000 procesa los archivos con batches 7 y 8; mientras que el *rundate* 20130214_000000 procesa el archivo con batch 9.

Se utilizó el orquestador **Airflow** para ejecutar bajo demanda cualquiera de las dos ejecuciones:

![](https://github.com/famenor/users_case/blob/main/pictures/11_airflow.jpg)

En un ambiente productivo solo sería necesario un **DAG** programado para ejecutar los rundates automáticamente, sin embargo para este ejercicio se crearon dos DAGs, los cuales envían los *rundates* 20130208_000000 y 20130214_000000 a Databricks bajo demanda respectivamente. 

![](https://github.com/famenor/users_case/blob/main/pictures/12_airflow.jpg)

## 2.- PERFILAMIENTO DE DATOS

El perfil de los datos es un paso importante con el cual fue posible definir parte de los metadatos, validaciones y tipos de datos necesarios durante el procesamiento.

A continuación se listan algunos problemas o rasgos encontrados:

- En el *batch* 7 hay dos registros con el mismo Email, para este ejercicio se asumurá que no se puede repetir un Email en el mismo *batch*.
- Las columnas no estaban homologadas, concretamente las columnas *jk* y *fhg* se renombraron por *jyv*:

![](https://github.com/famenor/users_case/blob/main/pictures/13_perfil_columnas.jpg)

- La columna *jvy* tiene todos los valores nulos.
- *Badmail* puede ser nulo o HARD.
- *Baja* puede ser nulo o SI.
- En algunas columnas el valor nulo está representado por un guión medio.
- Las fechas de envio, *open* y *click* tienen fechas con formato dd/MM/yyyy HH:mm
- *Opens*, *Opens virales*, *Clicks* y *Clicks virales* son enteros con valores entre 0 y 10.
- *Links*, *IPs*, *Navegadores* y *Plataformas* contienen arreglos o valores simples, sin embargo a veces hay valores *Unknown* o elementos nulos en las listas:

![](https://github.com/famenor/users_case/blob/main/pictures/14_perfil_arreglos.jpg)

En el cuaderno *appendix_d_profiling* se encuentra el perfilamiento realizado.

## 3.- ETL

El proceso ETL se dividió en 4 pasos que serán detallados en esta sección.

### A) Carga al Servidor

Primero se copian los archivos de la fuente externa (carpeta *files*) hacia el servidor (carpeta *server_device*) y hacia la zona *landing* del **lakehouse** (carpeta *lakehouse/landing*):

![](https://github.com/famenor/users_case/blob/main/pictures/15_carga_servidor.jpg)

Después se crea un archivo ZIP con el **respaldo** de los archivos fuente y se guarda en el almacenamiento local (carpeta *local_device*):

![](https://github.com/famenor/users_case/blob/main/pictures/16_zip.jpg)

Finalmente se eliminan los archivos del servidor (carpeta *server_device*):

![](https://github.com/famenor/users_case/blob/main/pictures/17_depurar_servidor.jpg)

El cuaderno *01_server_load* contiene el código orquestado de esta sección.

### B) Generación de Tablas Bronce y Plata

El segundo proceso comienza buscando todos los *batches* (o archivos) asociados al *rundate*, si el *batch* ya ha sido procesado entonces éste será omitido:

![](https://github.com/famenor/users_case/blob/main/pictures/18_no_reprocesar.jpg)

Si el *batch* no ha sido procesado, entonces se ejecuta lo siguiente para la capa **RAW**:

- Extraer metadatos (de tablas de gobierno) y tabla fuente (de capa *landing*).
- Validar que las columnas empaten con las declaradas en los metadatos.
- Aplicar los tipos de datos definidos en los metadatos, estos tipos de datos no son finales, son los necesarios para poder almacer el dato aún con posibles errores.
- Validar que las columnas declaradas como no nulas tengan datos completos.
- Agregar comentarios de columnas.
- Generar métricas de extracción.
- Exportar tabla (hacia capa *raw*) y métricas.

![](https://github.com/famenor/users_case/blob/main/pictures/19_land_to_raw.jpg)

A continuación se muestra uno de los registros que contiene errores (en este caso intencionalmente introducidos) para resaltar que en esta capa se conservan los datos como venían originalmente:

![](https://github.com/famenor/users_case/blob/main/pictures/20_tabla_raw.jpg)

Y las métricas de ingesta hacia la capa *raw* de los tres *batches*:

![](https://github.com/famenor/users_case/blob/main/pictures/21_metrica_raw.jpg)

También se muestra cómo los comentarios capturados en los metadados YAML ahora están disponibles en el **catálogo** de Databricks:

![](https://github.com/famenor/users_case/blob/main/pictures/22_comentarios.jpg)

Posteriormente se procede a generar la tabla de la capa **audit**, ejecutando lo siguiente:

- Extraer metadatos (de tablas de gobierno) y tabla cruda (de capa *raw*).
- Renombrar columnas conforme a lo declarado los metadatos.
- Aplicar preprocesamiento específico.
- Aplicar las validaciones declaradas en los metadatos.
- Castear columnas con tipos de dato finales declarados en los metadatos.
- Generar métricas de extracción.
- Exportar tabla (hacia capa *audit*) y métricas.

![](https://github.com/famenor/users_case/blob/main/pictures/23_raw_to_audit.jpg)

A continuación se muestra el mismo registro de la capa anterior, esta vez con los campos erroneos anulados y con una etiqueta que indica que no pasó la **auditoría**, otros campos que tenían guiones fueron sustituidos por valores nulos:

![](https://github.com/famenor/users_case/blob/main/pictures/24_tabla_audit.jpg)

en la bitácora de errores se almacenan los problemas encontrados:

![](https://github.com/famenor/users_case/blob/main/pictures/25_errores_audit.jpg)

Y las métricas de ingesta hacia la capa *audit* de los tres *batches*:

![](https://github.com/famenor/users_case/blob/main/pictures/26_metrica_audit.jpg)

Finalmente se procede a generar la tabla de la capa **historic** ejecutando lo siguiente:

- Extraer metadatos (de tablas de gobierno) y tabla limpia (de capa *audit*, considerando solo registros que hayan cumplido la auditoría).
- Consolidar cambios históricos, se generan registros cada vez que uno de los campos marcados para seguimiento tenga algún cambio.
- Generar métricas de procesamiento.
- Exportar tabla (hacia capa *historic*) y métricas.

![](https://github.com/famenor/users_case/blob/main/pictures/27_audit_to_historic.jpg)

En la tabla histórica existen tres campos especiales para indicar si es el más reciente y las fechas de validez:

![](https://github.com/famenor/users_case/blob/main/pictures/28_tabla_historic.jpg)

También se generan métricas de ingesta:

![](https://github.com/famenor/users_case/blob/main/pictures/29_metrica_historic.jpg)

En este punto se han generado las tablas auditadas e históricas que servirán para construir las tablas de consumo final, el cuaderno *02_bronze_to_silver_tables* contiene el código orquestado de esta sección.

### C) Generación de Tablas Oro

El tercer proceso se encarga de generar las tablas oro, para este ejercicio se generó una tabla oro que contiene el resumen por usuario de las visitas realizadas:

![](https://github.com/famenor/users_case/blob/main/pictures/30_tabla_oro.jpg)

Cada vez que se ejecuta el *rundate*, esta tabla se actualiza mediante el cuaderno *03_gold_tables* que contiene el código orquestado de esta sección.

### D) Exportar a la Base de Datos

El cuarto proceso se encarga de exportar los diferenciales a la Base de Datos, en todos los casos se utiliza un filtro para considerar los registros ligados al *rundate* correspondiente.

La primera tabla a actualizar es *visitor* y contiene los datos de la tabla oro generada en el punto anterior:

~~~sql
WITH filter AS (
    SELECT DISTINCT Email FROM domain_dev.silver_analytics.audit_visitas a
    WHERE a.metadata_batch_id IN 
    (
        SELECT batch_id FROM governance_prod.metrics.ingestions 
        WHERE rundate = '{rundate}' AND catalog_name='domain_dev' AND schema_name='silver_analytics' 
        AND table_name='audit_visitas'
    )
)
SELECT v.* FROM domain_dev.gold_analytics.visitor v
INNER JOIN filter f ON v.Email = f.Email
~~~

![](https://github.com/famenor/users_case/blob/main/pictures/31_visitor.jpg)

La segunda tabla a actualizar es *statistic* y contiene los datos de la tabla auditada de visitas:

~~~sql
WITH filter AS (
    SELECT DISTINCT batch_id FROM governance_prod.metrics.ingestions 
    WHERE rundate = '{rundate}' AND catalog_name='domain_dev' AND schema_name='silver_analytics' 
        AND table_name='audit_visitas'
)
SELECT Email, Jyv, Badmail, Baja, FechaEnvio, FechaOpen, Opens, OpensVirales, FechaClick, Clicks, ClicksVirales,
    Links, IPs, Navegadores, Plataformas, metadata_batch_id 
FROM domain_dev.silver_analytics.audit_visitas v
WHERE metadata_batch_id IN (SELECT batch_id FROM filter) AND v.metadata_audit_passed = TRUE
~~~

![](https://github.com/famenor/users_case/blob/main/pictures/32_statistic.jpg)

La tercera tabla a actualizar es *event_errors* y contiene los datos de la bitácora de errores:

~~~sql
WITH filter AS (
    SELECT DISTINCT batch_id FROM governance_prod.metrics.ingestions 
    WHERE rundate = '{rundate}' AND catalog_name='domain_dev' AND schema_name='silver_analytics' 
        AND table_name='audit_visitas'
)
SELECT *
FROM governance_prod.metrics.event_errors v
WHERE batch_id IN (SELECT batch_id FROM filter)
~~~

![](https://github.com/famenor/users_case/blob/main/pictures/33_event_errors.jpg)

El cuaderno *04_export_to_database* contiene el código orquestado de esta sección, también hay una copia de los **entregables** con formato CSV en la carpeta */local_device/home/mysql*.

## 4.- ESTRUCTURA DEL REPOSITORIO

El proyecto está organizado de la siguiente manera:

- /application

Contiene los cuadernos orquestados (*01_server_load*, *02_bronze_to_silver_tables*, *03_gold_tables* y *04_export_to_database*) y los cuadenos no orquestados (*appendix*) utilizados para inicializar los recursos y explorar datos.

- /application/lib

Contiene las clases concretas e interfaces utilizadas por los procesos principales:

a) *Gateways* para interactuar con la Base de Datos (archivos Delta de Databricks) y archivos planos.

b) *Hooks* de preprocesamiento en la capa *audit*.

c) *Interactors* para activos de datos, tablas de gobierno, llaves surrogadas y validaciones.

d) Plantillas y sus fábricas abstractas utilizadas para el procesamiento de tablas.

- /files

Contiene los archivos originales organizados por rundate, estas carpetas representan lo que en producción sería enviado hacia el servidor.

- /lakehouse/governance

Contiene los archivos YAML con la especificacion de los metadatos.

- /lakehouse/landing

Contiene los archivos de la capa landing que serán el punto de inicio para el ETL.

- /local_device/home/etl

Contiene los archivos ZIP con los respaldos de los archivos cargados en el servidor.

- /local_device/home/mysql

Contiene la base de datos en duckdb utilizada para emular MySQL, así como una copia de los **entregables**.

- /orquestacion

Contiene los DAGs de Airflow utilizados para orquestar el ETL.

- /pictures

Contiene las imagenes utilizadas en el archivo README.md

- /service_device

Contiene la estructura de archivos utilizada en el servidor real, se utiliza como almacenamiento temporal para emular la carga al servidor y su posterior respaldo y depuración.

## 5.- DISCUSION Y MEJORAS

En esta última sección se discuten algunos aspectos técnicos y posibles implementaciones a futuro.

### Enfoque de Metadatos

El enfoque de metadatos fue muy útil para ejecutar tareas repetitivas, nuevos archivos que tengan patrones similares podrían ser procesados rápidamente y se tendría un éstandar para documentación. La dificultad es que se tienen que implementar validaciones de los archivos YAML y comunmente habrá situaciones que se salgan de los patrones comunes, sin embargo para estos últimos se puede hacer uso de **hooks** como se hizo en el proceso de auditoría.

Como trabajo a futuro quedaría plantear cómo se pueden especificar los *hooks*, las tablas oro y los entregables en los metadatos.

### Arquitectura de Software

Se procuró seguir los principios *SOLID* y los principios de desarrollo de componentes, aunque quedaron partes pendientes de refactorizar y algunas podrían mejorarse, algunas características de la arquitectura son:

- Uso de plantillas (patrón de diseño **template**) para el procesamiento entre las capas *land* y *raw*; *raw* y *audit*; así como *audit* e *historic*, las plantillas contienen un método principal que se encarga de ejecutar ordenadamente los métodos a cargo de las actividades específicas, uno de estos métodos ejecuta el *hook* del que se hablaŕa en la siguiente sección.

~~~python
#INTERFACE FOR THE TEMPLETE
class InterfaceRawToAuditTemplate(ABC):

    #@abstractmethod
    def set_component_factory(self, component_factory: AbstractFactoryRawToAudit):
        pass

    @abstractmethod
    def process(self):
        pass

#IMPLEMENTATION FOR THE TEMPLATE
class RawToAuditTemplate(InterfaceRawToAuditTemplate):

    def __init__(self, catalog_name: str, schema_name: str, table_name: str, rundate: str, batch_id: str):
        ...
        
    def set_component_factory(self, component_factory: AbstractFactoryRawToAudit):
        self.governance_interactor = component_factory.governance_interactor
        self.asset_interactor = component_factory.asset_interactor
        self.hash_key_interactor = component_factory.hash_key_interactor
        self.facade_screen_validator = component_factory.facade_screen_validator
        self.hook_preprocessing = component_factory.hook_preprocessing
  
    def process(self):

        #EXTRACTION STEPS
        self.extract_metadata()
        self.extract_table()

        #PROCESSING STEPS
        self.rename_columns()
        self.apply_hook_preprocessing()
        self.validate_screens()
        self.cast_data_types()
        self.add_metadata()
        self.generate_errors_dataframe()
        self.discard_primary_key_errors()

        #EXPORTING STEPS
        self.write_table()
        self.get_accumulated_rows()
        self.generate_ingestion_metrics()
        self.write_metrics()
        self.write_errors()
~~~

- Uso de fábricas abstractas (patrón de diseño **abstract factory**) para crear los grupos de objectos que las plantillas necesitan para funcionar.

~~~python
#ABTRACT FACTORY WITH METHODS TO BE IMPLEMENTED
class AbstractFactoryRawToAudit(ABC):

    @abstractmethod
    def create(self):
        pass

#CONCRETE FACTORY FOR VISITS
class FactoryRawToAuditForVisit(AbstractFactoryRawToAudit):

    def __init__(self):
        pass
        
    def create(self):

        database_gateway = SparkSQLDatabaseGateway()
  
        self.governance_interactor = GovernanceInteractor(database_gateway=database_gateway)
        self.asset_interactor = AssetInteractor(database_gateway=database_gateway)
        self.hash_key_interactor = HashKeyInteractor()
        self.facade_screen_validator = FacadeScreenValidator()
        self.hook_preprocessing = HookVisitsPreprocessing()
~~~

- Uso de clases especializadas para extender la funcionalidad sin comprometer la funcionalidad base:

~~~python
class InterfaceScreenValidator(ABC):

    @abstractmethod
    def filter(self):
        pass

    @abstractmethod
    def get_validation_code(self) -> str:
        pass

    @abstractmethod
    def validate(self):
        pass

class AbstractScreenValidator(InterfaceScreenValidator):

    def __init__(self, dataframe, column, dataframe_errors):
        ...
        self.validation_code = self.get_validation_code()

    @abstractmethod
    def filter(self):
        pass

    @abstractmethod
    def get_validation_code(self) -> str:
        pass

    def validate(self):

        print('Validating column: ' + self.column + ' with screen ' + self.validation_code)
        
        fails = self.filter()
        fails = fails.select('row_temp_id', self.column)
        fails = fails.withColumnRenamed(self.column, 'value')
        fails = fails.withColumn('screen_code', lit(self.validation_code))
        fails = fails.withColumn('column_name', lit(self.column))
        fails = fails.withColumn('value', col('value').cast(StringType()))

        ...
    
class IsNotNullScreenValidator(AbstractScreenValidator):

    def filter(self):
        return self.dataframe.where(col(self.column).isNull()).select('row_temp_id', self.column)

    def get_validation_code(self):
        return 'is_not_null'
    
class IsEmailFormatScreenValidator(AbstractScreenValidator):

    def filter(self):
        return self.dataframe.where(~regexp_extract(col(self.column), r'^.+@.+\..+$', 0).cast('string').isNotNull())

    def get_validation_code(self):
        return 'is_email_format'

~~~

También se procuró separar los componentes tal que las clases de alto nivel no tuvieran **dependencias** hacia las clases de bajo nivel, lo cual en el futuro deberá facilitar el mantenimiento o la extensión de funcionalidad.

### Uso de Hooks

Los *hooks* resultaron muy utiles para atender situaciones muy específicas, además permiten extender la funcionalidad sin alterar las clases base:

~~~python
#INTERFACE FOR PREPROCESSIG STEPS IN THE RAW TO AUDIT PROCESS
class InterfaceHookRawToAuditPreprocessing(ABC):

    @abstractmethod
    def transform(self) -> DataFrame:
        pass

#HOOK FOR VISITS
class HookVisitsPreprocessing(InterfaceHookRawToAuditPreprocessing):

    def __init__(self):
        pass

    def transform(self, dataframe):

        columns = ['FechaOpen', 'FechaClick', 'Links', 'IPs', 'Navegadores', 'Plataformas']
        for column in columns:
            dataframe = dataframe.withColumn(column, regexp_replace(col(column), 'unknown', ''))
            dataframe = dataframe.withColumn(column, when(col(column) == '-', lit(None)).otherwise(col(column)))
            dataframe = dataframe.withColumn(column, when(col(column) == '', lit(None)).otherwise(col(column)))

        columns = ['Links', 'IPs', 'Navegadores', 'Plataformas']
        for column in columns:
            dataframe = dataframe.withColumn(column, convert_to_json_string_udf(col(column)))

        return dataframe
~~~

El *hook* anterior se utilizó para preprocesar la tabla de visitas en la capa de auditoría, se incorporó a la plantilla mediante su **fábrica abstracta**.

### Llaves Surrogadas

Para las primeras tablas se utilizaron **llaves incrementales**, sin embargo esto en Databricks es particularmente lento por qué se tienen que hacer muchas consultas por separado, posteriormente se utilizaron campos **hash** generados a partir de las columnas base, lo cual ya no requiere el uso de consultas para determinar llaves existentes o saber si se tiene que generar una nueva.

### Llaves Primarias

Para futuras versiones se tendría que mejorar la gestión de llaves primarias, en particular los registros con errores de este tipo fueron los únicos descartados por qué provocaban errores con los mecanismos **MERGE** de las tablas Delta. También se tendría que dar soporte a los casos donde haya llaves primarias de dos o más columnas.

### Tablas Históricas

Un detalle importante con la implementación actual es que se sobreescribe por completo la tabla de la capa histórica, lo cual no será factible en tablas grandes, habría que definir a qué tablas se aplica esta funcionalidad o encontrar una manera de no sobreescribir todo. Adicionalmente hicieron falta pruebas para otros escenarios, como la eliminación suave.

### Versionado de Tablas

Si bien en los metadatos existe una especificación para la versión, aún no hay soporte para esta funcionalidad, sin embargo podría ser bueno que mediante el uso de integración continua se puedan agregar nuevas versiones que en automático puedan manejar los cambios a una tabla.

### Campos con datos personales

Si bien en los metadatos también existe esta especificación para el *Email* del ejercicio, no se ejecutó ninguna acción al respecto, habría que plantear algún mecanismo de **encriptación** y **desencriptación** así como políticas de quién puede acceder a estos campos (parte de esta información podría ser integrada en los metadatos).

### Particionado

Al igual que en los dos puntos anteriores, no hay aún soporte para particiones especificadas en los metadatos. Por default se particionó considerando la columna *metadata_batch_id* para las capa *raw*.

### Llaves Foraneas y Validaciones entre Tablas

En esta versión no hay soporte para validaciones que impliquen múltiples tablas, se tendría que plantear cómo especificar las validaciones e implementarlas.

### Soporte para otros Formatos de Tabla

En esta implementación existe una dependencia total hacia los *dataframes* de Pyspark, si se quisiera tener soporte para manejo de otros tipos de *dataframe* como *pandas* o algún otro motor **big data**, un posible planteamiento es mediante el uso de *dataclass* abstractos que contengan una interfaz de *dataframe* y delegar a subclases concretas para cada motor la funcionalidad particular. Sin embargo este enfoque podría complicarse en casos donde existan muchos casos de uso o no todos los motores manejen la misma funcionalidad.

### Soporte para Tipos de Escritura

Considerar casos donde haya otros tipos de escritura o se necesiten otros mecanismos *merge* para las tablas Delta.

### Manejo de Errores

En la implementación actual solo se hace un etiquetado de errores, en futuras versiones este mecanismo se debe hacer más robusto, posiblemente se puedan incorporar reglas para casos específicos así como la actualización de los eventos de error en caso de que en el futuro llegue alguna corrección.




