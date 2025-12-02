# FLUJO DE DATOS PARA CASO DE VISITA DE USUARIOS

## CONTENIDO

  1 - Perfilamiento de Datos
  
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
| Gold | Analytics | Almacenar tablas que necesiten transformaciones para su consumo final. |

Los se organizaron con una estructura de tres niveles, siguiendo la convención CATALOGO.ESQUEMA.TABLA

Un **catálogo** se compone por el dominio y el ambiente de trabajo, se crearon tres catálogos:

![](https://github.com/famenor/users_case/blob/main/pictures/01_catalogos.jpg)

El primer dominio es para el **gobierno de datos** y el segundo dominio es general para todas las tablas a procesar, para el segundo dominio hay dos ambienes: DEV y PROD, en este ejercicio solo utilizaremos el ambiente DEV.

Un **esquema** se compone por el nivel (medalla) y subdominio, excepto en el catálogo de gobierno de datos, para este catálogo se crearon los esquemas de métricas y metadatos:

![](https://github.com/famenor/users_case/blob/main/pictures/02_esquemas.jpg)

Una **tabla** se compone por la capa y el nombre plano de la tabla, excepto en el catálogo de gobierno de datos y las tablas oro que de momento no tienen convención establecida:

![](https://github.com/famenor/users_case/blob/main/pictures/03_tablas.jpg)

También se generó una tabla de gobierno de datos que almanena las principales especificaciones de las columnas que serán ingestadas:

![](https://github.com/famenor/users_case/blob/main/pictures/04_tablas_detalle.jpg)

### Métricas de Ingesta de Datos

Para la ingesta de datos se crearon dos tablas, la primera almacena estadísticas de ingesta de datos:

![](https://github.com/famenor/users_case/blob/main/pictures/05_ingesta.jpg)

mientras que la segunda tabla almacena los eventos de error (bitácora):

![](https://github.com/famenor/users_case/blob/main/pictures/06_eventos_error.jpg)

ACLARACIÓN: Con excepción del error de llave primera, los demás errores fueron introducidos deliberadamente para fines del ejercicio.

La creación de las tablas de gobierno de datos (metadatos y métricas) se implementó en el cuaderno appendix_a_init_governance, a continuación se muestra una captura en la sección de catálogos de Databricks donde se muestran los catálogos, esquemas y tablas de gobierno creados:

![](https://github.com/famenor/users_case/blob/main/pictures/07_tablas_gobierno.jpg)



  

~~~python
        #CHECK NULL VALUES


~~~
