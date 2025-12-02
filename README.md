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


  
 
## 1.- PERFILAMIENTO DE DATOS

Se recibieron 5 archivos con datos de la aseguradora, lo primero que se hizo fue hacer un análisis de perfilamiento, a continuación se presenta un resumen de este análisis (se puede revisar el analisis completo en el cuaderno https://github.com/famenor/insurance_case/blob/main/appendix_a_profiling.ipynb )

### Certificados

Cada cerfificado está ligado directamente a un cliente, contiene la información personal de los clientes.

- Llave natural en campo id y un número de certificado único.
- Nombre, email, edad (entre 40 y 85 años), fecha de nacimiento (entre 1941 y 1985), género (Posibles valores M y F) y ciudad (47 ciudades españolas).
- No se encontraron valores nulos.

### Términos

 Contienen los periodos de inicio y fin asociados a un cértificado, podría tratarse de la validez en la que una membresia o un seguro fue válido.

 - Llave natural en campo id.
 - La relación Certificados-Términos es 1:n mediante el id del certificado.
 - No se encontraron valores nulos.
 - Las fechas de inicio y témino oscilan entre 2021 y 2025.

### Patologias / Catálogo de enfermedades CIE

Contiene una lista de patologías y sus códigos internacionales

 - Llave natural: id
 - No se encontraron valores nulos

Sin embargo se encontraron faltantes por lo que se decidió incorporar un catálogo completo en su lugar. 

### Consultas y Diagnósticos

Contiene la información de las consultas médicas a las que acudieron los clientes, en el archivo original había una columna con formato JSON con información detallada, se decidió preprocesar este archivo para poderlo perfilar, finalmente se dividió en dos tablas.

La primera tabla contiene toda la información uno a uno correspondiente a la consulta médica.

- Llave natural: id
- La relación Cértificados-Consultas es 1:n mediante el id del certificado.
- Las fechas de consultas oscilan entre 2021 y 2025.
- Especialidades médicas (4 posibles valores).
- Indicador de receta o de orden médica.
- Indicador de si habrá siguiente consulta.
- Campos textuales con objetivos del paciente y médico.
- Campo textual con descripción de la consulta por parte del médico.

La segunda tabla contiene una lista de diagnósticos derivados de la consulta médica.

- No tiene llave natural.
- La relación Consulta-Diagnósticos es 1:n mediante el id de la consulta.
- Los diagnósticos (de haberlos) están representados por un identificador que se enlaza con el catálogo de patologías.

### Reclamos

Contiene los reclamos hechos por los clientes asegurados para recibir algún pago derivado de accidentes o enfermedades cubiertas. 

- Llave natural: CLAIM ID
- La relación Cértificados-Reclamos es 1:n mediante el número del certificado (en este caso no se usa el id)
- Existen 32 valores para las provincias de España.
- Contiene un diagnóstico con clave CIE.
- Fechas de ocurrencia, primer gasto y pago, las cuales siguen un orden cronológico.
- Campos de pagos y gastos, con valores que oscilan entre -85000 y 1000000 (con formato utilizado en España).
- Campo de causa con posibles dos valores entre ACCIDENTE y ENFERMEDAD.
- Tipo de pago, con un solo posible valor: PAGO DIRECTO.
- La columna NumCertificado es llave foranea hacia certificados (numero_certificado)
- En general no habia valores nulos, pero se supondrá más adelante que algunos campos pueden tenerlos.

![](https://github.com/famenor/insurance_case/blob/main/pictures/diagrama_er.jpg)

## 2.- Implementación del Almacén de Datos

Con el perfilamiento de la sección anterior ahora se conoce la estructura mediante la cual los datos se relacionan, así como las reglas principales que se deben de cumplir.

Se proponen 3 niveles de madurez segun el procesamiento de los datos:

a) Nivel Bronce: La fuente ha pasado por un proceso de extracción en el cual se han formateado los datos y se han aplicado políticas de validación e integridad, ningun dato se descarta pero aquellos registros que no cumplen con las políticas son etiquetados para fines de auditoría.

b) Nivel Plata: La fuente ha sido filtrada al descartar las filas etiquetadas del nivel anterior, los datos también han sido modelados con estructura de dimensión y hechos.

c) Nivel Oro: Las fuentes han sido utilizadas para generar información de alto valor.

### Herramientas para procesamiento

- Debido a la complejidad que requiere el tratamiento prematuro de los datos, se utilizará Python para procesar los datos hasta que lleguen al nivel plata.
- Una vez que los datos estén en el nivel plata, se utilizará DBT hasta que lleguen al nivel oro.
- Los procesos serán ejecutados mediante Dagster.

### Herramientas para almacenamiento

- Los archivos de entrada y salida se ubicarán en este mismo repositorio.
- Las tablas con nivel bronce, plata y oro se guardarán en Duckdb (emulando a un almacén de datos).

### Politicas de validación e integridad

Se definieron 7 políticas para aplicar, en las cuales se detectarán valores nulos, valores fuera de intervalo, valores con formato incorrecto, valores no únicos, valores fuera de lista (incluye llave foranea) y valores con orden incorrecto:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_screen.jpg)

Cuando se aplique una política y se encuentre un error, este será almacenado en una tabla de hechos especial, con esta información será posible etiquetar a los registros que no cumplieron con la política y en futuras iteraciones robustecer el proceso de auditoría.

![](https://github.com/famenor/insurance_case/blob/main/pictures/fact_event_error_datail.jpg)

### Dimensión Fecha

Todos las fechas fueron sustituidas con un identificador que apunta a una vista particular, estas vistas se generaron a partir de una dimensión fecha principal:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_date.jpg)

### Dimensión Certificado o Cliente

Se aplicaron las siguientes políticas de validación:

~~~python
        #CHECK NULL VALUES
        columns = ['name', 'email', 'age', 'city', 'birth_date', 'certificate_number', 'gender']
        for column in columns:
            self.facade_screens.apply_screen_is_missing_value(column)

        #CHECK UNIQUE VALUES
        self.facade_screens.apply_screen_is_not_unique('certificate_id')
        self.facade_screens.apply_screen_is_not_unique('certificate_number')

        #CHECK NOT DIGIT STRING VALUES
        self.facade_screens.apply_screen_is_not_digit_string('certificate_number', 6)

        #CHECK NOT DATE FORMAT VALUES
        self.facade_screens.apply_screen_is_not_date_format('birth_date', '%Y-%m-%d')

        self.data['birth_date'] = pd.to_datetime(self.data['birth_date'], format='%Y-%m-%d')

        #CHECK OUT OF BOUNDS VALUES
        self.facade_screens.apply_screen_is_out_of_bounds_value('age', 0, 100)
        self.facade_screens.apply_screen_is_out_of_bounds_value('birth_date', pd.Timestamp('1925-01-01'), pd.Timestamp('2024-12-31'))

        #CHECK OUT OF LIST VALUES
        self.facade_screens.apply_screen_is_out_of_list_value('gender', ['M', 'F'])
~~~

y se generó la dimensión bronce:

![](https://github.com/famenor/insurance_case/blob/main/pictures/dim_certificate_bronze.jpg)

Posteriormente, para el nivel plata se enmascararon los datos sensibles, se reemplazaron las llaves naturales por subrogadas (propia y en fecha de nacimiento):