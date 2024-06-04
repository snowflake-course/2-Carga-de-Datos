# 2-Carga-de-Datos

1. [Adquisición e ingesta de datos en Snowflake (stages)](#schema1)
2. [Carga de datos desde la interfaz fichero local](#schema2)
3. [Cómo crear un stage](#schema3)
4. [Carga de datos utilizando el comando COPY](#schema4)
5. [Transformación de datos](#schema5)
6. [Manejo de errores - Opción COPY: "ON_ERROR"](#schema6)
7. [Reutilizar formato ficheros con objeto file_format](#schema7)
8. [Modo de validación y tratamiento de errores](#schema8)

<hr>

<a name="schema1"></a>

## 1. Adquisición e ingesta de datos en Snowflake (stages)

![](./img/intro.png)
![](./img/intro_2.png)


<hr>

<a name="schema2"></a>

## 2. Carga de datos desde la interfaz fichero local
![](./img/intro_3.png)

<hr>

<a name="schema3"></a>


## 3. Cómo crear un stage

1. Crear Database para gestión de stages, fileformats, etc.


```sql
CREATE OR REPLACE DATABASE MANAGE_DB;

CREATE OR REPLACE SCHEMA external_stages;
```

2. Crear external stage
```sql
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3'
    credentials=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');
```
3. Descripción del external stage
```sql
DESC STAGE MANAGE_DB.external_stages.aws_stage; 
```    
    
4. Modificar external stage   
```sql
ALTER STAGE aws_stage
    SET credentials=(aws_key_id='XYZ_DUMMY_ID' aws_secret_key='987xyz');
```    
    
5. Eliminar credenciales puesto que es un fichero público    
```sql
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    url='s3://bucketsnowflakes3';
```
6. Listar los ficheros en el stage
```sql
LIST @aws_stage;
```

### **CARGA DE DATOS UTILIZANDO EL COMANDO COPY**

7. Crear tabla de datos ORDERS
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));
```
8. Uso de COPY con la sentencia de contexto completo
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails.csv');
```

9. Uso de COPY utilizando un patrón para el nombre del fichero (carga = 0 puesto que detecta que ya se ha cargado el fichero coincidente previamente)
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
```
### **TRANSFORMACIÓN DE DATOS**
- Ejemplo 1 seleccionar columnas - Tabla
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM (select s.$1, s.$2 from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;
```   
- Ejemplo 2 Uso de función SQL para columna nueva calculada - Tabla    
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
  
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM (select 
            s.$1,
            s.$2, 
            s.$3,
            CASE WHEN CAST(s.$3 as int) < 0 THEN 'not profitable' ELSE 'profitable' END 
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
```

- Ejemplo 3 Seleccionar Substring de una columna - Tabla
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    CATEGORY_SUBSTRING VARCHAR(5)
  
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM (select 
            s.$1,
            s.$2, 
            s.$3,
            substring(s.$5,1,5) 
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
```

- Ejemplo 4 Utilizar un conjunto de columnas y el resto vacías
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
  
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (ORDER_ID,PROFIT)
    FROM (select 
            s.$1,
            s.$3
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');

SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;
```

- Ejemplo 5 Añadir columna con autoincremento de ID
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID number autoincrement start 1 increment 1,
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
  
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (PROFIT,AMOUNT)
    FROM (select 
            s.$2,
            s.$3
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX WHERE ORDER_ID > 15;


    
DROP TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
```


<hr>

<a name="schema4"></a>


## 4. Carga de datos utilizando el comando COPY

1. Crear tabla
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));
    
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS;
```   

2. Primer comando COPY
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1);
```

3. Listar ficheros del stage
```sql
LIST @MANAGE_DB.external_stages.aws_stage;    
```

4. Comando COPY especificando el fichero
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails.csv');
```

5. Comando COPY a partir de un patrón
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*';
```
<hr>

<a name="schema5"></a>


## 5. Transformación de datos

- Ejemplo 1 seleccionar columnas - Tabla
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM (select s.$1, s.$2 from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;
``` 
- Ejemplo 2 Uso de función SQL para columna nueva calculada - Tabla    
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    PROFITABLE_FLAG VARCHAR(30)
  
    )

COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM (select 
            s.$1,
            s.$2, 
            s.$3,
            CASE WHEN CAST(s.$3 as int) < 0 THEN 'not profitable' ELSE 'profitable' END 
          from @MANAGE_DB.external_stages.aws_stage s)
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files=('OrderDetails.csv');


SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
```

<hr>

<a name="schema6"></a>


## 6. Manejo de errores - Opción COPY: "ON_ERROR"

1. Crear nuevo stage
```sql
 CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage_errorex
    url='s3://bucketsnowflakes4'
```
2. Listar los ficheros del stage
```sql
 LIST @MANAGE_DB.external_stages.aws_stage_errorex;
``` 
 
3. Crear tabla ejemplo
```sql
 CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));
```
4. Mostrar mensaje de error
```sql
 COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv');
```

5. Verificar que la tabla está vacía 
```sql
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX   
```
6. Manejo de errores usando la opción ON_ERROR = CONTINUE
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'CONTINUE';
```
7. Verificar resultados y truncar (limpiar) la tabla
```sql
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
SELECT COUNT(*) FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX

TRUNCATE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;
```
8. Manejo de errores usando la opción ON_ERROR = ABORT_STATEMENT (opción por defecto) y los 2 ficheros
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'ABORT_STATEMENT';
```

9. Verificar resultados y truncar (limpiar) la tabla
```sql
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
SELECT COUNT(*) FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX

TRUNCATE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;
```
10. Manejo de errores usando la opción ON_ERROR = SKIP_FILE y los 2 ficheros
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE';
```    
    
11. Verificar resultados y truncar (limpiar) la tabla
```sql
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
SELECT COUNT(*) FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX

TRUNCATE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;    
```

12. Manejo de errores usando la opción ON_ERROR = SKIP_FILE_<number> y los 2 ficheros
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE_2';    
```    
    
13. Verificar resultados y truncar (limpiar) la tabla 
```sql
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
SELECT COUNT(*) FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX

TRUNCATE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;    
```
    
14. Manejo de errores usando la opción ON_ERROR = SKIP_FILE_<number> porcentual y los 2 ficheros
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv','OrderDetails_error2.csv')
    ON_ERROR = 'SKIP_FILE_0.5%'; 
```  
  
15. Verificar resultados y truncar (limpiar) la tabla 
```sql
SELECT * FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
SELECT COUNT(*) FROM PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX

TRUNCATE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX;  
```

<hr>

<a name="schema7"></a>

## 7. Reutilizar formato ficheros con objeto file_format


- **MÉTODO 1:** 
1. Especificar el formato del fichero dentro del comando COPY
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format = (type = csv field_delimiter=',' skip_header=1)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 
```

2. Vaciamos la tabla
```sql
TRUNCATE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
```
3. Crear tabla
```sql
CREATE OR REPLACE TABLE PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));    
```    
3. Creamos el esquema para organización
```sql
CREATE OR REPLACE SCHEMA MANAGE_DB.file_formats;
```
4. Creamos el objeto de tipo file_format (por defecto es CSV)
```sql
CREATE OR REPLACE file format MANAGE_DB.file_formats.mi_formato;
```
5. Ver propiedades del objeto file format
```sql
DESC file format MANAGE_DB.file_formats.mi_formato;
```

**MÉTODO 2:**
1. Usamos el objeto file format creado dentro del comando COPY       
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.mi_formato)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 
```

2. Modificar el objeto file format (el tipo no se puede modificar una vez creado, habría que recrearlo)
```sql
ALTER file format MANAGE_DB.file_formats.mi_formato
    SET SKIP_HEADER = 1;
```
3. Si queremos definir objeto de formato con otros tipo de datos (por ejemplo JSON)  
```sql
CREATE OR REPLACE file format MANAGE_DB.file_formats.mi_formato_fichero_2
    TYPE=JSON,
    TIME_FORMAT=AUTO;    
```    
4. Ver propiedades   
```sql
DESC file format MANAGE_DB.file_formats.mi_formato_fichero_2;   
```
  
5. Usar objeto de formato erróneo     
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM @MANAGE_DB.external_stages.aws_stage_errorex
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.mi_formato_fichero_2)
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 

```
6. Limpiamos la tabla
```sql
TRUNCATE table PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
```

7. Sobreescribir alguna propiedad del objeto file format      
```sql
COPY INTO PRIMERABBDD.PRIMERESQUEMA.ORDERS_EX
    FROM  @MANAGE_DB.external_stages.aws_stage_errorex
    file_format = (FORMAT_NAME= MANAGE_DB.file_formats.mi_formato field_delimiter = ',' skip_header=1 )
    files = ('OrderDetails_error.csv')
    ON_ERROR = 'SKIP_FILE_3'; 
```

<hr>

<a name="schema8"></a>


## 8. Modo de validación y tratamiento de errores

### **MODO VALIDACIÓN**
1. Crear BBDD y tabla
```sql
CREATE OR REPLACE DATABASE COPY_DB;


CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));
```
2. Preparar stage
```sql
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3://snowflakebucket-copyoption/size/';
  
LIST @COPY_DB.PUBLIC.aws_stage_copy;
```  
    
3. Cargar datos con COPY con VALIDATION_MODE = RETURN_ERRORS
```sql
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS
```    
4. Cargar datos con COPY con VALIDATION_MODE = VALIDATION_MODE = RETURN_5_ROWS (retornar 5 filas)   
```sql
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
   VALIDATION_MODE = RETURN_5_ROWS 
```
**Usar ficheros con errores**
```sql
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    url='s3://snowflakebucket-copyoption/returnfailed/';

LIST @COPY_DB.PUBLIC.aws_stage_copy;    



COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS



COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_1_rows
```    

### **Trabajando con los resultados de error**

1) Guardar los registros erróneos después de VALIDATION_MODE
```sql
CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30));


COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    file_format= (type = csv field_delimiter=',' skip_header=1)
    pattern='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;
```

-  Almacenar registros erróneos en una tabla
```sql
CREATE OR REPLACE TABLE rechazados AS 
select rejected_record from table(result_scan(last_query_id()));

SELECT * FROM rechazados;
```


2) Transformar registros erróneos
```sql
CREATE OR REPLACE TABLE valores_erroneos as
SELECT 
SPLIT_PART(rejected_record,',',1) as ORDER_ID, 
SPLIT_PART(rejected_record,',',2) as AMOUNT, 
SPLIT_PART(rejected_record,',',3) as PROFIT, 
SPLIT_PART(rejected_record,',',4) as QUATNTITY, 
SPLIT_PART(rejected_record,',',5) as CATEGORY, 
SPLIT_PART(rejected_record,',',6) as SUBCATEGORY
FROM rechazados; 


SELECT * FROM valores_erroneos;
```