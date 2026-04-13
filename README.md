Un README profesional debe ser el mapa técnico de tu proyecto. No debe limitarse a decir qué hace el código, sino por qué está diseñado de esa manera. Esto es lo que separa a un analista de un ingeniero de datos.

Aquí tienes la estructura optimizada para tu repositorio en GitHub:

Multi-Node Sales ETL Pipeline: Postgres to Azure Data Lake
Este proyecto implementa un pipeline de datos escalable y profesional que extrae información de ventas desde múltiples nodos regionales de PostgreSQL (desplegados en Docker), consolida los datos mediante lógica incremental y los persiste en un Data Lake en Azure Blob Storage utilizando el formato Apache Parquet.

1. Arquitectura del Proyecto
El sistema simula un entorno multinacional con tres bases de datos independientes (Argentina, Brasil y México).

Fuentes de Datos: 3 Instancias de PostgreSQL en contenedores Docker independientes.

Orquestación de Datos (ETL): Script en Python 3.13 con lógica de extracción incremental.

Almacenamiento (Cloud): Azure Blob Storage (Capa Bronze/Silver).

Consumo: Power BI Desktop conectado directamente al Data Lake.

2. Características Técnicas "Superadoras"
Extracción Incremental (High Watermark)
A diferencia de procesos básicos que realizan cargas completas (Full Load), este script implementa un High Watermark (HWM) basado en la columna ultima_actualizacion. El sistema guarda el estado de la última carga en un archivo JSON local, permitiendo extraer únicamente los registros nuevos en cada ejecución. Esto reduce drásticamente el consumo de red y recursos.

Formato Apache Parquet
Se optó por Parquet sobre CSV por tres razones críticas:

Tipado fuerte: Preserva los tipos de datos (dates, decimals) sin necesidad de casting manual en el destino.

Compresión columnar: Reduce el peso de los archivos hasta en un 80%.

Performance en Power BI: El motor de Power BI lee metadatos de Parquet de forma nativa, permitiendo consultas mucho más rápidas.

Particionamiento en el Data Lake
Los datos se cargan en Azure siguiendo una estructura de carpetas dinámica:
ventas/año=YYYY/mes=MM/ventas_delta_TIMESTAMP.parquet.
Esto permite un Partition Pruning eficiente al momento de consumir los datos desde herramientas de Big Data o Power BI.

3. Stack Tecnológico
Python 3.13: Lógica principal de transformación.

Pandas & SQLAlchemy: Procesamiento de datos y conectores SQL.

Docker & Docker Compose: Virtualización de bases de datos.

Azure Storage Blob SDK: Integración con la nube.

PyArrow: Motor de procesamiento para archivos Parquet.

4. Configuración del Entorno
Requisitos Previos
Docker Desktop.

Python 3.10+.

Cuenta de Azure Storage.

Instalación
Clonar el repositorio.

Levantar las bases de datos:

Bash
docker-compose up -d
Instalar dependencias:

Bash
pip install -r requirements.txt
Variables de Entorno (Seguridad)
Por razones de seguridad, las credenciales no están hardcodeadas. Debes configurar la siguiente variable en tu sistema:

PowerShell
$env:AZURE_STORAGE_CONNECTION_STRING = "tu_connection_string_aqui"
5. Ejecución del Pipeline
Generación de Datos (Mocking):

Bash
python scripts/Generar_Datos.py
Simula la inserción de 100k registros por país.

Proceso ETL:

Bash
python scripts/etl_ventas.py
Realiza la extracción incremental, genera el Parquet y lo sube a Azure.

6. Monitoreo y Logs
El sistema genera un archivo etl_ventas.log que registra:

Registros extraídos por nodo.

Errores de conexión.

Rutas de archivos cargados en el Cloud.

Desarrollado por: Lucas Luiselli
Rol: Data Scientist / BI / Data Engineer entusiasta.