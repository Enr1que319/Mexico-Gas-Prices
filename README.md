# Mexico-Gas-Prices

[![](Resources/Img/AA-FuelPrice-PR.jpg)]()  
----

In Mexico, the price of gasoline is constantly changing since its prices depend a lot on the prices of barrel, transfer and competitors. That is why keeping track of prices can save money when looking for the best price to fill your tank.


The task
----

The objective of this project is proces this fuel prices data obtained from [CRE](https://www.datos.gob.mx/busca/dataset/estaciones-de-servicio-gasolineras-y-precios-finales-de-gasolina-y-diesel). (Comision Regulatoria de Energía en México) in the cloud to storage a daily basis in Storage and in Big Query Data Warehouse

Tools that were used to accomplish this task:

- Python
    - Pandas
    - Requests
    - reverse_geocoder
    - unidecode
    - pyarrow
    - google-cloud-bigquery
    - google-cloud-storage
    - gcsfs
- Google Cloud Storage
- BigQuery
- Cloud Functions

[![](Resources/Img/1200px-Python.svg.png)]()   


Big Query Data
----

The schema of the data is as follows:

    [ 
      {
        "mode": "REQUIRED",
        "name": "Permiso",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Marca",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Nombre",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Direccion",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Producto",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Precio",
        "type": "FLOAT64"
      },
      {
        "mode": "REQUIRED",
        "name": "Estado",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Municipio",
        "type": "STRING"
      },
      {
        "mode": "REQUIRED",
        "name": "Fecha",
        "type": "DATE"
      },
      {
        "mode": "REQUIRED",
        "name": "y",
        "type": "FLOAT64"
      },
      {
        "mode": "REQUIRED",
        "name": "x",
        "type": "FLOAT64"
      }
    ]

Process
----

The procedure that was taken to process the information is as follows:

- Get xml element tree from CRE web page
- S
- Use Google Workflow templates to automate the Spark ETL batch processing job
- Use Apache Airflow to create DAGs and automate the batch processing job

The folder 'Manual Job' have all the scripts step by step in individual batch files that run all process manually.
The folder 'Workflow' automates all the process using an unique batch file that runs all the commands excluding the creation of the tables in Big Query. To run it you should trigger the file manually.
The folder 'Airflow' brings a python file that automates all the proccess, the advange of this script is that is triggers alone everyday at 2:30 pm.

Architecture
----

Here is the GCP architecture, this shows the resources that are used

[![](Images/arch.jpeg)]()     