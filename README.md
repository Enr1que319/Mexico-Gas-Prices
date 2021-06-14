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
- Struct data in json objects to then convert it to pandas dataframe
- Transforms data with pandas and other libraries to insert geo coordinates and insert new columns
- Store transformed data to storage
- Send the final dataframe to Big Query 


Architecture
----

Here is the GCP architecture, this shows the resources that are used

[![](Images/arch.jpeg)]()     