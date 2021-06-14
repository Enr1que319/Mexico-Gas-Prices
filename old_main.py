from requests import get
from xml.etree import ElementTree
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
import gcsfs

def download_prices(data, context):

    PROJECT = 'tidy-weaver-308700'
    BUCKET = 'shell--test-cloud21'
    FULL_FILE_PATH = 'gs://{}/Files/'.format(BUCKET)
    TABLE_ID = '{}.Shell.Precios'.format(PROJECT)

    # Blob for write in storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET)

    # XML Path that contains CRE data
    xml_path = 'https://bit.ly/2JNcTha'

    # Getting the data from xml web path
    response = get(xml_path)

    # Formatting the xml response to string
    tree = ElementTree.fromstring(response.content)

    # Creating the list where is going to store the data
    data = []

    # Creating loop that form the dict that pandas is going to read to create a dataframe
    for child in tree:
        for subchild in child:
            dict = {'id_lugar': int(child.attrib['place_id']),
            'Producto': subchild.attrib['type'],
            'Precio': str(subchild.text),
            'Fecha' : date.today().strftime('%Y-%m-%d')
            }
            data.append(dict)

    # Creating the dataframe
    prices = pd.DataFrame(data)

    # Opening Stations_data.csv to merge dataframes
    station_data = pd.read_csv(FULL_FILE_PATH + 'Stations_data.csv')

    # Aplying an inner join to the 2 dataframes using the place id and ordering columns
    merged_df = pd.merge(left=prices, right=station_data, left_on='id_lugar', right_on='Apoyo 2')
    merged_df = merged_df[['Numero','Direccion','Producto','Precio','EntidadFederativaId','MunicipioId','Nombre','Fecha']]

    # Opening brands file and using old variable
    prices = pd.read_csv(FULL_FILE_PATH + 'Permiso-Marca.csv')

    # Merging brands file with other merged file
    merged_all = pd.merge(left=merged_df, right=prices, left_on='Numero', right_on='Permit')

    # Renaming brand column
    merged_all = merged_all.rename(columns={'Brand': 'Marca',})

    # Reordering the columns
    merged_all = merged_all[['Numero', 'Marca', 'Direccion','Producto','Precio','EntidadFederativaId','MunicipioId','Nombre','Fecha']]

    # Converting columns to data types
    merged_all['Precio'] = merged_all['Precio'].astype('float')
    merged_all['Fecha'] = pd.to_datetime(merged_all['Fecha'])

    # Creating the output object
    output_data = pd.DataFrame(data=merged_all).to_csv(sep=",", index=False, quotechar='"', encoding="UTF-8")

    # Path where file is going to be created
    blob = bucket.blob('Reportes/Reporte_{}.csv'.format(date.today().strftime('%Y-%m-%d')))

    # Pushing the object file to the cloud
    blob.upload_from_string(data=output_data)

    # WRITTING DATA TO BIGQUERY

    # Creating the client to bigquery
    client = bigquery.Client()

    # Set the schema of the table
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Numero", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Marca", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Direccion", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Producto", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Precio", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("EntidadFederativaId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("MunicipioId", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Nombre", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Fecha", bigquery.enums.SqlTypeNames.DATE),
    ],
    write_disposition="WRITE_APPEND",
    )

    # Sending the job to write the data
    job = client.load_table_from_dataframe(
    merged_all, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()

    # Print the info of the job
    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), TABLE_ID
        )
    )