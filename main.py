from requests import get
from xml.etree import ElementTree
import pandas as pd
from datetime import date
from google.cloud import storage, bigquery
import gcsfs
import reverse_geocoder as rg
import unidecode

def download_prices(data, context):

    PROJECT = 'tidy-weaver-308700'
    BUCKET = 'shell--test-cloud21'
    FULL_FILE_PATH = 'gs://{}/Files/'.format(BUCKET)
    TABLE_ID = '{}.Shell.Precios'.format(PROJECT)

    # Blob for write in storage
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(BUCKET)

    # XML Paths that contains CRE data
    xml_prices = 'https://bit.ly/2JNcTha'
    xml_geo = 'https://bit.ly/2V1Z3sm'

    # Getting the data from xml web paths
    response_prices = get(xml_prices)
    response_geo = get(xml_geo)

    # Formatting the xml responses to string
    prices_tree = ElementTree.fromstring(response_prices.content)
    geo_tree = ElementTree.fromstring(response_geo.content)

    # Creating the lists where is going to store the data
    prices_data = []
    geo_data = []

    # Creating loops that form the dict that pandas is going to read to create dataframes
    for child in prices_tree:
        for subchild in child:
            dict = {'id_lugar': int(child.attrib['place_id']),
            'Producto': subchild.attrib['type'],
            'Precio': str(subchild.text),
            'Fecha' : date.today().strftime('%Y-%m-%d')
            }
            prices_data.append(dict)

    for child in geo_tree:
        dict = {'id_lugar': float(child.attrib['place_id']),
            'Nombre': child.find('name').text,
            'Permiso': child.find('cre_id').text,
            'y' : float(child.find('location').find('y').text),
            'x' : float(child.find('location').find('x').text)
            }
        geo_data.append(dict)

    # Creating the dataframes
    prices = pd.DataFrame(prices_data)
    location = pd.DataFrame(geo_data)

    # Merging dataframes
    merged_df = pd.merge(how='left',left=prices, right=location, left_on='id_lugar', right_on='id_lugar')

    #Creating object with coordinates
    coords = tuple(zip(merged_df['y'], merged_df['x']))

    #Getting coordinates data
    results_rg = rg.search(coords)

    #Getting states and municipalities columns
    state = [obj.get('admin1') for obj in results_rg]
    municipality = []

    for obj in results_rg:
        mun = obj.get('admin2')
        if mun == '':
            mun = obj.get('name')
        municipality.append(mun)
        
    merged_df['EstadoTemp'] = state
    merged_df['MunicipioTemp'] = municipality

    # Opening Stations_data.csv to merge dataframes
    prices = pd.read_csv(FULL_FILE_PATH + 'Stations_data.csv')

    # Aplying LEFT join to the 2 dataframes using the place id and ordering columns
    location = pd.merge(how='left',left=merged_df, right=prices, left_on='Permiso', right_on='Numero')
    location['Estado'] = location['EntidadFederativaId'].where(location['EntidadFederativaId'].notnull(), location['EstadoTemp'])
    location['Municipio'] = location['MunicipioId'].where(location['MunicipioId'].notnull(), location['MunicipioTemp'])
    
    # Opening brands file and using old variable
    merged_df = pd.read_csv(FULL_FILE_PATH + 'Permiso-Marca.csv')
    prices = pd.merge(how='left',left=location, right=merged_df, left_on='Permiso', right_on='Permit')

    # Creating final dataframe
    location = prices[['Permiso','Brand','Nombre_x','Direccion','Producto','Precio','Estado','Municipio','Fecha','y','x']]
    location = location.rename(columns={'Nombre_x':'Nombre','Brand':'Marca'})

    # Converting columns to data types
    location['Precio'] = location['Precio'].astype('float')
    location['Fecha'] = pd.to_datetime(location['Fecha'])
    location['y'] = location['y'].astype('float')
    location['x'] = location['x'].astype('float')

    # Removing accents
    location['Estado'] = location['Estado'].apply(unidecode.unidecode)

    # Removing duplicate names of states
    location.loc[location.Estado == 'Mexico City', 'Estado'] = 'Ciudad de Mexico'
    location.loc[location.Estado == 'Veracruz de Ignacio de la Llave', 'Estado'] = 'Veracruz'
    location.loc[location.Estado == 'Michoacan de Ocampo', 'Estado'] = 'Michoacan'
    location.loc[location.Estado == 'Coahuila de Zaragoza', 'Estado'] = 'Coahuila'

    # List of mexican states
    states = [
        'Aguascalientes','Baja California','Baja California Sur','Campeche','Chiapas','Chihuahua',
        'Ciudad de Mexico','Coahuila','Colima','Durango','Guanajuato','Guerrero','Hidalgo','Jalisco',
        'Mexico','Michoacan','Morelos','Nayarit','Nuevo Leon','Oaxaca','Puebla','Queretaro',
        'Quintana Roo','San Luis Potosi','Sinaloa','Sonora','Tabasco','Tamaulipas','Tlaxcala','Veracruz',
        'Yucatan','Zacatecas'
    ]

    # Removing wrong states (bad coordenates)
    location = location[location['Estado'].isin(states)]

    # Creating the output object
    prices = pd.DataFrame(data=location).to_csv(sep=",", index=False, quotechar='"', encoding="UTF-8")

    # Path where file is going to be created
    blob = bucket.blob('Reportes/Reporte_{}.csv'.format(date.today().strftime('%Y-%m-%d')))

    # Pushing the object file to the cloud
    blob.upload_from_string(data=prices)

    # WRITTING DATA TO BIGQUERY

    # Creating the client to bigquery
    client = bigquery.Client()

    # Set the schema of the table
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("Permiso", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Marca", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Nombre", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Direccion", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Producto", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Precio", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("Estado", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Municipio", bigquery.enums.SqlTypeNames.STRING),
        bigquery.SchemaField("Fecha", bigquery.enums.SqlTypeNames.DATE),
        bigquery.SchemaField("y", bigquery.enums.SqlTypeNames.FLOAT64),
        bigquery.SchemaField("x", bigquery.enums.SqlTypeNames.FLOAT64)
    ],
    write_disposition="WRITE_APPEND",
    )

    # Sending the job to write the data
    job = client.load_table_from_dataframe(
    location, TABLE_ID, job_config=job_config
    )  # Make an API request.
    job.result()

    # Print the info of the job
    table = client.get_table(TABLE_ID)  # Make an API request.
    print(
    "Loaded {} rows and {} columns to {}".format(
        table.num_rows, len(table.schema), TABLE_ID
        )
    )