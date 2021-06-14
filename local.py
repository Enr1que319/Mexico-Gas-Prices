# import pandas as pd
# import numpy as np

# station_data = pd.read_csv('Stations_data.csv')

# data = station_data[['Numero','Direccion','EntidadFederativaId','MunicipioId','Nombre','Apoyo 2']]
# data = data.drop_duplicates()

# data.to_csv('Stations_data.csv')

# data = station_data[['Numero','Direccion','EntidadFederativaId','MunicipioId','Nombre','Apoyo 2']]

# print(data.dtypes)

# data['Apoyo 2'] = data['Apoyo 2'].astype(np.int64)

# print(data.head())

# data.to_csv('Clean_rep.csv')

from requests import get
from xml.etree import ElementTree
import pandas as pd
from datetime import date, datetime
import reverse_geocoder as rg
import time
import unidecode

PROJECT = 'tidy-weaver-308700'
BUCKET = 'shell--test-cloud21'
FULL_FILE_PATH = 'gs://shell--test-cloud21/Files/Stations_data.csv'
TABLE_ID = '{}.Shell.Precios'.format(PROJECT)


# XML Path that contains CRE data
xml_prices = 'https://bit.ly/2JNcTha'
xml_geo = 'https://bit.ly/2V1Z3sm'

def my_function():
    # Getting the data from xml web path
    response_prices = get(xml_prices)
    response_geo = get(xml_geo)

    # Formatting the xml response to string
    prices_tree = ElementTree.fromstring(response_prices.content)
    geo_tree = ElementTree.fromstring(response_geo.content)

    # Creating the list where is going to store the data
    prices_data = []
    geo_data = []

    # Creating loop that form the dict that pandas is going to read to create a prices dataframe
    for child in prices_tree:
        for subchild in child:
            dict = {'id_lugar': int(child.attrib['place_id']),
            'Producto': subchild.attrib['type'],
            'Precio': float(subchild.text),
            'Fecha' : date.today().strftime('%Y-%m-%d')
            }
            prices_data.append(dict)

    # Creating loop that form the dict that pandas is going to read to create a geo dataframe
    for child in geo_tree:
        # places_obj = (float(child.find('location').find('y').text),float(child.find('location').find('x').text))
        # time.sleep(1)
        dict = {'id_lugar': float(child.attrib['place_id']),
            'Nombre': child.find('name').text,
            'Permiso': child.find('cre_id').text,
            'y' : float(child.find('location').find('y').text), #rg.search(places_obj)[0]['admin1']
            'x' : float(child.find('location').find('x').text)
            }
        geo_data.append(dict)
        
                
    # Creating dataframes
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

    station_data = pd.read_csv('Resources/Stations_data.csv')
    merged_df2 = pd.merge(how='left',left=merged_df, right=station_data, left_on='Permiso', right_on='Numero')
    merged_df2['Estado'] = merged_df2['EntidadFederativaId'].where(merged_df2['EntidadFederativaId'].notnull(), merged_df2['EstadoTemp'])
    merged_df2['Municipio'] = merged_df2['MunicipioId'].where(merged_df2['MunicipioId'].notnull(), merged_df2['MunicipioTemp'])
   
    prices = pd.read_csv('Resources/Permiso-Marca.csv')
    merged_df3 = pd.merge(how='left',left=merged_df2, right=prices, left_on='Permiso', right_on='Permit')

    final_df = merged_df3[['Permiso','Brand','Nombre_x','Direccion','Producto','Precio','Estado','Municipio','Fecha']]
    final_df = final_df.rename(columns={'Nombre_x':'Nombre'})

    final_df['Estado'] = final_df['Estado'].apply(unidecode.unidecode)

    final_df.loc[final_df.Estado == 'Mexico City', 'Estado'] = 'Ciudad de Mexico'
    final_df.loc[final_df.Estado == 'Veracruz de Ignacio de la Llave', 'Estado'] = 'Veracruz'
    final_df.loc[final_df.Estado == 'Michoacan de Ocampo', 'Estado'] = 'Michoacan'
    final_df.loc[final_df.Estado == 'Coahuila de Zaragoza', 'Estado'] = 'Coahuila'

    states = [
        'Aguascalientes','Baja California','Baja California Sur','Campeche','Chiapas','Chihuahua',
        'Ciudad de Mexico','Coahuila','Colima','Durango','Guanajuato','Guerrero','Hidalgo','Jalisco',
        'Mexico','Michoacan','Morelos','Nayarit','Nuevo Leon','Oaxaca','Puebla','Queretaro',
        'Quintana Roo','San Luis Potosi','Sinaloa','Sonora','Tabasco','Tamaulipas','Tlaxcala','Veracruz',
        'Yucatan','Zacatecas'
    ]

    final_df = final_df[final_df['Estado'].isin(states)]
    print(final_df.head(10))

if __name__ == "__main__":
    my_function()
# # station_data = pd.read_csv('Resources/Stations_data.csv')
# # merged_df = pd.merge(left=prices, right=station_data, left_on='id_lugar', right_on='Apoyo 2')
# # print(prices.head(10))
# # print(merged_df.count())
# # # merged_df = merged_df[['Numero','Direccion','Precio','EntidadFederativaId','MunicipioId','Nombre','Fecha']]

# # # print(merged_df.dtypes)
# # # print(merged_df.head())
# # # prices = pd.read_csv('Resources/Permiso-Marca.csv')
# # # merged_all = pd.merge(left=merged_df, right=prices, left_on='Numero', right_on='Permit')
# # # merged_all = merged_all.rename(columns={'Brand': 'Marca',})
# # # merged_all.loc[merged_all['Marca'].isin(['sale','fullprice']), 'Marca'] = 'Pemex'
# # # merged_all = merged_all[['Numero', 'Marca', 'Direccion','Precio','EntidadFederativaId','MunicipioId','Nombre','Fecha']]


# # # merged_all['Precio'] = merged_df['Precio'].astype('float')
# # # merged_all['Fecha'] = pd.to_datetime(merged_df['Fecha'])




