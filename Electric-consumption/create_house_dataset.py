import csv, requests, time, os, datetime
from datetime import timedelta, tzinfo
from datetime import datetime, timezone
import datetime as dt
import pandas as pd
import numpy as np
from pandas import Int64Dtype, read_csv
import xml.etree.ElementTree as ET

GMLCOV_POSITIONS = ".//{http://www.opengis.net/gmlcov/1.0}positions"
GML_DOUBLE_OR_NIL_REASON_TUPLE_LIST = ".//{http://www.opengis.net/gml/3.2}doubleOrNilReasonTupleList"
GML_ID = "{http://www.opengis.net/gml/3.2}id"
GML_NAME = ".//{http://www.opengis.net/gml/3.2}name"
GML_POINT = ".//{http://www.opengis.net/gml/3.2}Point"
GML_POS = ".//{http://www.opengis.net/gml/3.2}pos"
TARGET_LOCATIONCOLLECTION = ".//{http://xml.fmi.fi/namespace/om/atmosphericfeatures/1.1}LocationCollection"
FMISID_ATTRIBUTE = "http://xml.fmi.fi/namespace/stationcode/fmisid"
NAME_ATTRIBUTE = "http://xml.fmi.fi/namespace/locationcode/name"
GEOID_ATTRIBUTE = "http://xml.fmi.fi/namespace/locationcode/geoid"
WMO_ATTRIBUTE = "http://xml.fmi.fi/namespace/locationcode/wmo"
REGION_ATTRIBUTE = "http://xml.fmi.fi/namespace/location/region"

fmi_URL = "http://opendata.fmi.fi/wfs?" #Palvelun osoite
get_feature_BASE = "service=WFS&version=2.0.0&request=GetFeature&storedquery_id=" 

outputfilename = 'housedata.csv' #Datasetin nimi
path = 'Electricity_consumption' #Hakemiston nimi jonka alta data löytyy, yksittäinen data tulee olla nimetty YYYY-M. Havaintojen vuosi luetaan tiedostojen nimestä.

#Löytää toisen esiintymän merkkijonosta
def find_2nd(string, substring):
       return string.find(substring, string.find(substring) + 1)
   
def datetime_to_ISO(query):
    """Muuttaa datetimen ISO-Z (ISO-8601) muotoon"""
    return query.strftime('%Y-%m-%dT%H:%M:%SZ') 

def read_house_data():

    tempfilename = 'tempfile.csv'
    data = []
    missing = 0

    for filename in os.listdir(path):
        if filename.endswith('.csv'):

            with open(f'{path}/{filename}',encoding= 'ISO-8859-1') as csvfile:
                
                #Lue csvdata listaan
                rawdata = list(csv.reader(csvfile,delimiter=";"))
                #Poistetaan kaikki rivit joissa tekstiä
                rawdata = [row for row in rawdata if not any(letter.isalpha() for letter in str(row))]
                #Muodosta vuosiluku tiedostonimestä, sillä sitä ei ole tiedoston datassa
                year = filename[0:4]

                #Luetaan rivi kerrallaan kokoomalistaan
                for row in rawdata:
                    #print(row)
                    #Splittaa arvot listaan ja muuta tyhjät muuttujat NaN-arvoiksi   
                    datarow = row[0].split(';')
                    #Lisätään tiedostonimestä luettu vuosi mukaan keskelle stringiä
                    year_pos = find_2nd(datarow[0],'.') + 1
                    if (year_pos != 0): datarow[0] = f'{datarow[0][0:year_pos]}{year}{datarow[0][year_pos:]}'
                    
                    #Splittaa arvot listaan ja muuta tyhjät muuttujat NaN-arvoiksi
                    datarow = [x if x != '' else np.NaN for x in datarow]  

                    #Tarkastetaan jos dataa puuttuu, oletettavasti se on energiankulutus               
                    if (len(row) == 1): 
                        datarow.append(pd.NA)
                        missing += 1
                    else:
                        datarow.append(row[1])

                    data.append(datarow)
            
    #Tehdään dataframe luetusta datasta
    datatypes = { 'MTime': str, 'Consumption' : 'Int64'}
    df = pd.DataFrame(data, columns=['MTime','Consumption'])
    df = df.astype(datatypes,errors='ignore')
    df['MTime'] = pd.to_datetime(df['MTime'],format='%d.%m.%Y %H:%M:%S')
    
    #Kirjoitetaan se ensin temppitiedostoon tarkistusta varten
    try:
        df.to_csv(tempfilename,index=False)
        print(f'File {tempfilename} written, {missing} rows missing data. Total rows: {df.shape[0]}.')
        print(f'Temp file written.')
    except:
        print("Problemo writing the temp-filio.")
        
    #Sen jälkeen muutetaan sen aikaindeksi UTC-aikaan, sillä
    #ilmatieteen laitoksen aika on UTC-ajassa, samalla poistetaan kesä- ja talviaika
    try:
        df = pd.read_csv(tempfilename,index_col=False)
        print('Reading temp file and converting to UTC-time')
        df['MTime'] = pd.to_datetime(df['MTime'])
        df.set_index('MTime',drop=True,inplace=True)
        df.index = df.index.tz_localize('Europe/Helsinki',ambiguous='infer').tz_convert('UTC')
        df = df.sort_index(axis=0)
        df.to_csv(outputfilename)
        print(f'Success. File {outputfilename} written successfully.')
    except:
        print('Problemo wraiting the datafilio. ')
    
    return df

def hourly_weather_mpc(start_time : datetime.date, end_time : datetime.date):
    """Hakee tuntikohtaiset säähavainnot, multipointcoverage-muodossa"""
    
    start = datetime_to_ISO(start_time)
    end = datetime_to_ISO(end_time)
    print(f'Now processing weather data: {start}-{end}')
    fmisid = 101339 #Jyväskylän lentoasema, toiselta asemalta ei löydy kaikkien päivien tietoa

    hourly_weather_QUERY_MPC="fmi::observations::weather::hourly::multipointcoverage"
        
    df = pd.DataFrame(columns= ['MTime','Latitude','Longitude','Place','AirTemperature(degC)','HighestTemperature(degC)','LowestTemperature(degC)','RelativeHumidity(%)','WindSpeed(m/s)','MaximumWindSpeed(m/s)','MinimumWindSpeed(m/s)','WindDirection(deg)','PrecipitationAmount(mm)','MaximumPrecipitationIntensity(mm/h)','AirPressure(hPa)','PresentWeather(rank)'])

    data = requests.get(f"{fmi_URL}{get_feature_BASE}{hourly_weather_QUERY_MPC}&starttime={start}&endtime={end}&fmisid={fmisid}")
    
    #Luetaan paikka-ja aikakoordinaatit datasta
    root = ET.fromstring(data.text)
    
    position_time_data = []
    measurement_data = []
    coordinates = {}
    
    #Käydään läpi XML-dataa ja luetaan arvot
    for points in root.findall(GML_POINT):
        for name in points.findall(GML_NAME):
            place = name.text
        for position in points.findall(GML_POS):
            lat, lon = position.text.split()
        coordinates[(float(lat),float(lon))] = place
        
    fmisids, names, geoids, regions = [],[],[],[]
    for target in root.findall(TARGET_LOCATIONCOLLECTION):
        for member in target:
            for location in member:
                for identifier in location:
                    if 'http://xml.fmi.fi/namespace/stationcode/fmisid' in list(identifier.attrib.values())[0]: fmisids.append(identifier.text)
                    if 'http://xml.fmi.fi/namespace/locationcode/name' in list(identifier.attrib.values())[0]: names.append(identifier.text)
                    if 'http://xml.fmi.fi/namespace/locationcode/geoid' in list(identifier.attrib.values())[0]: geoids.append(identifier.text)
                    if 'http://xml.fmi.fi/namespace/location/region' in list(identifier.attrib.values())[0]: regions.append(identifier.text)
                    
    if len(fmisids) != len(names) != len(geoids) != len(regions):
        print("Missing additional data!")
    
    #Tallennetaan positiot ja aika listaan
    for positions in root.findall(GMLCOV_POSITIONS):
        position_time_data.append(positions.text.split())
    
    #Luetaan mittaustulokset listaan
    for measures in root.findall(GML_DOUBLE_OR_NIL_REASON_TUPLE_LIST):
        measurement_data.append(measures.text.split())    

    #Jos dataa puuttuu,
    if len(measurement_data[0]) // 12 != len(position_time_data[0]) // 3:
        raise Exception("Wrong number of measurements!")

    #Siirretään data listoista dataframeen
    while len(position_time_data[0]) > 0:
        current_lat = float(position_time_data[0].pop(0))
        current_lon = float(position_time_data[0].pop(0))
        #Muutetaan aika kokonaislukumuotoon, jotta Epoch-ajasta saadaan suoraan timestamp
        current_time = int(position_time_data[0].pop(0))
        current_TA_PT1H_AVG = float(measurement_data[0].pop(0))
        current_TA_PT1H_MAX = float(measurement_data[0].pop(0))
        current_TA_PT1H_MIN = float(measurement_data[0].pop(0))
        current_RH_PT1H_AVG = float(measurement_data[0].pop(0))
        current_WS_PT1H_AVG = float(measurement_data[0].pop(0))
        current_WS_PT1H_MAX = float(measurement_data[0].pop(0))
        current_WS_PT1H_MIN = float(measurement_data[0].pop(0))
        current_WD_PT1H_AVG = float(measurement_data[0].pop(0))    
        current_PRA_PT1H_ACC = float(measurement_data[0].pop(0))
        current_PRI_PT1H_MAX = float(measurement_data[0].pop(0))
        current_PA_PT1H_AVG = float(measurement_data[0].pop(0))     
        current_WAWA_PT1H_RANK = float(measurement_data[0].pop(0))

        df = df.append({'MTime' : pd.Timestamp(current_time,unit='s'), 'Latitude' : current_lat, 'Longitude' : current_lon, 'Place':place, 'AirTemperature(degC)' : current_TA_PT1H_AVG, 'HighestTemperature(degC)' : current_TA_PT1H_MAX, 'LowestTemperature(degC)' : current_TA_PT1H_MIN, 'RelativeHumidity(%)' : current_RH_PT1H_AVG, 'WindSpeed(m/s)' : current_WS_PT1H_AVG, 'MaximumWindSpeed(m/s)' : current_WS_PT1H_MAX, 'MinimumWindSpeed(m/s)' : current_WS_PT1H_MIN, 'WindDirection(deg)' : current_WD_PT1H_AVG, 'PrecipitationAmount(mm)' : current_PRA_PT1H_ACC, 'MaximumPrecipitationIntensity(mm/h)' : current_PRI_PT1H_MAX, 'AirPressure(hPa)' : current_PA_PT1H_AVG,'PresentWeather(rank)' : current_WAWA_PT1H_RANK}, ignore_index=True)
    
    return df


def create_dataframe():
    
    #Luodaan dataframe talon datalle
    df_house = pd.read_csv(outputfilename)
    df_house.MTime = df_house.MTime.map(lambda x: x.removesuffix("+00:00"))
    start_date = datetime.strptime(df_house.iloc[0]['MTime'],'%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(df_house.iloc[-1]['MTime'],'%Y-%m-%d %H:%M:%S')
    df_house['MTime'] = df_house['MTime'].astype('datetime64')

    delta = timedelta(days=1)
    df_weather = pd.DataFrame() #Oma dataframe weather_weather datalle

    #Jos ensimmäinen päivä on vajaa, otetaan osatunnit ja käydään puutteellisen päivän loppuun
    start_query = start_date.replace(start_date.year,start_date.month,start_date.day,start_date.hour,start_date.minute,start_date.second)
    end_query = start_date.replace(start_date.year,start_date.month,start_date.day,23,59,59)
    df_weather = pd.concat([df_weather,hourly_weather_mpc(start_query,end_query)],ignore_index=True)

    #Sen jälkeen täysiä päiviä
    start_date = start_date.replace(start_date.year,start_date.month,start_date.day,0,0,0)

    #Luetaan dataa kunnes kaikki päivät on käyty läpi
    while start_date <= end_date:
            start_date += delta
            start_query = start_date.replace(start_date.year,start_date.month,start_date.day,0,0,0)
            end_query = start_date.replace(start_date.year,start_date.month,start_date.day,23,59,59)
            df_weather = pd.concat([df_weather,hourly_weather_mpc(start_query,end_query)],ignore_index=True)
    
    #Kirjoitetaan säästä oma csv
    df_weather.to_csv('weatherdata.csv')   
    #Yhdistetään se aikaindeksin pohjalta kulutusdatasetin kanssa     
    df = pd.merge(df_house,df_weather,on=['MTime'],how='left')
    print(df.head(5))
    #Kirjoitetaan lopullinen datasetti
    df.to_csv('dataset_mansion.csv')
 
read_house_data()   
create_dataframe()
