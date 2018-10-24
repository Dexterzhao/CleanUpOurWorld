import pandas as pd
from geopy.geocoders import Nominatim
import numpy as np


def conversion():

    # geolocator = Nominatim(user_agent="specify_your_app_name_here")
    # location = geolocator.geocode("Haddam Meadows St. Park")
    # print(location.address)
    # #Flatiron Building, 175, 5th Avenue, Flatiron, New York, NYC, New York, ...
    # print((location.latitude, location.longitude))
    # #(40.7410861, -73.9896297241625)
    # print(location.raw) usecols=['inc_id', 'Water Body', 'City'],
    try:
        df = pd.read_csv("/home/dexter/Documents/PostGisData/ConneticutRiverConservancy.csv",error_bad_lines=False)
        # df.insert(0, 'New_ID', range(880, 880 + len(df)))
        df["latitude"] = np.nan
        df["longitude"] = np.nan
        geolocator = Nominatim(user_agent="specify_your_app_name_here",timeout=3)

        print(df.head(5))
        for i, row in df.iterrows():
        # print(row['Water Body'],row['City'])
            if not pd.isnull(row['Water Body']) and len(row['Water Body'].split()) < 11:
                location = geolocator.geocode(row['Water Body'])
                if location is None:
                    if not pd.isnull(row['City']) and len(row['City'].split()) < 11:
                        location = geolocator.geocode(row['City'])
                        if location is None:
                            print("none!\n")
                        else:
                            # df.set_value(i,'latitude',location.raw['lat'])
                            # df.set_value(i,'longitude',location.raw['lon'])
                            print(row['City'])
                            print(location.raw['lat'],location.raw['lon'])
                            df.at[i,'latitude']=location.raw['lat']
                            df.at[i,'longitude']=location.raw['lon']
                else:
                    # df.set_value(i,'latitude',location.raw['lat'])
                    # df.set_value(i,'longitude',location.raw['lon'])
                    print(row['Water Body'])
                    print(location.raw['lat'],location.raw['lon'])
                    df.at[i,'latitude']=location.raw['lat']
                    df.at[i,'longitude']=location.raw['lon']
                    # print(location)
            else:
                if not pd.isnull(row['City']) and len(row['City'].split()) < 11:
                    location = geolocator.geocode(row['City'])
                    if location is None:
                        print("none!\n")
                    else:
                        # df.set_value(i,'latitude',location.raw['lat'])
                        # df.set_value(i,'longitude',location.raw['lon'])
                        print(row['City'])
                        print(location.raw['lat'],location.raw['lon'])
                        df.at[i,'latitude']=location.raw['lat']
                        df.at[i,'longitude']=location.raw['lon']
        # print(df.head(5))

    finally:
        df.to_csv("sample.csv")

if __name__ == "__main__":
    conversion()
