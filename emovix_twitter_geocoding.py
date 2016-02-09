__author__ = 'Jordi Vilaplana'

from pymongo import MongoClient
from geopy.geocoders import Nominatim
import json
import logging

logging.basicConfig(
    filename='emovix_twitter_geocoding.log',
    level=logging.WARNING,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%d-%m-%y %H:%M')

# Configuration parameters
database_host = ""
database_name = ""

client = None
db = None

if __name__ == '__main__':
    logging.debug('emovix_twitter_geocoding.py starting ...')

    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)
        database_host = config['database_host']
        database_name = config['database_name']

    client = MongoClient('mongodb://' + database_host + ':27017/')
    db = client[database_name]

    geolocator = Nominatim()

    while True:
        try:
            user = db.twitterUser.find_one({ "location_geocoding": { "$exists": False } })

            if user:
                user_location = user['location']
                if user_location:
                    user_location = user_location.encode('utf-8').lower()

                cached_location = db.twitterGeocoding.find_one({ "location": user_location})

                if cached_location:
                    user['location_geocoding'] = { "latitude": cached_location['latitude'], "longitude": cached_location['longitude'] }
                else:
                    location = geolocator.geocode(user_location)
                    if user_location == None:
                        location = None
                    if location:
                        user['location_geocoding'] = { "latitude": location.latitude, "longitude": location.longitude }
                        db.twitterGeocoding.insert_one( { "location": user_location, "latitude": location[1][0], "longitude": location[1][1]})
                    else:
                        user['location_geocoding'] = { "latitude": 0, "longitude": 0 }
                        db.twitterGeocoding.insert_one( { "location": user_location, "latitude": 0, "longitude": 0})

                db.twitterUser.update( { "_id": user['_id']}, user, upsert=True)


        except Exception as e:
            # Oh well, just keep going
            logging.error(e.__class__)
            logging.error(e)
            continue
        except KeyboardInterrupt:
            break
