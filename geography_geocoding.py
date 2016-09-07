from geopy.exc import GeocoderTimedOut

__author__ = 'Jordi Vilaplana'

from pymongo import MongoClient
from geopy.geocoders import Nominatim
from geopy.geocoders import GoogleV3
from geopy.exc import GeocoderQuotaExceeded
from geopy.exc import GeocoderServiceError
import json
import logging
import time

logging.basicConfig(
    filename='geography_geocoding.log',
    level=logging.WARNING,
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%d-%m-%y %H:%M')

# Configuration parameters
database_host = ""
database_name = ""

client = None
db = None

if __name__ == '__main__':
    logging.debug('geography_geocoding.py starting ...')

    # Load configuration
    with open('config.json', 'r') as f:
        config = json.load(f)
        database_host = config['database_host']
        database_name = config['database_name']

    client = MongoClient('mongodb://' + database_host + ':27017/')
    db = client[database_name]

    # geolocator = Nominatim()
    geolocator = GoogleV3()

    # Let's run for a while ...
    while True:
        # try:
        # Get a twitter user account from the database that has not been geocoded yet.
        # This is, where the "location_geocoding" field doesn't exist.
        tweet = db.geo_eu_twitterStatus_ca.find_one({"user.location_geocoding": {"$exists": False}})

        if not tweet:
            break

        user = tweet['user']
        # If we get a user ... (maybe we have already geocoded all the users?)
        if user:
            # Get the user location. This field is filled in the twitter profile page of the user, and it's an
            # optional string field, so expect to get almost anything here.
            user_location = user['location']

            # As mentioned, this could be empty ...
            if user_location:
                user_location = user_location.encode('utf-8').lower()

            # So now we are interested in checking whether we have already checked that same location before
            # or not. Once a location has been checked, it is 'cached' in the 'twitterGeocoding' collection.
            cached_location = db.twitterGeocoding.find_one({"location": user_location})

            # So, did we have it?
            if cached_location:
                # Yay! We just need to add the existing coordinates to the 'location_geocoding' field of the user.
                # This is in geoJSON format: http://geojson.org/
                user['location_geocoding'] = {"type": "Point",
                                              "coordinates": [cached_location['longitude'], cached_location['latitude']]}

            else:
                # Nay! Welp, let's get the coordinates from our geocoding service.
                logging.warning("Going to get the location " + user_location + " ...")

                location = None
                try:
                    location = geolocator.geocode(user_location)
                    # Just wait for 1 second (required by Nominatim usage policy)
                    time.sleep(1)
                except GeocoderTimedOut as e:
                    logging.error("Nominatim gave a GeocoderTimedOut exception for this location ...")
                except GeocoderQuotaExceeded as e:
                    logging.error("The geocoder quota has been exceded, we are waiting for 1 hour now ...")
                    time.sleep(3600)
                    continue
                except GeocoderServiceError as e:
                    logging.error("Geocoder 500 Service Exception error, neglecting this location ...")
                logging.warning("Done!")

                # Wait, if our twitter user had no location, let's just override whatever the geocoding service
                # got for us. We do not want some random coordinates for the ones that do not have a location.
                if user_location == None:
                    # Okay, maybe not
                    location = None

                # So, did we get a location from our geocoding service or not?
                if location:
                    # Yay! So let's set the 'location_geocoding' parameter of our user to the coordinates obtained
                    # from the geocoding service and cache these coordinates as well.
                    user['location_geocoding'] = {"type": "Point", "coordinates": [location.longitude, location.latitude]}
                    db.twitterGeocoding.insert_one(
                        {"location": user_location, "latitude": location[1][0], "longitude": location[1][1],
                         "total_users": 1})
                else:
                    # Okay, so the user provided some weird location and our geocoding service was unable to
                    # provide us a set of coordinates, uh? Let's just set the coordiantes to {0,0} and we will
                    # sort this out later.
                    user['location_geocoding'] = {"type": "Point", "coordinates": [0, 0]}
                    db.twitterGeocoding.insert_one(
                        {"location": user_location, "latitude": 0, "longitude": 0, "total_users": 1})

            tweet['user'] = user
            # Let's just end this and update our twitter user with whatever the result was.
            db.geo_eu_twitterStatus_ca.update({"_id": tweet['_id']}, tweet, upsert=True)

        # No user found, maybe all work is done? Let's take a coffee for 10 seconds ...
        else:
            break
