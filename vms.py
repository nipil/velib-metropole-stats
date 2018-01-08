#!/usr/bin/env python3

"""
aze
"""

import argparse
import configparser
import logging
import os.path
import sys

import pdb

import arrow
import peewee
import requests


class VmsException(Exception):
    """
    Custom exception root type
    """


class GpsCoordinates:
    """
    Holds GPS coordinates
        b = GpsCoordinates(48.1, 1.5)
        t = GpsCoordinates(49.1, 1.6)
    """

    def __init__(self, latitude, longitude):
        try:
            self.latitude = float(latitude)
            self.longitude = float(longitude)
        except ValueError as exception:
            raise VmsException("Invalid latitude {0} or longitude {1}: {2}".format(latitude, longitude, exception))

    def __repr__(self):
        return "{0}({1}, {2})".format(__class__.__name__, self.latitude, self.longitude)

    def __lt__(self, other):
        return (self.latitude < other.latitude
            and self.longitude < other.longitude)

    def __iter__(self):
        yield self.latitude
        yield self.longitude

    @classmethod
    def from_dict(cls, data):
        """
        Builds an instance from a dictionary:
        {
            'longitude':2.333428381875887,
            'latitude':48.84373446877937
        }
        """
        try:
            return cls(data['latitude'], data['longitude'])
        except (TypeError, KeyError, ValueError) as exception:
            logging.warning("Input gps coordinates: %s", data)
            raise VmsException("Cannot build gps coordinates: ({0}) {1}".format(type(exception).__name__, exception))


class BaseModel(peewee.Model):
    """
    aze
    """
    class Meta:
        """
        aze
        """
        database = None

    # @classmethod
    # def set_database_filepath(cls, file_path):
    #     """
    #     aze
    #     """
    #     # See https://github.com/coleifer/peewee/issues/221
    #     logging.info("Using database:", file_path)
    #     cls._meta.database = peewee.SqliteDatabase(file_path)

    # @classmethod
    # def open_database(cls):
    #     """
    #     aze
    #     """
    #     cls._meta.database.connect()

    @classmethod
    def create_tables(cls):
        """
        aze
        """
        for subclass in cls.__subclasses__():
            if not subclass.table_exists():
                logging.info("Creating table %s", subclass.__name__)
                subclass.create_table()


class ApiReachabilityStat(BaseModel):
    """
    aze
    """
    moment = peewee.TimestampField(primary_key=True, utc=True)
    result = peewee.BooleanField()
    detail = peewee.TextField(null=True, default=None)


class StationInfo(BaseModel):
    """
    Holds "permanent" station information
    """
    moment = peewee.TimestampField(utc=True)
    state = peewee.CharField() # TODO: "Operative"/?/? could translate to integers ?
    name = peewee.CharField()
    stype = peewee.BooleanField()
    code = peewee.IntegerField()
    due_date = peewee.TimestampField(utc=True, null=True)
    gps_latitude = peewee.FloatField()
    gps_longitude = peewee.FloatField()

    class Meta:

        primary_key = peewee.CompositeKey('moment', 'code')

    def __repr__(self):
        return "{0}({1}, {2}, {3}, {4}, {5}, {6}, {7}, {8})".format(
            __class__.__name__,
            self.moment,
            self.state,
            self.name,
            self.stype,
            self.code,
            self.due_date.timestamp if self.due_date else None,
            self.gps_latitude,
            self.gps_longitude)

    @classmethod
    def from_dict(cls, moment, data):
        """
        Builds an object from a dictionary :
        {
            'state': 'Operative',
            'name': 'Assas - Luxembourg',
            'type': 'yes',
            'code': '6008',
            'dueDate': 1514761200.0,
            'gps': {
                'longitude': 2.333428381875887,
                'latitude': 48.84373446877937
            }
        }
        """
        try:
            return cls(
                moment=moment,
                state=data['state'],
                name=data['name'],
                stype=VelibMetropoleApi.bool_from_yes_no_str(data['type']),
                code=int(data['code']),
                gps_latitude=float(data['gps']['latitude']),
                gps_longitude=float(data['gps']['longitude']),

                # FIX: due_date is None seen on 2018-01-07 10:09
                # {
                #     'name': 'Saint-Fargeau - Mortier',
                #     'code': '20117',
                #     'type': 'yes',
                #     'dueDate': None,
                #     'gps': {
                #         'latitude': 48.872747269036246,
                #         'longitude': 2.408203454302088
                #     },
                #     'state': 'Operative'
                # }

                # WARNING: api provides fractions of second in timestamp, arrow conversion loses it
                due_date=arrow.get(float(data['dueDate'])) if data['dueDate'] is not None else None)

        except (TypeError, KeyError, ValueError, arrow.parser.ParserError) as exception:
            logging.warning("Input station information: %s", data)
            raise VmsException("Cannot build station information: ({0}) {1}".format(type(exception).__name__, exception))


class StationRecord(BaseModel):
    """
    Holds full station information and state at a specific moment in time
    """
    moment = peewee.TimestampField(utc=True)
    code = peewee.IntegerField()
    overflow = peewee.BooleanField()
    max_bike_overflow = peewee.IntegerField()
    nb_e_bike_overflow = peewee.IntegerField()
    kiosk_state = peewee.BooleanField()
    density_level = peewee.IntegerField()
    nb_ebike = peewee.IntegerField()
    nb_free_dock = peewee.IntegerField()
    nb_dock = peewee.IntegerField()
    nb_bike_overflow = peewee.IntegerField()
    nb_e_dock = peewee.IntegerField()
    credit_card = peewee.BooleanField()
    nb_bike = peewee.IntegerField()
    nb_free_e_dock = peewee.IntegerField()
    overflow_activation = peewee.BooleanField()

    class Meta:

        primary_key = peewee.CompositeKey('moment', 'code')

    def __repr__(self):
        return ("{0}({1}, {2}, {3}, {4}, {5}, "
                "{6}, {7}, {8}, {9}, {10}, "
                "{11}, {12}, {13}, {14}, {15}, {16})").format(
                    __class__.__name__,
                    self.moment,
                    self.code,
                    self.overflow,
                    self.max_bike_overflow,
                    self.nb_e_bike_overflow,
                    self.kiosk_state,
                    self.density_level,
                    self.nb_ebike,
                    self.nb_free_dock,
                    self.nb_dock,
                    self.nb_bike_overflow,
                    self.nb_e_dock,
                    self.credit_card,
                    self.nb_bike,
                    self.nb_free_e_dock,
                    self.overflow_activation)

    @classmethod
    def from_dict(cls, moment, data):
        """
        Builds an object from a dictionary :
        {
            'station':{
                'state':'Operative',
                'name':'Assas - Luxembourg',
                'type':'yes',
                'code':'6008',
                'dueDate':1514761200.0,
                'gps':{
                    'longitude':2.333428381875887,
                    'latitude':48.84373446877937
                }
            },
            'overflow':'no',
            'maxBikeOverflow':0,
            'nbEBikeOverflow':0,
            'kioskState':'no',
            'densityLevel':0,
            'nbEbike':2,
            'nbFreeDock':0,
            'nbDock':0,
            'nbBikeOverflow':0,
            'nbEDock':35,
            'creditCard':'no',
            'nbBike':7,
            'nbFreeEDock':25,
            'overflowActivation':'no'
        }
        """
        try:
            return cls(
                moment=moment,
                code=int(data['station']['code']),
                overflow=VelibMetropoleApi.bool_from_yes_no_str(data['overflow']),
                max_bike_overflow=int(data['maxBikeOverflow']),
                nb_e_bike_overflow=int(data['nbEBikeOverflow']),
                kiosk_state=VelibMetropoleApi.bool_from_yes_no_str(data['kioskState']),
                density_level=int(data['densityLevel']),
                nb_ebike=int(data['nbEbike']),
                nb_free_dock=int(data['nbFreeDock']),
                nb_dock=int(data['nbDock']),
                nb_bike_overflow=int(data['nbBikeOverflow']),
                nb_e_dock=int(data['nbEDock']),
                credit_card=VelibMetropoleApi.bool_from_yes_no_str(data['creditCard']),
                nb_bike=int(data['nbBike']),
                nb_free_e_dock=int(data['nbFreeEDock']),
                overflow_activation=VelibMetropoleApi.bool_from_yes_no_str(data['overflowActivation']))
        except (TypeError, KeyError, ValueError) as exception:
            logging.warning("Input station record: %s", data)
            raise VmsException("Cannot build station record: ({0}) {1}".format(type(exception).__name__, exception))


class StationSample:
    """
    aze
    """
    def __init__(self, info, record):
        self.info = info
        self.record = record

    def __repr__(self):
        return ("{0}({1}, {2})").format(
                    __class__.__name__,
                    self.info,
                    self.record)

    @classmethod
    def from_dict(cls, moment, data):
        """
        Builds an object from a dictionary :
        {
            'station':{
                ...
            },
            ...
        }
        """
        return cls(StationInfo.from_dict(moment, data['station']),
                   StationRecord.from_dict(moment, data))


class VelibMetropoleApi:
    """
    Allows access to velib-metropole.fr data feed
    """

    @staticmethod
    def bool_from_yes_no_str(value):
        """
        aze
        """
        if value == "yes":
            return True
        elif value == "no":
            return False
        else:
            raise VmsException("Invalid value for boolean conversion: {0}".format(value))

    URL_TEMPLATE = (
        "https://www.velib-metropole.fr/webapi/map/details?"
        "gpsTopLatitude={0}&"
        "gpsTopLongitude={1}&"
        "gpsBotLatitude={2}&"
        "gpsBotLongitude={3}&"
        "zoomLevel={4}")

    def __init__(self, top_coordinates=None, bottom_coordinates=None, zoom_level=15):

        self._top_coordinates = top_coordinates
        if not self._top_coordinates:
            self._top_coordinates = GpsCoordinates(49.1, 2.7)

        self._bottom_coordinates = bottom_coordinates
        if not self._bottom_coordinates:
            self._bottom_coordinates = GpsCoordinates(48.6, 1.9)

        if not self._bottom_coordinates < self._top_coordinates:
            raise VmsException("Constraint violated: {0} < {1}".format(self._bottom_coordinates, self._top_coordinates))

        self._zoom_level = zoom_level

    def __str__(self):
        return self.to_url()

    def __repr__(self):
        return "{0}({1}, {2}, {3})".format(
            __class__.__name__,
            self._top_coordinates,
            self._bottom_coordinates,
            self._zoom_level)

    def to_url(self):
        """
        Get url with parameters filled with member values
        """
        return self.URL_TEMPLATE.format(
            *self._top_coordinates,
            *self._bottom_coordinates,
            self._zoom_level)

    def get_data(self):
        """
        Fetches API data using parametrized URL
        Parses JSON input
        Returns a list of StationSamples
        """
        try:
            # get content
            request = requests.get(self.to_url(), timeout=1)

            # handle non-ok return codes
            request.raise_for_status()

            # parse json
            json_data = request.json()

            # analyze
            # - list if OK: [{"station": ...]
            # - dict if KO: {"error":{"code":503,"message":"Service Unavailable"}}
            try:
                error = json_data['error']
                raise VmsException("API problem with error: {0}".format(error))
            except TypeError as exception:
                # raised if json_data is not a dictionary
                return json_data
            except KeyError as exception:
                # raised if json_data is a dict but 'error' was unavailable
                raise VmsException("API problem without error: {0}".format(json_data))
        except requests.exceptions.RequestException as exception:
            raise VmsException("Could not download API data: {0}".format(exception))
        except ValueError as exception:
            raise VmsException("Could not parse json content (hexed-binary): {0}".format(request.content.hex()))


class Configuration:

    def __init__(self, config_file):
        self._configuration = configparser.ConfigParser()
        self._configuration.read(os.path.expanduser(config_file))

    def get(self, section, name):
        try:
            return self._configuration[section][name]
        except KeyError:
            raise VmsException("Undefined option '{0}' in configuration section '{1}'".format(name, section))


class App:
    """
    aze
    """
    def __init__(self, args):
        # read configuration file
        self._configuration = Configuration(args.config)
        # setup logging
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S %z',
                            filename=os.path.expanduser(self._configuration.get('logging', 'file_path')),
                            filemode='a')
        console = logging.StreamHandler()
        console.setLevel(logging.WARN)
        formatter = logging.Formatter('%(levelname)s %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        # setup database target
        # BaseModel.set_database_filepath(os.path.expanduser(self._configuration.get('database', 'file_path'))) # TODO: fix
        # BaseModel.open_database() # TODO: fix
        BaseModel.create_tables()
        # instanciate data
        self._api = VelibMetropoleApi()

    def record_api_success(self, timestamp):
        """
        Mark fetch sample as a success in stats
        """
        obj = ApiReachabilityStat(moment=timestamp, result=True)
        obj.save()

    def record_api_error(self, timestamp, message):
        """
        Mark fetch sample as an error in stats
        """
        obj = ApiReachabilityStat(moment=timestamp, result=False, detail=message)
        obj.save()

    def get_api_data(self):
        """
        Parses JSON data into our own objects
        """
        # atomic timestamping
        now = arrow.utcnow()

        # Handles fetch api success stats
        try:
            data = self._api.get_data()
            self.record_api_success(now.timestamp)
        except VmsException as exception:
            self.record_api_error(now.timestamp, str(exception))
            raise exception

        # do *NOT* return an iterator: parsing must be done atomically
        return [StationSample.from_dict(now.timestamp, entry) for entry in data]

    def run(self):
        """
        aze
        """
        station_records = self.get_api_data()
        for i, entry in enumerate(station_records):
            print(i, entry)


def main():
    """
    aze
    """

    try:
        parser = argparse.ArgumentParser(description="velib-metropole-stats")
        parser.add_argument('-c', '--config', default='vms.conf')
        args = parser.parse_args()
        app = App(args)
        app.run()
        sys.exit(0)

    except KeyboardInterrupt:
        logging.warning("Caught SIGINT (Ctrl-C), exiting.")
        sys.exit(1)

    except SystemExit as exception:
        message = "Exiting with return code {0}".format(exception.code)
        if exception.code == 0:
            logging.info(message)
        else:
            logging.warning(message)
            raise exception

    except VmsException as exception:
        logging.critical("%s", exception)
        raise  # DEBUG


if __name__ == '__main__':
    # pdb.set_trace()
    main()
