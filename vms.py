#!/usr/bin/env python3

"""
aze
"""

import argparse
import bz2
import configparser
import json
import logging
import os.path
import sys

import pdb

import arrow
import peewee
import requests

from path import Path


DATABASE = peewee.SqliteDatabase(os.path.join(os.path.dirname(os.path.realpath(__file__)), "db.sqlite3"))


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
        database = DATABASE

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
    moment = peewee.IntegerField(primary_key=True)
    result = peewee.BooleanField()
    detail = peewee.TextField(null=True, default=None)

    @classmethod
    def save_api_stat(cls, moment, result, detail=None):
        """
        aze
        """
        try:
            with DATABASE.atomic() as transaction:
                logging.debug("Saving api stat: %s %s", result, detail)
                cls.create(moment=moment, result=result, detail=detail)
        except Exception as exception:  # TODO: reduce exceptions type
            logging.warning("Could not save api statistics (%s %s) due to (%s) %s", moment, result, type(exception).__name__, detail)

class StationCommon:
    """
    aze
    """
    def get_previous(self):
        """
        aze
        """
        cls = self.__class__
        sub_query = cls.select(peewee.fn.MAX(cls.moment)).where(cls.moment < self.moment)
        query = cls.select().where(cls.code == self.code).where(cls.moment == sub_query)
        try:
            return query.get()
        except peewee.DoesNotExist as exception:
            return None

    def save_if_changed(self):
        # logging.debug("Saving if changed %s", self)
        previous = self.get_previous()
        # logging.debug("Last preceding %s", previous)
        print("================")
        print(self)
        print(previous)
        if previous is None or self.has_changed(previous):
            # logging.debug("Inserting %s", self)
            print("insert")
            return self.save(force_insert=True)
        return 0


class StationInfo(StationCommon, BaseModel):
    """
    Holds "permanent" station information
    """
    moment = peewee.IntegerField()
    state = peewee.CharField() # TODO: "Operative"/?/? could translate to integers ?
    name = peewee.CharField()
    stype = peewee.BooleanField()
    code = peewee.IntegerField()
    due_date = peewee.IntegerField(null=True)
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
            self.due_date,
            self.gps_latitude,
            self.gps_longitude)

    def has_changed(self, other):
        """
        Compare everything except moment and code
        """
        return (self.state != other.state
                or self.name != other.name
                or self.stype != other.stype
                or self.due_date != other.due_date
                or self.gps_latitude != other.gps_latitude
                or self.gps_longitude != other.gps_longitude)

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
                due_date=int(data['dueDate']) if data['dueDate'] is not None else None)

        except (TypeError, KeyError, ValueError, arrow.parser.ParserError) as exception:
            logging.warning("Input station information: %s", data)
            raise VmsException("Cannot build station information: ({0}) {1}".format(type(exception).__name__, exception))


class StationRecord(StationCommon, BaseModel):
    """
    Holds full station information and state at a specific moment in time
    """
    moment = peewee.IntegerField()
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

    def has_changed(self, other):
        """
        Compare everything except moment and code
        """
        return (self.overflow != other.overflow
                or self.max_bike_overflow != other.max_bike_overflow
                or self.nb_e_bike_overflow != other.nb_e_bike_overflow
                or self.kiosk_state != other.kiosk_state
                or self.density_level != other.density_level
                or self.nb_ebike != other.nb_ebike
                or self.nb_free_dock != other.nb_free_dock
                or self.nb_dock != other.nb_dock
                or self.nb_bike_overflow != other.nb_bike_overflow
                or self.nb_e_dock != other.nb_e_dock
                or self.credit_card != other.credit_card
                or self.nb_bike != other.nb_bike
                or self.nb_free_e_dock != other.nb_free_e_dock
                or self.overflow_activation != other.overflow_activation)

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
        self._info = info
        self._record = record

    def __repr__(self):
        return ("{0}({1}, {2})").format(
                    __class__.__name__,
                    self._info,
                    self._record)

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

    def save_all_if_changed(self):
        """
        aze
        """
        n_saved = 0
        n_saved += StationCommon.save_if_changed(self._info)
        n_saved += StationCommon.save_if_changed(self._record)
        return n_saved


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

        self._top_coordinates = top_coordinates or GpsCoordinates(49.1, 2.7)

        self._bottom_coordinates = bottom_coordinates or GpsCoordinates(48.6, 1.9)

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

    def get_json(self):
        """
        Fetches API data using parametrized URL
        Parses JSON input
        Returns a list of StationSamples
        """
        try:
            # get content
            request = requests.get(self.to_url(), timeout=30)
            # handle non-ok return codes
            request.raise_for_status()
            # convert to text
            text = request.text
            # return our precious data
            return text

        except requests.exceptions.RequestException as exception:
            # inform caller
            raise VmsException("Could not download API data: {0}".format(exception))


class Configuration:

    def __init__(self, config_file):
        self._configuration = configparser.ConfigParser()
        self._configuration.read(Path(config_file).expand())

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
        self._args = args
        self._configuration = Configuration(self._args.config)

        # log everything to file
        logging.basicConfig(level=logging.DEBUG,
                            format='%(asctime)s %(levelname)s %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S %z',
                            filename=Path(self._configuration.get('logging', 'file_path')).expand(),
                            filemode='a')

        # reduced logging for screen output
        console = logging.StreamHandler()
        logging.getLogger('').addHandler(console)
        console.setFormatter(logging.Formatter('%(levelname)s %(message)s'))
        console.setLevel(logging.WARNING)
        if args.debug:
            console.setLevel(logging.DEBUG)
        else:
            level = self._configuration.get('logging', 'console_log_level')
            numeric_level = getattr(logging, level.upper(), None)
            if not isinstance(numeric_level, int):
                raise VmsException('Invalid log level: {0}'.format(level))
            console.setLevel(numeric_level)

        # setup database target
        # BaseModel.set_database_filepath(os.path.expanduser(self._configuration.get('database', 'file_path'))) # TODO: fix
        # BaseModel.open_database() # TODO: fix
        BaseModel.create_tables()

        # instanciate data
        self._api = VelibMetropoleApi()

    def get_from_file(self, file_path):
        """
        aze
        """
        file_path = Path(self._args.file).expand()
        moment = arrow.get(file_path.name, 'YYYY-MM-DD_HH-mm-ss_ZZZ')
        try:
            with bz2.open(file_path, 'rt') as file_obj:
                data = file_obj.read()
        except OSError as exception:
            raise VmsException("Could not bunzip2 {0}: {1}".format(file_path, exception))
        # return infos to caller
        return (moment, data)

    def get_from_api(self):
        """
        aze
        """
        moment = arrow.utcnow()
        try:
            data = self._api.get_json()
        except VmsException as exception:
            # save stats for errors
            ApiReachabilityStat.save_api_stat(moment.timestamp, False, str(exception))
            raise
        # save stats for successes
        ApiReachabilityStat.save_api_stat(moment.timestamp, True)
        # return infos to caller
        return (moment, data)

    def run(self):
        """
        aze
        """
        # load moment/data from designated source
        if self._args.file:
            moment, data = self.get_from_file(self._args.file)
        else:
            moment, data = self.get_from_api()

        # parse json
        try:
            json_data = json.loads(data)
        except json.JSONDecodeError as exception:
            logging.debug("Invalid JSON: %s", data)
            raise VmsException("Could not parse json data: {0}".format(exception))

        # analyse input
        # - dict if KO: {"error":{"code":503,"message":"Service Unavailable"}}
        # - list if OK: [{"station": ...]
        try:
            error = json_data['error']
            raise VmsException("API problem with error: {0}".format(error))
        except TypeError as exception:
            # raised if json_data is not a dictionary
            # https://www.json.org
            # then it is a list (it was valid json)
            # so we can proceed
            pass
        except KeyError as exception:
            # raised if json_data is a dict but 'error' was unavailable
            raise VmsException("API problem without error: {0}".format(json_data))

        # log some statistics
        logging.info("%s records in incoming data", len(json_data))

        # build objects
        station_records = (StationSample.from_dict(moment.timestamp, entry) for entry in json_data)

        # process
        new_records_count = 0
        with DATABASE.atomic() as transaction:
            for entry in station_records:
                new_records_count += entry.save_all_if_changed()
        logging.info("%s updates detected", new_records_count)


def main():
    """
    aze
    """
    try:
        parser = argparse.ArgumentParser(description="velib-metropole-stats")
        parser.add_argument('-c', '--config', default='vms.conf')
        parser.add_argument('-d', '--debug', default=False, action='store_true')
        parser.add_argument('-f', '--file')
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
