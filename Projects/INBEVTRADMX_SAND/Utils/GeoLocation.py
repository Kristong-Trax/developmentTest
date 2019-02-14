from Trax.Utils.Logging.Logger import Log
from haversine import haversine
import pandas as pd
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert


# from math import radians, cos, sin, asin, sqrt

__author__ = 'yoava'


# def haversine2(lat1, lon1, lat2, lon2):
#     r = 6272.8
#
#     d_lat = radians(lat2 - lat1)
#     d_lon = radians(lon2 - lon1)
#     lat1 = radians(lat1)
#     lat2 = radians(lat2)
#
#     a = sin(d_lat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(d_lon / 2) ** 2
#     c = 2 * asin(sqrt(a))
#
#     return r * c * 1000


class INBEVTRADMX_SANDGeo:

    GEO_THRESHOLD = 100
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, rds_conn, session_uid, data_provider, kpi_static_data, common, common2):
        self.rds_conn = rds_conn
        self.session_uid = session_uid
        self.data_provider = data_provider
        self.common = common
        self.common2 = common2
        self.kpi_static_data = kpi_static_data

    @staticmethod
    def calculate_distance_between_two_points(first_point, second_point):
        """
        this method calculates haversine distance between two points on earth. it is as generic as it could be for
        testing purpose
        :param first_point: first point
        :param second_point: second point
        :return: haversine distance
        """
        # result = haversine(first_point.pos_lat, first_point.pos_long, second_point.pos_lat, second_point.pos_long)
        result = haversine((first_point.pos_lat, first_point.pos_long),
                           (second_point.pos_lat, second_point.pos_long)) * 1000
        return "%.1f" % round(result, 1)

    @staticmethod
    def is_probes_location_none(probes_location):
        return (probes_location.store_uid.values[0] is None) and (probes_location.pos_lat.values[0] is None) and \
               (probes_location.pos_long.values[0] is None)

    @staticmethod
    def is_store_location_none(store_location):
        return (store_location.pos_lat.values[0] is None) or (store_location.pos_long.values[0] is None)

    def calculate_geo_location(self):
        """"
        this method calculates the distance between the store we are visiting and the average location of the taken
        probes to prevent frauds.
        it uses haversine method to measure distance between two points on earth
        """
        # current session uid
        session_uid = self.session_uid
        # get average probe location
        probes_query = """SELECT store_uid, CAST(AVG(pos_lat) AS DECIMAL(10,7)) AS pos_lat,
               CAST(AVG(pos_long) AS DECIMAL(10,7)) AS pos_long FROM probedata.probe WHERE delete_time IS NULL
                   AND (pos_lat <> 0 OR pos_long <> 0)
                   AND session_uid = '""" + str(session_uid) + "';"
        probes_location = pd.read_sql_query(probes_query, self.rds_conn.db)
        if not probes_location.empty and not self.is_probes_location_none(probes_location):
            store_uid = probes_location.store_uid.values[0]
            # get store location
            stores_query = """SELECT latitude as pos_lat , longitude as pos_long FROM static.stores s WHERE
                       s.delete_date IS NULL AND (s.test_store = 'N' OR s.test_store IS NULL) and
                       store_uid = '""" + str(store_uid) + "';"
            store_location = pd.read_sql_query(stores_query, self.rds_conn.db)
            if not store_location.empty and not self.is_store_location_none(store_location):
                distance = self.calculate_distance_between_two_points(store_location, probes_location)
                Log.info("distance in session {} is {}".format(session_uid, distance))
                return distance
            else:
                Log.info("store location is not defined for session {} , "
                         "can't calculate distance".format(session_uid))
                return -1
        else:
            Log.info("probe location is not defined for session {} , can't calculate distance".format(session_uid))
            return -1

    def get_geo_score(self, geo_result):
        """
        get the correct score for the geo result
        :param geo_result: GEO location result
        :return: score
        """
        if geo_result < 0:
            return geo_result
        elif geo_result < self.GEO_THRESHOLD:
            return 100
        else:
            return 0

    def write_atomic_kpi(self, kpi_name, geo_result):
        """
        this method prepares insert query to kpi_results table
        :param kpi_name: Geo location
        :param geo_result: result of GEO location KPI
        :return: None
        """
        atomic_kpi_fk = self.kpi_static_data.atomic_kpi_fk[self.kpi_static_data.kpi_name == kpi_name].values[0]
        basic_dict = self.common.create_basic_dict(atomic_kpi_fk)
        basic_dict['display_text'] = kpi_name
        basic_dict['threshold'] = self.GEO_THRESHOLD
        basic_dict['result'] = geo_result
        geo_score = self.get_geo_score(geo_result)
        self.common.write_to_db_result(atomic_kpi_fk, self.LEVEL3, geo_score, **basic_dict)
        new_kpi_fk = self.common2.get_kpi_fk_by_kpi_name(kpi_name)
        self.common2.write_to_db_result(fk=new_kpi_fk, result=geo_result, target=self.GEO_THRESHOLD, score=geo_score)

    def create_kpi_insert_attributes(self, fk, geo_result, geo_score, level):
        """
        this method creates attributes dictionary for later insert
        :param fk: fk for the correct level
        :param geo_result:result of GEO location KPI
        :param geo_score: score for GEO location KPI
        :param level: correct level for inserting
        :return: attrubuts
        """
        attrs = self.common.create_attributes_dict(geo_score, fk, level)
        attrs['score_2'] = {0: geo_result}
        attrs['score_3'] = {0: self.GEO_THRESHOLD}
        return attrs

    def insert_attributes_to_db(self, attrs, table):
        query = insert(attrs, table)
        self.common.kpi_results_queries.append(query)

    def write_kpi(self, kpi_name, geo_result):
        kpi_fk = self.kpi_static_data.kpi_fk[self.kpi_static_data.kpi_name == kpi_name].values[0]
        geo_score = self.get_geo_score(geo_result)
        attrs = self.create_kpi_insert_attributes(kpi_fk, geo_result, geo_score, self.LEVEL2)
        self.insert_attributes_to_db(attrs, self.common.KPK_RESULT)

    def write_kpi_set(self, kpi_name, geo_result):
        kpi_set_fk = self.kpi_static_data.kpi_set_fk[self.kpi_static_data.kpi_name == kpi_name].values[0]
        attrs = self.create_kpi_insert_attributes(kpi_set_fk, geo_result, self.get_geo_score(geo_result),
                                                  self.LEVEL1)
        self.insert_attributes_to_db(attrs, self.common.KPS_RESULT)

    def write_geo_to_db(self, geo_result):
        """
        this method extracts relevant data and inserts it to db tables
        :param geo_result: result of GEO location KPI
        :return: None
        """
        kpi_name = "Geo location"
        self.write_atomic_kpi(kpi_name, geo_result)
        self.write_kpi(kpi_name, geo_result)
        self.write_kpi_set(kpi_name, geo_result)
