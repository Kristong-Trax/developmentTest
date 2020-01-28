from Trax.Utils.Logging.Logger import Log
import haversine
import pandas as pd
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

__author__ = 'krishnat'


class LIBERTYGeoToolBox:
    def __init__(self, data_provider, output, common_db):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.common_db = common_db
        self.manufacturer_fk = self.data_provider[Data.OWN_MANUFACTURER]['param_value'].iloc[0]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)

    def main_calculation(self):
        self.calculate_geolocation_distance()

    def calculate_geolocation_distance(self):
        kpi_fk = self.common_db.get_kpi_fk_by_kpi_type('Geolocation')

        # probe location
        probes_query = """SELECT scene_fk,store_uid,pos_lat, pos_long
                                 FROM probedata.probe
                                 where session_uid = '{}';""".format(self.session_uid)
        probes_location = pd.read_sql_query(probes_query, self.rds_conn.db)
        probes_lat_and_long = probes_location[['pos_lat', 'pos_long']].values

        # store location
        store_uid = probes_location.store_uid.values[
            0] if not self.is_probes_location_none(probes_location) else 0
        stores_query = """SELECT latitude as pos_lat , longitude as pos_long FROM static.stores  WHERE
                                     store_uid = '{}';""".format(store_uid)
        store_location = pd.read_sql_query(stores_query, self.rds_conn.db)

        # Accounts for Index Errors and Empty DataFrame
        if self.is_store_location_none(store_location) or self.is_probes_location_none(probes_location):
            return self.common_db.write_to_db_result(kpi_fk, result=None)

        store_lat_and_long = store_location[['pos_lat', 'pos_long']].values[0]
        store_lat_and_long[1] = -store_lat_and_long[1] if store_lat_and_long[1] > 0 else store_lat_and_long[1]

        threshold = 0
        for scene_id, probe_lat_and_long in zip(probes_location.scene_fk, probes_lat_and_long):
            distance = haversine.haversine(probe_lat_and_long, store_lat_and_long)
            if distance > threshold:
                threshold = distance
                relevant_scene_fk = scene_id

        rounded_distance = round(threshold, 2)
        result = 1 if rounded_distance < 1 else 0

        return self.common_db.write_to_db_result(kpi_fk, numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                          numerator_result=rounded_distance, denominator_result=relevant_scene_fk,
                                          result=result)

    @property
    def rds_conn(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        return self._rds_conn

    @staticmethod
    def is_probes_location_none(probes_location):
        return (probes_location.store_uid.values[0] is None) and (probes_location.pos_lat.values[0] is None) and \
               (probes_location.pos_long.values[0] is None)

    @staticmethod
    def is_store_location_none(store_location):
        return (store_location.pos_lat.values[0] is None) and \
               (store_location.pos_long.values[0] is None)
