import haversine
import pandas as pd
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
        result_dict = {'fk': kpi_fk, 'numerator_id': self.manufacturer_fk, 'denominator_id': self.store_id}

        probes_location = self._get_probes_location()
        store_location = self._get_store_location(probes_location) if not probes_location.empty else pd.DataFrame()

        if not self.is_probes_location_none(probes_location) and not store_location.empty:
            store_lat_and_long = store_location[['pos_lat', 'pos_long']].values[0]
            probes_lat_and_long = probes_location[['pos_lat', 'pos_long']].values

            ''' This is done because of a discrepancy between the coordinate systems used by the probes and store in 
            terms of longitude. The store's longitude is given to Trax by Liberty and is based on the left-handed 
            cartesian coordinate system. Where as the probe's location is calculated by Trax. The probe's longitude is 
            based on right-handed cartesian coordinate system, which is used internationally. The main difference is that the
            right-handed cartesian system depicts the Eastern Hemisphere with a positive longitude where the US uses 
            the coordinate system where the system depicts the Western Hemisphere with a positive longitude. To convert the 
            store's longitude to a right-handed Cartesian System, the longitude of the Western Hemisphere needs to be negative.'''
            store_lat_and_long[1] = -store_lat_and_long[1] if store_lat_and_long[1] > 0 else store_lat_and_long[1]

            threshold = 0
            for scene_id, probe_lat_and_long in zip(probes_location.scene_fk, probes_lat_and_long):
                distance = haversine.haversine(probe_lat_and_long, store_lat_and_long)
                if distance > threshold:
                    threshold = distance
                    relevant_scene_fk = scene_id

            rounded_distance = round(threshold, 2)
            result = 19 if distance < 1 else 20  # 19 = Pass | 20 = Fail

            result_dict['numerator_result'] = rounded_distance
            result_dict['denominator_result'] = relevant_scene_fk
            result_dict['result'] = result
        else:
            result_dict['result'] = 21  # Not Applicable

        self.common_db.write_to_db_result(**result_dict)

    def _get_probes_location(self):
        # probe location
        probes_query = """SELECT scene_fk,store_uid,pos_lat, pos_long
                                         FROM probedata.probe
                                         where session_uid = '{}';""".format(self.session_uid)
        probes_location = pd.read_sql_query(probes_query, self.rds_conn.db)
        return probes_location

    def _get_store_location(self, probes_location):
        # store location
        store_uid = probes_location.store_uid.values[
            0] if not self.is_probes_location_none(
            probes_location) else 'b98cd7c1-c496-11e9-be75-126896c6904a'  # This store uid retrieves lat and long (which is null) of TEST STORE
        stores_query = """SELECT latitude as pos_lat , longitude as pos_long FROM static.stores  WHERE
                                             store_uid = '{}';""".format(store_uid)
        store_location = pd.read_sql_query(stores_query, self.rds_conn.db)
        store_location = store_location[store_location['pos_lat'].notnull()]
        return store_location

    @staticmethod
    def is_probes_location_none(probes_location):
        return (probes_location.store_uid.values[0] is None) and (probes_location.pos_lat.values[0] is None) and \
               (probes_location.pos_long.values[0] is None)

    @staticmethod
    def is_store_location_none(store_location):
        return (store_location.pos_lat.values[0] is None) and \
               (store_location.pos_long.values[0] is None)