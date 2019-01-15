from Trax.Apps.Services.KEngine.Handlers.Utils.Scripts import PlanogramFinderBaseClass
import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
import json

__author__ = 'Shivi'

SCENE_INFO_QUERY = """  SELECT 
                            stores.store_type,
                            stores.store_number_1,
                            templates.pk AS 'template_fk',
                            templates.template_group
                        FROM
                            (SELECT 
                                *
                            FROM
                                probedata.scene
                            WHERE
                                pk = {}) scenes
                                JOIN
                            probedata.session sessions ON scenes.session_uid = sessions.session_uid
                                JOIN
                            static.stores stores ON stores.pk = sessions.store_fk
                                JOIN
                            static.template templates ON scenes.template_fk = templates.pk;"""
PLANOGRAMS_QUERY = "SELECT * FROM pservice.planogram_custom_policy;"
STORE_TYPE = "store_type"
STORE_NUMBER_1 = "store_number_1"
TEMPLATE_FK = "template_fk"
TEMPLATE_GROUP = "template_group"
PLANOGRAM_FK = "planogram_fk"
NECESSARY_COLUMNS = [STORE_TYPE, STORE_NUMBER_1, TEMPLATE_FK, TEMPLATE_GROUP]


class PlanogramCompliance(PlanogramFinderBaseClass):

    def get_planogram_id(self):
        self.project_name = self._data_provider._project_name
        self.scene_id = self._data_provider._scene_id
        self.rds_conn = self.rds_connection()
        return self.get_scene_info_and_planograms()

    def get_planogram_id_by_policies(self):
        if self.planogram_policies.empty:
            Log.error("There is no Planogram that matches this scene.")
            return None
        filtered_planograms = self.planogram_policies
        for column in NECESSARY_COLUMNS:
            filtered_planograms = filtered_planograms[filtered_planograms[column] == self.scene_info[column] |
                                                      filtered_planograms[column].isnan]
        return filtered_planograms.iloc[0][PLANOGRAM_FK]

    def get_scene_info_and_planograms(self):
        self.rds_conn.connect_rds()
        self.scene_info = pd.read_sql_query(SCENE_INFO_QUERY.format(self.scene_id), self.rds_conn.db).iloc[0]
        self.planograms_info = pd.read_sql_query(PLANOGRAMS_QUERY, self.rds_conn.db)
        self.get_planograms_details()

    def get_planograms_details(self):
        if self.planograms_info.empty:
            return
        policies_df = self.planograms_info.policy.apply(json.loads).apply(pd.io.json.json_normalize).pipe(
            lambda x: pd.concat(x.values))
        pogs_df = self.planograms_info.drop(['policy'], axis=1)
        self.planogram_policies = pd.concat([pogs_df.reset_index(drop=True), policies_df.reset_index(drop=True)],
                                            axis=1)
        for column in NECESSARY_COLUMNS:
            if not self.planogram_policies.has_key(column):
                self.planogram_policies[column] = None

    def rds_connection(self):
        if not hasattr(self, '_rds_conn'):
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self._rds_conn.db)
        except Exception as e:
            self._rds_conn.disconnect_rds()
            self._rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
            Log.error(e.message)
        return self._rds_conn
