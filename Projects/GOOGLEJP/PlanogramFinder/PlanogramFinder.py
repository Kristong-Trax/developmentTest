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
                            templates.template_group,
                            retailers.pk AS 'retailer_fk',
                            regions.pk AS 'region_fk'
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
                            static.template templates ON scenes.template_fk = templates.pk
                                LEFT JOIN
                            static.retailer retailers ON stores.retailer_fk = retailers.pk
                                LEFT JOIN
                            static.regions regions ON stores.region_fk = regions.pk;"""
PLANOGRAMS_QUERY = "SELECT * FROM pservice.planogram_custom_policy;"
STORE_TYPE = "store_type"
STORE_NUMBER_1 = "store_number_1"
TEMPLATE_FK = "template_fk"
TEMPLATE_GROUP = "template_group"
PLANOGRAM_FK = "planogram_fk"
REGION_FK = "region_fk"
RETAILER_FK = "retailer_fk"
SUM_POLICY_ATTRIBUTES = "policy_attrs"
NECESSARY_COLUMNS = [STORE_TYPE, STORE_NUMBER_1, TEMPLATE_FK, TEMPLATE_GROUP, REGION_FK, RETAILER_FK]


class PlanogramFinder(PlanogramFinderBaseClass):

    def get_planogram_id(self, project_name=None, scene_id=None):
        try:
            self.project_name = project_name if project_name else self._data_provider._project_name
            self.scene_id = scene_id if scene_id else self._data_provider._scene_id
            self.rds_conn = self.rds_connection()
            self.get_scene_and_planograms_details()
            return self.get_planogram_id_by_policies()
        except Exception as e:
            Log.error("Could not find the planogram id: " + e.message)
            return None

    def get_planogram_id_by_policies(self):
        filtered_planograms = self.planogram_policies
        for column in NECESSARY_COLUMNS:
            filtered_planograms = filtered_planograms[filtered_planograms[column].isin(['', self.scene_info[column]])]
        transposed_policies = filtered_planograms.T
        for i in transposed_policies.keys():
            policy_attrs = transposed_policies[transposed_policies[i] != ''][i].count()
            filtered_planograms.loc[i, SUM_POLICY_ATTRIBUTES] = policy_attrs
        if filtered_planograms.empty:
            Log.error("There is no Planogram that matches this scene.")
            return None
        else:
            filtered_planograms = filtered_planograms.sort_values(by=[SUM_POLICY_ATTRIBUTES], ascending=False)
            return filtered_planograms.iloc[0][PLANOGRAM_FK]

    def get_scene_and_planograms_details(self):
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
                                            axis=1).fillna('')
        for column in NECESSARY_COLUMNS:
            if column not in self.planogram_policies.keys():
                self.planogram_policies[column] = ''

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

# from Trax.Utils.Conf.Configuration import Config
# from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
# if __name__ == '__main__':
#     LoggerInitializer.init('POG finder test')
#     Config.init()
#     pog = PlanogramFinder(data_provider=None)
#     for scene_id in [49741, 49780]:
#         print "{} = {}".format(scene_id, pog.get_planogram_id(project_name="googlejp", scene_id=scene_id))
