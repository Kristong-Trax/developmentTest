import pandas as pd
from Trax.Utils.Logging.Logger import Log
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.SOSCalculations import SOS

from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Keys import DbUsers

__author__ = 'Sam'

############
STORE_TYPES = {
    "CR SOVI RED": "CR&LT",
    "DRUG SOVI RED": "Drug",
    "VALUE SOVI RED": "Value",
    "United Test - Value SOVI RED": "Value",
    "United Test - Drug SOVI RED": "Drug",
    "United Test - CR SOVI RED": "CR&LT",
    "FSOP - QSR": "QSR",
}

class SceneSessionToolBox:
    HIERARCHY_SESSION_TABLE = "report.kpi_hierarchy"
    SESSION_RESULT_FK = "session_result_fk"
    SCENE_RESULT_FK = "scene_result_fk"
    PARENT_FK = "session_parent_fk"
    SHOULD_ENTER = "should_enter"
    COLUMNS = [SESSION_RESULT_FK, PARENT_FK, SHOULD_ENTER, SCENE_RESULT_FK]
    SCENE_SESSION_KPI = {
                        2160: 2161
                            }

    def __init__(self, data_provider):
        self.data_provider = data_provider
        self.common = CommonV2(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.manufacturer_fk = 1
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.scif = self.scif[~(self.scif['product_type'] == 'Irrelevant')]
        self.region = self.store_info['region_name'].iloc[0]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.program = self.store_info['additional_attribute_3'].iloc[0]
        if self.store_type in STORE_TYPES: #####
            self.store_type = STORE_TYPES[self.store_type] ####
        self.store_attr = self.store_info['additional_attribute_3'].iloc[0]



        self.results = self.data_provider[Data.SCENE_KPI_RESULTS]
        self.hierarchy_table = pd.DataFrame(columns=self.COLUMNS)

    # main functions:


    def main_calculation(self, *args, **kwargs):
        """
            This function gets all the scene results from the SceneKPI, after that calculates every session's KPI,
            and in the end it calls "filter results" to choose every KPI and scene and write the results in DB.
        """
        self.write_scene_parent()

    def write_scene_parent(self):
        scene_kpis = self.results['kpi_level_2_fk'].unique().tolist()
        for i, scene_kpi in enumerate(scene_kpis):
            if scene_kpi in self.SCENE_SESSION_KPI:
                session_kpi = self.SCENE_SESSION_KPI[scene_kpi]
                kpi_res = self.results[self.results['kpi_level_2_fk'] == scene_kpi]
                num = kpi_res['numerator_result'].sum()
                den = kpi_res['denominator_result'].sum()
                score = kpi_res['score'].sum()

                if den:
                    ratio = round((float(num) / den)*100, 2)
                else:
                    ratio = 0

                self.common.write_to_db_result(fk=session_kpi, numerator_result=num,
                                                   numerator_id=self.manufacturer_fk, denominator_id=self.store_id,
                                                   denominator_result=den, result=ratio, score=score, target=den)
                self.write_hierarchy(kpi_res, i)

    def write_hierarchy(self, kpi_res, i):
        for j, kpi_line in kpi_res.iterrows():
            kpi_fk = kpi_line['scene_kpi_fk']
            new_line = pd.DataFrame([(0, i, True, kpi_fk)],
                                      columns=self.COLUMNS)
            self.hierarchy_table = pd.concat((self.hierarchy_table, new_line))

    def commit_results(self):
        insert_queries = self.common.merge_insert_queries(self.common.kpi_results[self.common.QUERY].tolist())
        if not insert_queries:
            return

        local_con = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = local_con.db.cursor()

        Log.info('Start committing results')
        cur.execute(insert_queries[0] + ";")
        cur.execute(self.common.queries.get_last_id())
        last_id = cur.fetchmany()
        self.hierarchy_table[self.PARENT_FK] += int(last_id[0][0])
        self.common.kpi_results = self.hierarchy_table
        insert_tree_queries = self.common.get_insert_queries_hierarchy('session', True)
        if insert_tree_queries:
            insert_tree_queries = self.common.merge_insert_queries(insert_tree_queries)[0] + ";"
            cur.execute(insert_tree_queries)
        local_con.db.commit()

