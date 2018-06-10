
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
# from Trax.Utils.Logging.Logger import Log
import numpy as np
from KPIUtils_v2.DB.Common import Common
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey
import pandas as pd
# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class PERFETTICNToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_new_kpi_static_data()
        self.kpi_results_queries = []
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.assortment = Assortment(self.data_provider, self.output)
        self.template = self.data_provider.all_templates

    def main_calculation(self, *args, **kwargs):

        self.display_count()
        self.assortment_calculation()
        self.common.commit_results_data_to_new_tables()

    def display_count (self):

        num_brands = {}
        display_info = self.scif['template_fk']
        display_fks = display_info.unique()
        template_seco = self.template[self.template['included_in_secondary_shelf_report'] == 'Y']['template_fk']
        display_fks = list(filter(lambda x : x  in template_seco.values, display_fks))
        count_fk = self.kpi_static_data[self.kpi_static_data['client_name'] == 'COUNT OF DISPLAY']['pk'].iloc[0]
        for value in display_fks:
            num_brands[value] = display_info[display_info == value].count()
            score = num_brands[value]
            self.common.write_to_db_result_new_tables(count_fk,value , score,None, None, score, score)

        return

    def assortment_calculation(self):
        """
        This function calculates the KPI results.
        """
        lvl3_result = self.assortment.calculate_lvl3_assortment()

        for result in lvl3_result.itertuples():
            score = result.in_store
            self.common.write_to_db_result_new_tables(result.kpi_fk_lvl3, result.product_fk, result.in_store,
                                                      score, result.assortment_group_fk, 1, score)
        if not lvl3_result.empty:
            lvl2_result = self.assortment.calculate_lvl2_assortment(lvl3_result)
            for result in lvl2_result.itertuples():
                denominator_res = result.total
                res = np.divide(float(result.passes), float(denominator_res))
                if result.passes >= 1:
                    score = 1
                else:
                    score = 0
                self.common.write_to_db_result_new_tables(result.kpi_fk_lvl2, result.assortment_group_fk, result.passes,
                                                          res, result.assortment_super_group_fk, denominator_res, score)

            if not lvl2_result.empty:
                lvl1_result = self.assortment.calculate_lvl1_assortment(lvl2_result)
                for result in lvl1_result.itertuples():
                    denominator_res = result.total
                    res = np.divide(float(result.passes), float(denominator_res))
                    if res >= 0:
                        score = 1
                    else:
                        score = 0
                    self.common.write_to_db_result_new_tables(fk=result.kpi_fk_lvl1,
                                                              numerator_id=result.assortment_super_group_fk,
                                                              numerator_result=result.passes,
                                                              denominator_result=denominator_res,
                                                              result=res, score=score)
        return

    # def get_match_display(self,session_uid):
    #
    #     get_query = """
    #                 SELECT st.store_number_1,display_name, COUNT(*) as cnt
    #                 FROM probedata.match_display_in_scene
    #                 JOIN static.display ON static.display.pk = probedata.match_display_in_scene.display_fk
    #                 JOIN  (SELECT  pk AS scene_pk, store_fk
    #                 FROM probedata.scene
    #                 WHERE session_uid = '{}') scene_detail ON scene_pk = scene_fk
    #                 JOIN  static.stores st ON st.pk = store_fk
    #                 GROUP BY display_name , st.store_number_1;
    #             """.format(session_uid)
    #     data = pd.read_sql(get_query, self.rds_conn.db)
    #     return data

    # def get_match_display(self,session_uid):
    #
    #     get_query="""
    #                 SELECT display_name, COUNT(*)
    #                 FROM  probedata.match_display_in_scene
    #                 JOIN static.display ON static.display.pk = probedata.match_display_in_scene.display_fk
    #                 WHERE
    #                 scene_fk IN (SELECT pk
    #                 FROM  probedata.scene
    #                 WHERE session_uid = '{}')
    #                 GROUP BY display_name;
    #             """.format(session_uid)
    #     data = pd.read_sql(get_query, self.rds_conn.db)
    #     return data

