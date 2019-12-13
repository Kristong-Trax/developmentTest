import os
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils.GlobalProjects.DIAGEO.KPIGenerator import DIAGEOGenerator
from KPIUtils.DB.Common import Common
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider


class DIAGEOMXToolBox:

    def __init__(self, data_provider, output):
        self.data_provider = data_provider
        self.ps_data_provider = PsDataProvider(data_provider)
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.output = output
        self.common = Common(self.data_provider)
        self.commonV2 = CommonV2(self.data_provider)
        self.diageo_generator = DIAGEOGenerator(self.data_provider, self.output, self.common)
        self.session_uid = self.data_provider.session_uid

    def main_calculation(self):
        # a = 1
        # if self.exists_survey_resposnse() is None:
        """ This function calculates the KPI results """
        # SOS Out Of The Box kpis
        self.diageo_generator.activate_ootb_kpis(self.commonV2)

        # sos by scene type
        self.diageo_generator.sos_by_scene_type(self.commonV2)

        # Global assortment kpis
        assortment_res_dict = self.diageo_generator.diageo_global_assortment_function_v2()
        self.commonV2.save_json_to_new_tables(assortment_res_dict)

        # Global assortment kpis - v3 for NEW MOBILE REPORTS use
        assortment_res_dict_v3 = self.diageo_generator.diageo_global_assortment_function_v3()
        self.commonV2.save_json_to_new_tables(assortment_res_dict_v3)

        # global SOS kpi
        res_dict = self.diageo_generator.diageo_global_share_of_shelf_function()
        self.commonV2.save_json_to_new_tables(res_dict)

        # global touch point kpi
        template_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'Data',
                                     'TOUCH POINT  Noviembre 2019 2.xlsx')
        res_dict = self.diageo_generator.diageo_global_touch_point_function(template_path)
        self.commonV2.save_json_to_new_tables(res_dict)

        # committing to the old tables
        self.common.commit_results_data()  # commit to old tables
        # committing to new tables
        self.commonV2.commit_results_data()



    def exists_survey_resposnse(self):
        # Method is used to check in the db to if survey question_fk 98 is answered
        query = """SELECT  sr.selected_option_fk
                        FROM probedata.survey_response sr
                        Left Join static.survey_question_answer qa on sr.selected_option_text = qa.pk
                        where sr.question_fk = 98 and session_uid = '{}';""".format(self.session_uid)

        session_survey_response = pd.read_sql_query(query, self.ps_data_provider.rds_conn.db).iloc[0, 0]
        return session_survey_response
