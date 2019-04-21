
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
#from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.DB.CommonV2 import Common, PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.Common import Common
import os
from datetime import datetime

import pandas as pd
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'limorc'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'


class CCKRToolBox:
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        kpi_path = os.path.dirname(os.path.realpath(__file__))
        base_file = os.path.basename(kpi_path)
        kpi_info = pd.read_excel(
            os.path.join(kpi_path[:- len(base_file)], 'Data', 'Template.xlsx'),
            sheetname="KPI")
        file_template = pd.read_excel(
            os.path.join(kpi_path[:- len(base_file)], 'Data', 'Template.xlsx'),
            sheetname="Template")
        self.kpi_template_info = pd.DataFrame(kpi_info)  # contains the kpis + ean codes
        self.kpi_metadata = self.data_provider.kpi  # information about kpis such as (presentation order)
        self.template_info = self.data_provider.templates  # static data on templates
        self.kpi_template = pd.DataFrame(file_template)  # relevant template for kpis

    def main_calculation(self, *args, **kwargs):

        self.kpi_calc()
        self.common.commit_results_data()


    def kpi_calc(self):

        sum_kpi = 0
        relevant_template_fk = self.template_info[self.template_info["additional_attribute_1"].isin(self.kpi_template['additional_attribute_1'].unique())]['template_fk']
        self.scif = self.scif[self.scif['template_fk'].isin(relevant_template_fk.unique())]
        for row in self.kpi_template_info.iterrows():
            result = self.scif[self.scif['product_ean_code'] == str(row[1]['SKU EAN Code'])]
            result = 0 if result.empty else int(result['facings'].sum())
            result_final = result if result > 0 else None
            score = 100 if result > 0 else None
            self.kpi_res(self.LEVEL3, score, row,result,result_final)
            score = 0 if score is None else score
            self.kpi_res(self.LEVEL2, score, row)
            sum_kpi += score  # score_1(level1)

        denominator = len(self.kpi_template_info.axes[0]) #score_2(level 2)

        self.kpi_res(self.LEVEL1, sum_kpi/100, row, denominator)

    def kpi_res(self, level, score, row, result=None, result_final=None):

        kpi_fks = self.kpi_static_data[
            self.kpi_static_data['atomic_kpi_name'].str.encode('utf8') == row[1]['Atomic Kpi Name'].encode('utf8')]
        if kpi_fks.empty:
            print(row[1]['Atomic Kpi Name'])
            Log.error("differences between kpi template and kpi static table in DB")
            return
        kps_name = kpi_fks['kpi_set_name'].iloc[0]

        if level == self.LEVEL1:
            self.common.write_to_db_result(fk=kpi_fks['kpi_set_fk'].iloc[0], session_uid=self.session_uid, store_fk=self.store_id,
                                           visit_date=self.visit_date.isoformat(), level=self.LEVEL1, kps_name=kps_name,
                                           kps_result=0,kpi_set_fk=kpi_fks['kpi_set_fk'].iloc[0],score=score,
                                           score_2=result, score_3=0)
        else:
            kpi_fk = kpi_fks['kpi_fk'].iloc[0]
            presentation_order = self.kpi_metadata[self.kpi_metadata["pk"] == kpi_fk]['presentation_order'].iloc[0]
            if level == self.LEVEL2:
                self.common.write_to_db_result(fk=kpi_fk, session_uid=self.session_uid, store_fk=self.store_id,
                                               visit_date=self.visit_date.isoformat(), level=self.LEVEL2,
                                               kpk_name=kpi_fks['kpi_name'].iloc[0], kpi_fk=kpi_fk, score=score,
                                               presentation_order=presentation_order,
                                               score_2=100, score_3=0)
            else: #level 3 DB insertion
                self.common.write_to_db_result(level=self.LEVEL3, fk=kpi_fk, kpi_fk=kpi_fk, score=score, session_uid=self.session_uid,
                                               store_fk=self.store_id, visit_date=self.visit_date.isoformat(),
                                               calculation_time=datetime.utcnow().isoformat(), kps_name=kps_name,
                                               missing_kpi_score="Bad", style=("good" if score == 100 else None),
                                               kpi_presentation_order=kpi_fk, atomic_kpi_fk=kpi_fks['atomic_kpi_fk'].iloc[0],
                                               display_text=row[1]['Kpi Display Text (SKU Name)'],
                                               atomic_kpi_presentation_order=1, score_2=100, vs_1_facings=result,
                                               threshold=1, result=result_final,result_2=1, result_3=1)
