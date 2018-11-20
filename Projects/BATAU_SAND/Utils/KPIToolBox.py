import pandas as pd
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'sathiyanarayanan'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

#KPI DB
PS_KPI_FAMILY = 19
KPI_FAMILY='kpi_family_fk'
TYPE = "type"

MAPPINGS = {'manufacturer_name':'manufacturer_fk',
            'brand_name':'brand_fk',
            'category':'category_fk',
            'sub_category':'sub_category_fk',
            'product_name':'product_fk'}

# Template
KPI_SHEET  = 'KPI'
KPI_NAME = 'kpi_name'
KPI_TEMPLATE_TYPE = 'template_type'
KPI_TYPE = 'kpi_type'
NUMERATOR_FILTER = 'numerator_filter'
DENOMINATOR_FILTER = 'denominator_filter'
GROUP_BY = 'group_by'

class BATAUToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'Template.xlsx')
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
        self.kpi_static_data = self.common.new_kpi_static_data
        self.kpi_results_queries = []

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        df_tp_ps_kpis = self.get_template_details(KPI_SHEET)

        for index, row in df_tp_ps_kpis.iterrows():
            kpi=self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                 & (self.kpi_static_data[TYPE] == row[KPI_NAME])]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(row[KPI_NAME]))
            else:
                #print("KPI Name:{} pk:{}".format(row[KPI_NAME],int(kpi['pk'])))
                if row[KPI_TYPE]=='SOR':
                    self.calculate_share_of_range(kpi_fk=kpi['pk'],
                                                  group_by = row[GROUP_BY],
                                                  numerator_filter = row[NUMERATOR_FILTER],
                                                  denominator_filter = row[DENOMINATOR_FILTER])
        self.common.commit_results_data_to_new_tables()
        score = 0
        return score

    def calculate_share_of_range(self,**filters):

        kpi_output = "kpi_fk:{}, numerator_id:{}, numerator_result:{}, result:{}, denominator_id:{}, denominator_result:{}, score:{}"
        kpi_fk = int(filters['kpi_fk'])
        group_by = [MAPPINGS[x.strip()] for x in filters[GROUP_BY].split(',')]

        denominator = len(self.scif.query(filters[DENOMINATOR_FILTER]))
        denominator_id = numerator= numerator_id= result= score= 0
        numerator = 0
        numerator_id = 0
        result = 0
        score = 0

        df_numerator = pd.DataFrame(self.scif.query(filters[NUMERATOR_FILTER]).groupby(group_by).size().reset_index(name='count'))

        if df_numerator.empty:
            #print("No records matching filter:{}".format(filters[NUMERATOR_FILTER]))
            print(kpi_output.format(kpi_fk, numerator_id, numerator, result, denominator_id, denominator, score))
            self.common.write_to_db_result_new_tables(fk=kpi_fk,
                                                      numerator_id=numerator_id,
                                                      numerator_result=numerator,
                                                      result=result,
                                                      denominator_id=denominator_id,
                                                      denominator_result=denominator,
                                                      score=score,
                                                      score_after_actions=0)
        else:
            for index, row in df_numerator.iterrows():
                numerator = int(row['count'])
                numerator_id = int(row[group_by[len(group_by)-1]])
                denominator_id = int(self.store_id)

                try:
                    result = round(float(numerator) / float(denominator),2)
                except:
                    result = 0

                score = result
                self.common.write_to_db_result_new_tables(fk = kpi_fk,
                                                          numerator_id = numerator_id,
                                                          numerator_result = numerator,
                                                          result = result,
                                                          denominator_id = denominator_id,
                                                          denominator_result = denominator,
                                                          score = score,
                                                          score_after_actions = 0)

                print(kpi_output.format(kpi_fk,numerator_id,numerator,result,denominator_id,denominator,score))

    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheet_name=sheet_name)
        return template