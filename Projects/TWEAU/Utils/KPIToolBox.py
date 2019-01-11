import os
import math

import pandas as pd

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.CommonV2 import Common, PSProjectConnector
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nidhin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
KPI_FAMILY = 'kpi_family_fk'
PS_KPI_FAMILY = 19
TYPE = 'type'

# Template
KPI_SHEET = 'KPI'
KPI_TYPE = 'kpi_type'
KPI_NAME = 'kpi_name'
CATEGORY_SHEET = 'Category'

NUMERATOR_FK = 'numerator_key'
DENOMINATOR_FK = 'denominator_key'
NUMERATOR_FILTER_ENTITIES = ['filter_entity_1', 'filter_entity_2', 'filter_entity_3']
EXCEL_DB_MAP = {
    "manufacturer_name": "manufacturer_fk",
    "scene": "scene_id",
    "sub_category": "sub_category_fk",
    "store": "store_id",
}
COL_FOR_MACRO_LINEAR_CALC = 'gross_len_ign_stack'
ROUNDING_DIGITS = 4


class TWEAUToolBox:
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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.calculate_macro_linear()
        self.common.commit_results_data()
        score = 0
        return score

    def calculate_macro_linear(self):
        kpi_sheet = self.get_template_details(KPI_SHEET)
        category_sheet = self.get_template_details(CATEGORY_SHEET)

        for index, kpi_sheet_row in kpi_sheet.iterrows():
            kpi = self.kpi_static_data[(self.kpi_static_data[KPI_FAMILY] == PS_KPI_FAMILY)
                                       & (self.kpi_static_data[TYPE] == kpi_sheet_row[KPI_TYPE])
                                       & (self.kpi_static_data['delete_time'].isnull())]
            if kpi.empty:
                print("KPI Name:{} not found in DB".format(kpi_sheet_row[KPI_NAME]))
            else:
                print("KPI Name:{} found in DB".format(kpi_sheet_row[KPI_NAME]))
                # find the numerator length
                numerator_filters = []
                numerator_filter_string = ''
                # generate the numerator filter string
                for each_filter in NUMERATOR_FILTER_ENTITIES:
                    numerator_filter = kpi_sheet_row[each_filter]
                    if numerator_filter != numerator_filter:
                        # it is NaN ~ it is empty
                        continue
                    # grab the filters anyways to group
                    numerator_filters.append(EXCEL_DB_MAP[numerator_filter])
                    numerator_filter_value = kpi_sheet_row[each_filter + '_value']
                    if numerator_filter_value.lower() == "all":
                        # ignore the filter
                        continue
                    numerator_filter_string += '{key}=="{value}" and '.\
                        format(key=numerator_filter, value=numerator_filter_value)
                numerator_filter_string = numerator_filter_string.rstrip(' and')

                if numerator_filter_string:
                    numerator_data = self.scif.query(numerator_filter_string).fillna(0).\
                        groupby(numerator_filters, as_index=False).agg({COL_FOR_MACRO_LINEAR_CALC: 'sum'})
                else:
                    # nothing to query; group and get all data
                    numerator_data = pd.DataFrame(self.scif.groupby(numerator_filters, as_index=False).
                                                  agg({COL_FOR_MACRO_LINEAR_CALC: 'sum'}))
                # find the denominator length
                denominator_filter_string = ''
                # filter data
                denominator_kpi_df = category_sheet.loc[(category_sheet[KPI_NAME] == kpi_sheet_row[KPI_NAME])]
                index_of_row = denominator_kpi_df.index.values[0]

                temp_denominator_filters = denominator_kpi_df.columns.tolist()
                # grab the filters and map it; remove KPI_NAME from filter items
                temp_denominator_filters.remove(KPI_NAME)
                denominator_filters = [EXCEL_DB_MAP[x] for x in temp_denominator_filters]
                # generate the denominator filter string
                for key, value in denominator_kpi_df.iteritems():
                    if key == KPI_NAME:
                        continue
                    if value[index_of_row].lower() == 'all':
                        continue
                    denominator_filter_string += '{key} == "{value}" and '.format(key=key, value=value[0])
                denominator_filter_string = denominator_filter_string.rstrip(' and')

                if denominator_filter_string:
                    denominator_data = self.scif.query(denominator_filter_string).fillna(0).\
                        groupby(denominator_filters, as_index=False).agg({COL_FOR_MACRO_LINEAR_CALC: 'sum'})
                else:
                    # nothing to query; no grouping; Transform the DataFrame; get all data
                    denominator_data = pd.DataFrame(self.scif.agg({COL_FOR_MACRO_LINEAR_CALC: 'sum'})).T
                for d_idx, denominator_row in denominator_data.iterrows():
                    denominator = denominator_row.get(COL_FOR_MACRO_LINEAR_CALC)
                    for idx, numerator_row in numerator_data.iterrows():
                        numerator = numerator_row.get(COL_FOR_MACRO_LINEAR_CALC)
                        try:
                            result = round(float(numerator) / float(denominator), ROUNDING_DIGITS)
                        except ZeroDivisionError:
                            result = 0
                        numerator_id = int(numerator_row[EXCEL_DB_MAP[kpi_sheet_row.numerator_fk]])
                        denominator_key_str = EXCEL_DB_MAP[kpi_sheet_row.denominator_fk]
                        denominator_id = getattr(self, denominator_key_str, None)
                        if not denominator_id:
                            denominator_id = self.get_denominator_id(denominator_key_str,
                                                                     numerator_row,
                                                                     denominator_row)
                        else:
                            raise Exception("Denominator ID cannot be found. [TWEAU/Utils/KPIToolBox.py]")
                        print "Saving for {kpi_name} with pk={pk}. Numerator={num} & Denominator={den}".format(
                            idx=idx,
                            kpi_name=kpi_sheet_row[KPI_NAME],
                            pk=kpi['pk'],
                            num=numerator_id,
                            den=denominator_id,
                        )
                        self.common.write_to_db_result(fk=int(kpi['pk']),
                                                       numerator_id=numerator_id,
                                                       numerator_result=numerator,
                                                       denominator_id=denominator_id,
                                                       denominator_result=denominator,
                                                       result=result,
                                                       score=result,
                                                       identifier_result=kpi_sheet_row[KPI_NAME],
                                                       should_enter=True,
                                                       )

    def get_denominator_id(self, denominator_key_str, numerator_row, denominator_row):
        """

        :param denominator_key_str: str
        :param numerator_row: pd.Dataframe
        :param denominator_row: pd.Dataframe
        :return: int
                # first check in denominator
                # second check in numerator
                # third check in self.scif
        >> always return one integer denominator_id
        """

        denominator_data = denominator_row.get(denominator_key_str,
                                               numerator_row.get(denominator_key_str,
                                                                 self.scif.get(denominator_key_str)))
        one_and_only_denominator_id = denominator_data.drop_duplicates()[0]
        return one_and_only_denominator_id

    def get_template_details(self, sheet_name):
        template = pd.read_excel(self.excel_file_path, sheetname=sheet_name)
        return template
