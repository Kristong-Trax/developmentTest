
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
from KPIUtils.ParseTemplates import parse_template
from KPIUtils_v2.DB.CommonV2 import Common
from Projects.CBCDAIRYIL.Utils.Consts import Consts
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'idanr'


class CBCDAIRYILToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.common = Common(self.data_provider)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.session_uid = self.data_provider.session_uid
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.session_fk = self.session_info['pk'][0]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.kpis_data = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_SHEET, lower_headers_row_index=1)
        self.kpi_weights = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_WEIGHT, lower_headers_row_index=0)
        self.gap_data = parse_template(Consts.TEMPLATE_PATH, Consts.KPI_GAP, lower_headers_row_index=0)
        self.template_data = self.encode_template_data()
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_additional_attr_1 = self.get_store_attribute(Consts.ADDITIONAL_ATTRIBUTE_1)
        self.store_type = self.get_store_attribute(Consts.STORE_TYPE)



        # self.match_display_in_scene = self.get_match_display()
        # self.match_stores_by_retailer = self.get_match_stores_by_retailer()
        # self.match_template_fk_by_category_fk = self.get_template_fk_by_category_fk()
        # self.tools = CBCILCBCIL_GENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        # self.kpi_static_data = self.get_kpi_static_data()
        # self.kpi_results_queries = []
        # self.gaps = pd.DataFrame(columns=[self.KPI_NAME, self.KPI_ATOMIC_NAME, self.GAPS])
        # self.gaps_queries = []
        # self.cbcil_id = self.get_own_manufacturer_pk()

    def get_store_attribute(self, attribute_name):
        """
        This function encodes and returns the relevant store attribute.
        :param attribute_name: Attribute name from the data_provider[Data.STORE_INFO].
        :return: The relevant attribute with encode (if necessary).
        """
        store_att = self.store_info.at[0, attribute_name]
        if not store_att:
            Log.warning("The store attribute {} doesn't exist for store fk = {}".format(attribute_name, self.store_id))
            return None
        if isinstance(store_att, (unicode, str)):
            store_att = store_att.encode('utf-8')
        return store_att

    def encode_template_data(self):
        """
        This function responsible to handle the encoding issue we have in the template because of the Hebrew language.
        :return: Same template data with encoding.
        """
        encoding_data = self.kpis_data[(self.kpis_data[Consts.STORE_TYPE].str.encode('utf-8').isin(self.store_type)) &
                                       (self.kpis_data[Consts.ADDITIONAL_ATTRIBUTE_1].str.encode('utf-8').isin(
                                           self.store_additional_attr_1))]
        return encoding_data

    def get_relevant_kpis_for_calculation(self):
        """
        This function retrieve the relevant KPIs to calculate from the template
        :return: A tuple: (set_name, [kpi1, kpi2, kpi3...]) to calculate.
        """
        kpi_set = self.template_data[Consts.KPI_SET].values[0]
        kpi = self.template_data[self.template_data[Consts.KPI_SET].str.encode('utf-8') ==
                                 kpi_set.encode('utf-8')][Consts.KPI_NAME].unique()
        return kpi_set, kpi

    def get_atomics_to_calculate(self, kpi_name):
        """

        :param kpi_name:
        :return:
        """
        atomics = self.template_data[self.template_data[Consts.KPI_NAME].str.encode('utf-8') ==
                                     kpi_name.encode('utf-8')]
        return atomics

    def get_relevant_scenes_by_params(self, template_names, template_groups):
        """
        This function returns the relevant scene_fks to calculate
        :param template_names: The relevant scene type from the template.
        :param template_groups: The relevant template group from the template.
        :return: List of scene fks.
        """
        filtered_scif = self.scif[['scene_fk', 'template_name', 'template_group']]
        if template_names:
            filtered_scif = filtered_scif[filtered_scif['template_name'].isin(template_names)]
        if template_groups:
            filtered_scif = filtered_scif[filtered_scif['template_group'].isin(template_groups)]

        return filtered_scif[Consts.SCENE_FK].unique().tolist()

    def get_general_filters(self, params):
        """
        This function returns the relevant KPI filters according to the template.
        :param params: The Atomic KPI row in the template
        :return:
        """
        template_name = params[Consts.TEMPLATE_NAME].split(Consts.SEPARATOR)
        template_group = params[Consts.TEMPLATE_GROUP].split(Consts.SEPARATOR)
        relevant_scenes = self.get_relevant_scenes_by_params(template_name, template_group)
        params1 = params2 = params3 = []
        if params[Consts.PARAMS_VALUE_2]:
            params1 = map(unicode.strip, params[Consts.PARAMS_VALUE_1].split(','))
        if params[Consts.PARAMS_VALUE_2]:
            params2 = map(float, params[Consts.PARAMS_VALUE_2].split(','))
        if params[Consts.PARAMS_VALUE_3]:
            params3 = map(float, params[Consts.PARAMS_VALUE_3].split(','))

        result = {Consts.TARGET: params[Consts.TARGET],
                  Consts.SPLIT_SCORE: params[Consts.SPLIT_SCORE],
                  Consts.SCENE_ID: relevant_scenes,
                  'filters': {
                      '1': {params[Consts.PARAMS_TYPE_1]: params1},
                      '2': {params[Consts.PARAMS_TYPE_2]: params2},
                      '3': {params[Consts.PARAMS_TYPE_3]: params3},
                  }
                  }
        return result

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        if self.template_data.empty:
            Log.warning("The template data is empty! Exiting...")
            return
        kpi_set, kpis_list = self.get_relevant_kpis_for_calculation()
        for kpi in kpis_list:
            atomics_df = self.get_atomics_to_calculate(kpi)
            for i in atomics_df.index:
                kpi_type = atomics_df.at[i, Consts.KPI_TYPE]        # TODO: CHECK FOR SINGLE ATOMIC
                general_filters = self.get_general_filters(atomics_df.iloc[i])







        score = 0
        return score


