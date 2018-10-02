from Trax.Algo.Calculations.Core.DataProvider import Data
# from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
# from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.DB.Common import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
from Projects.CCBOTTLERSUS_SAND.SOVI.Fetcher import SQLQueries
import pandas as pd

__author__ = 'huntery'


class SOVIToolBox:

    def __init__(self, data_provider, output, common_db2):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.common_v2 = common_db2
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
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.sos = SOS(self.data_provider, self.output)
        self.rds_conn = AwsProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.last_pk = self.get_last_pk()

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.remove_empties_from_scif()
        self.calculate_entire_store_sos()

    def calculate_entire_store_sos(self):
        general_filters = {}  # entire session/store visit
        sos_filters = {'United Deliver': 'Y'}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('Entire store: {}%'.format(sos_value))

        # this assumes that template groups where United does NOT have products should NOT be shown
        united_df = self.scif[self.scif['United Deliver'] == 'Y']
        template_group_list = united_df.template_group.unique()

        numerator_result = united_df.facings.sum()
        denominator_result = denominator_result = self.scif.facings.sum()

        self.common_v2.write_to_db_result(3000, numerator_id=self.store_id, numerator_result=numerator_result,
                                          result=sos_value,
                                          denominator_id=999, denominator_result=denominator_result, score=sos_value,
                                          score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=None)

        self.last_pk = self.last_pk + 1
        own_pk = self.last_pk

        for template_group in template_group_list:
            self.calculate_template_group_sos(template_group, own_pk)

    def calculate_template_group_sos(self, template_group, parent_pk):
        general_filters = {}  # entire session/store visit
        sos_filters = {'United Deliver': 'Y',
                       'template_group': template_group}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{}: {}%'.format(template_group, sos_value))

        template_group_df = self.scif[(self.scif['United Deliver'] == 'Y') &
                                      (self.scif['template_group'] == template_group)]
        att4_list = template_group_df.att4.unique()
        template_group_id = template_group_df.template_fk.unique()[0]

        numerator_result = template_group_df.facings.sum()
        denominator_result = self.scif.facings.sum()

        self.common_v2.write_to_db_result(3001, numerator_id=template_group_id, numerator_result=numerator_result,
                                          result=sos_value,
                                          denominator_id=self.store_id, denominator_result=denominator_result,
                                          score=sos_value, score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=parent_pk)

        self.last_pk = self.last_pk + 1
        own_pk = self.last_pk

        for att4 in att4_list:
            self.calculate_att4_sos(template_group, att4, own_pk)

    def calculate_att4_sos(self, template_group, att4, parent_pk):
        general_filters = {}  # entire session/store visit
        sos_filters = {'United Deliver': 'Y',
                       'template_group': template_group,
                       'att4': att4
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {}: {}%'.format(template_group, att4, sos_value))

        att4_df = self.scif[(self.scif['United Deliver'] == 'Y') &
                            (self.scif['template_group'] == template_group) &
                            (self.scif['att4'] == att4)]
        category_list = att4_df.category.unique()
        template_group_id = att4_df.template_fk.unique()[0]

        numerator_result = att4_df.facings.sum()
        denominator_result = self.scif.facings.sum()

        self.common_v2.write_to_db_result(3002, numerator_id=999, numerator_result=numerator_result, result=sos_value,
                                          denominator_id=template_group_id, denominator_result=denominator_result,
                                          score=sos_value, score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=parent_pk)

        self.last_pk = self.last_pk + 1
        own_pk = self.last_pk

        for category in category_list:
            self.calculate_category_sos(template_group, att4, category, own_pk)

    def calculate_category_sos(self, template_group, att4, category, parent_pk):
        general_filters = {}
        sos_filters = {'United Deliver': 'Y',
                       'template_group': template_group,
                       'att4': att4,
                       'category': category
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {}: {}%'.format(template_group, att4, category, sos_value))

        category_df = self.scif[(self.scif['United Deliver'] == 'Y') &
                                (self.scif['template_group'] == template_group) &
                                (self.scif['att4'] == att4) &
                                (self.scif['category'] == category)]

        manufacturer_list = category_df.manufacturer_name.unique()

        category_id = category_df.category_fk.unique()[0]

        numerator_result = category_df.facings.sum()
        denominator_result = self.scif.facings.sum()

        self.common_v2.write_to_db_result(3003, numerator_id=category_id, numerator_result=numerator_result,
                                          result=sos_value,
                                          denominator_id=999, denominator_result=denominator_result, score=sos_value,
                                          score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=parent_pk)

        self.last_pk = self.last_pk + 1
        own_pk = self.last_pk

        for manufacturer_name in manufacturer_list:
            self.calculate_manufacturer_sos(template_group, att4, category, manufacturer_name, own_pk)

    def calculate_manufacturer_sos(self, template_group, att4, category, manufacturer_name, parent_pk):
        general_filters = {
            'template_group': template_group,
            'att4': att4,
            'category': category
        }
        sos_filters = {'manufacturer_name': manufacturer_name}
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name, sos_value))

        manufacturer_df = self.scif[(self.scif['template_group'] == template_group) &
                                    (self.scif['att4'] == att4) &
                                    (self.scif['category'] == category) &
                                    (self.scif['manufacturer_name'] == manufacturer_name)]

        brand_name_list = manufacturer_df.brand_name.unique()
        category_id = manufacturer_df.category_fk.unique()[0]
        manufacturer_id = manufacturer_df.manufacturer_fk.unique()[0]

        numerator_result = manufacturer_df.facings.sum()
        denominator_result = self.apply_filters_to_df(self.scif, general_filters).facings.sum()

        self.common_v2.write_to_db_result(3004, numerator_id=manufacturer_id, numerator_result=numerator_result,
                                          result=sos_value,
                                          denominator_id=category_id, denominator_result=denominator_result,
                                          score=sos_value, score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=parent_pk)

        self.last_pk = self.last_pk + 1
        own_pk = self.last_pk

        for brand_name in brand_name_list:
            self.calculate_brand_sos(template_group, att4, category, manufacturer_name, brand_name, own_pk)

    def calculate_brand_sos(self, template_group, att4, category, manufacturer_name, brand_name, parent_pk):
        general_filters = {
            'template_group': template_group,
            'att4': att4,
            'category': category
        }
        sos_filters = {'manufacturer_name': manufacturer_name,
                       'brand_name': brand_name
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)
        print('{} - {} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name,
                                                   brand_name, sos_value))

        brand_df = self.scif[(self.scif['template_group'] == template_group) &
                             (self.scif['att4'] == att4) &
                             (self.scif['category'] == category) &
                             (self.scif['manufacturer_name'] == manufacturer_name) &
                             (self.scif['brand_name'] == brand_name) &
                             (self.scif['product_type'] != 'Empty')]

        product_name_list = brand_df.product_name.unique()
        brand_id = brand_df.brand_fk.unique()[0]
        manufacturer_id = brand_df.manufacturer_fk.unique()[0]

        numerator_result = brand_df.facings.sum()
        denominator_result = self.apply_filters_to_df(self.scif, general_filters).facings.sum()

        self.common_v2.write_to_db_result(3005, numerator_id=brand_id, numerator_result=numerator_result,
                                          result=sos_value,
                                          denominator_id=manufacturer_id, denominator_result=denominator_result,
                                          score=sos_value, score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=parent_pk)

        self.last_pk = self.last_pk + 1
        own_pk = self.last_pk

        for product_name in product_name_list:
            self.calculate_product_name_sos(template_group, att4, category, manufacturer_name, brand_name, product_name,
                                            own_pk)

    def calculate_product_name_sos(self, template_group, att4, category, manufacturer_name, brand_name, product_name,
                                   parent_pk):
        general_filters = {
            'template_group': template_group,
            'att4': att4,
            'category': category
        }
        sos_filters = {'manufacturer_name': manufacturer_name,
                       'brand_name': brand_name,
                       'product_name': product_name
                       }
        sos_value = self.sos.calculate_share_of_shelf(sos_filters, **general_filters)
        sos_value *= 100
        sos_value = round(sos_value, 2)

        product_df = self.scif[(self.scif['template_group'] == template_group) &
                               (self.scif['att4'] == att4) &
                               (self.scif['category'] == category) &
                               (self.scif['manufacturer_name'] == manufacturer_name) &
                               (self.scif['brand_name'] == brand_name) &
                               (self.scif['product_type'] != 'Empty') &
                               (self.scif['product_name'] == product_name)]

        product_id = product_df.product_fk.unique()[0]
        brand_id = product_df.brand_fk.unique()[0]

        numerator_result = product_df.facings.sum()
        denominator_result = self.apply_filters_to_df(self.scif, general_filters).facings.sum()

        self.common_v2.write_to_db_result(3006, numerator_id=product_id, numerator_result=numerator_result,
                                          result=sos_value,
                                          denominator_id=brand_id, denominator_result=denominator_result,
                                          score=sos_value, score_after_actions=sos_value,
                                          denominator_result_after_actions=None, numerator_result_after_actions=0,
                                          weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=parent_pk)

        self.last_pk = self.last_pk + 1
        print self.last_pk

        print('{} - {} - {} - {} - {} - {}: {}%'.format(template_group, att4, category, manufacturer_name,
                                                        brand_name, product_name.encode('utf-8'), sos_value))

    # def calculate_share_of_shelf(self, sos_filters=None, include_empty=EXCLUDE_EMPTY, **general_filters):
    #     """
    #     :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
    #     :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
    #     :param general_filters: These are the parameters which the general data frame is filtered by.
    #     :return: The ratio of the Facings SOS.
    #     """
    #     if include_empty == self.EXCLUDE_EMPTY and 'product_type' not in sos_filters.keys() + general_filters.keys():
    #         general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
    #     pop_filter = self.toolbox.get_filter_condition(self.scif, **general_filters)
    #     subset_filter = self.toolbox.get_filter_condition(self.scif, **sos_filters)
    #
    #     try:
    #         ratio = self.k_engine.calculate_sos_by_facings(pop_filter=pop_filter, subset_filter=subset_filter)
    #     except Exception as e:
    #         Log.error(e.message)
    #         ratio = 0
    #
    #     if not isinstance(ratio, (float, int)):
    #         ratio = 0
    #     return ratio

    def get_last_pk(self):
        query = SQLQueries.get_kpi_level_2_results_max_pk()
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df['MAX(pk)'][0]

    def remove_empties_from_scif(self):
        self.scif = self.scif[self.scif['product_type'] != 'Empty']

    @staticmethod
    def apply_filters_to_df(df, filters):
        for k, v in filters.iteritems():
            df = df[df[k] == v]
        return df

    def commit_results(self):
        self.common_v2.commit_results_data()
