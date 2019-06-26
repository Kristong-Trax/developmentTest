import pandas as pd

from Projects.RINIELSENUS.Utils.Const import SET_CATEGORIES, FILTER_NAMING_DICT, \
    SET_PRE_CALC_CHECKS, DOG_MAIN_MEAL_WET, SPT_DOG_TREATS, SPT_CAT_TREATS, \
    SPT_CAT_MAIN_MEAL, SPT_DOG_MAIN_MEAL, CAT_TREATS, CAT_MAIN_MEAL_DRY, \
    CAT_MAIN_MEAL_WET, DOG_MAIN_MEAL_DRY, DOG_TREATS, BDB_RETAILERS, BDB_CHANNELS, SPT_RETAILERS, \
    SPT_CHANNELS  # SPT_DOG_TREATS_Q1_2018, SPT_CAT_TREATS_Q1_2018, SPT_CAT_MAIN_MEAL_Q1_2018, SPT_DOG_MAIN_MEAL_Q1_2018

from Projects.RINIELSENUS.Utils.Fetcher import MarsUsQueries
from Projects.RINIELSENUS.Utils.GeneralToolBox import MarsUsGENERALToolBox
from Projects.RINIELSENUS.Utils.Loader import Definition
from Projects.RINIELSENUS.Utils.ParseTemplates import ParseMarsUsTemplates, KPIConsts
from Projects.RINIELSENUS.Utils.Runner import Results
from Projects.RINIELSENUS.Utils.Writer import KpiResultsWriter as KpiResultsWriter
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Projects.RINIELSENUS.Utils.Fake_Common import NotCommon as Common
from Trax.Utils.Logging.Logger import Log

__author__ = 'nethanel'


class MarsUsDogMainMealWet(object):
    def __init__(self, data_provider, output):
        self._data_provider = data_provider
        self.common = Common(self._data_provider)
        self.mpip_sr = self.common.read_custom_query(MarsUsQueries.get_updated_mvp_sr())
        self.project_name = self._data_provider.project_name
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self._output = output
        self._tools = MarsUsGENERALToolBox(self._data_provider, self._output, ignore_stacking=True)
        self._template = ParseMarsUsTemplates()
        self._writer = self._get_writer()
        self.store_id = self._data_provider[Data.STORE_FK]
        self._data_provider.channel = self.get_store_att17(self.store_id)
        self._data_provider.retailer = self.get_store_retailer(self.store_id)
        self._data_provider.probe_groups = self.get_probe_group(self._data_provider.session_uid)
        self.store_type = data_provider.store_type
        self.rds_conn.disconnect_rds()
        self._data_provider.trace_container = pd.DataFrame(columns=['kpi_display_text', 'scene_id',
                                                     'products&brands', 'allowed_products', 'kpi_pass'])

    def get_store_att17(self, store_fk):
        query = MarsUsQueries.get_store_attribute(17, store_fk)
        att15 = pd.read_sql_query(query, self.rds_conn.db)
        return att15.values[0][0]

    def get_store_att8(self, store_fk):
        query = MarsUsQueries.get_store_attribute(8, store_fk)
        att10 = pd.read_sql_query(query, self.rds_conn.db)
        return att10.values[0][0]

    def get_store_retailer(self, store_fk):
        query = MarsUsQueries.get_store_retailer(store_fk)
        att10 = pd.read_sql_query(query, self.rds_conn.db)
        return att10.values[0][0]

    def get_probe_group(self, session_uid):
        query = MarsUsQueries.get_probe_group(session_uid)
        probe_group = pd.read_sql_query(query, self.rds_conn.db)
        return probe_group

    @property
    def _get_store_channel(self):
        return self._data_provider.channel

    @property
    def _get_store_type(self):
        return self._data_provider.store_type

    @property
    def _get_retailer_name(self):
        return self._data_provider.retailer

    def _get_writer(self):
        return KpiResultsWriter(session_uid=self._data_provider.session_uid,
                                project_name=self._data_provider.project_name,
                                store_id=self._data_provider[Data.STORE_FK],
                                visit_date=self._data_provider[Data.VISIT_DATE])

    def calculate_scores(self):
        """
        This function calculates the KPI results.
        """
        if not self.is_relevant_retailer_channel():
            Log.warning('retailer: {} and channel: {} are are not relevant'.format(self._get_retailer_name,
                                                                                   self._get_store_channel))
            self._writer.commit_results_data()
            return

        # if self._is_pet_food_category_excluded():
        #     Log.warning('pet food category does not exists or it was excluded by decision unit')
        #     self._writer.commit_results_data()
        #     return

        # template SPT
        if self.is_relevant_retailer_channel_spt():
            set_names = self._get_set_spt_names()
            for set_name in set_names:
                if self._is_session_irrelevant_for_set(set_name):
                    Log.info('Skipping set: {}'.format(set_name))
                    continue
                Log.info('Starting set: {}'.format(set_name))
                if not self.retailer_channel_has_spt_sales_data(set_name):
                    Log.warning('no sales data for retailer: {},'
                                ' channel: {}, set: {}'.format(self._get_retailer_name,
                                                               self._get_store_channel,
                                                               set_name))

                template_data = self._template.parse_template(set_name, 0)
                hierarchy = Definition(template_data, self._get_store_channel, self._get_retailer_name,self._get_store_type).get_atomic_hierarchy_and_filters(set_name)
                preferred_range = template_data[KPIConsts.PREFERRED_RANGE_SHEET]
                Results(self._tools, self._data_provider, self.mpip_sr, self.common, self._writer,
                        preferred_range[preferred_range['Set name'] == set_name]).calculate(hierarchy)

        # template BDB
        if self.is_relevant_retailer_channel_bdb():
            set_names = self._get_set_names()
            for set_name in set_names:
                if self._is_session_irrelevant_for_set(set_name):
                    Log.info('Skipping set: {}'.format(set_name))
                    continue
                Log.info('Starting set: {}'.format(set_name))
                if not self.retailer_channel_has_sales_data(set_name):
                    Log.warning('no sales data for retailer: {},'
                                ' channel: {}, set: {}'.format(self._get_retailer_name,
                                                               self._get_store_channel,
                                                               set_name))

                template_data = self._template.parse_template(set_name, 1)
                hierarchy = Definition(template_data, self._get_store_channel, self._get_retailer_name,self._get_store_type).get_atomic_hierarchy_and_filters(set_name)
                preferred_range = template_data[KPIConsts.PREFERRED_RANGE_SHEET]
                Results(self._tools, self._data_provider, self.mpip_sr, self.common, self._writer,
                        preferred_range[preferred_range['Set name'] == set_name]).calculate(hierarchy)

        # self._data_provider.trace_container.to_csv('/home/Israel/Desktop/trace_block.csv')
        self._writer.commit_results_data()
        self.common.commit_results_data()

    @staticmethod
    def _get_set_names():
        return [
            DOG_MAIN_MEAL_DRY,
            DOG_MAIN_MEAL_WET,
            CAT_TREATS,
            CAT_MAIN_MEAL_DRY,
            CAT_MAIN_MEAL_WET,
            DOG_TREATS
        ]

    @staticmethod
    def _get_set_spt_names():
        # return [
        #     SPT_DOG_TREATS_Q1_2018,
        #     SPT_CAT_TREATS_Q1_2018,
        #     SPT_CAT_MAIN_MEAL_Q1_2018,
        #     SPT_DOG_MAIN_MEAL_Q1_2018
        # ]
        return [
            SPT_DOG_TREATS,
            SPT_CAT_TREATS,
            SPT_CAT_MAIN_MEAL,
            SPT_DOG_MAIN_MEAL
        ]

    def _is_pet_food_category_excluded(self):
        category_fk = 13
        decision_unit = self._data_provider._decision_unit_data_provider
        session_category = decision_unit._session_categories_status
        if session_category[session_category['category_fk'] == category_fk]['exclude_status_fk'].empty:
            return True
        category_excluded = \
            session_category[session_category['category_fk'] == category_fk]['exclude_status_fk'].fillna(1).iloc[0] != 1
        session_excluded = decision_unit._session_exclude_status['exclude_status_fk'].fillna(1).iloc[0] != 1
        return category_excluded | session_excluded

    def _is_session_irrelevant_for_set(self, set_name):
        matches = self._tools.match_product_in_scene.copy()
        relevant_filters = SET_PRE_CALC_CHECKS[set_name]
        filtered_matches = matches[self._tools.get_filter_condition(matches, **relevant_filters)]
        if filtered_matches.empty:
            return True
        return False

    @staticmethod
    def _get_set_category_fk(set_name):
        return SET_CATEGORIES[set_name]

    def check_template(self):
        set_names = self._get_set_names()
        suspicious = {'kpis with empty filters': [], 'filters with no data': {}, 'kpi with no filters': []}
        for set_name in set_names:
            template_data = self._template.parse_template(set_name)
            hierarchy = Definition(template_data, self._get_store_channel).get_atomic_hierarchy_and_filters(set_name)
            product = self._data_provider.all_products
            for kpi in hierarchy:
                if kpi.has_key('filters'):
                    fil = kpi['filters']
                    if len(fil) == 0:
                        suspicious['kpis with empty filters'].append(
                            'set {} atomic {} filters are empty'.format(set_name, kpi['atomic']))
                    for key, value in fil.iteritems():
                        if key == 'order by':
                            continue
                        key = self.split_filter(key)
                        key = self.rename_filter(key)
                        if isinstance(value, tuple):
                            value = value[0]
                        Log.info('set {} atomic {}, checking {} in {}'.format(set_name, kpi['atomic'], value, key))
                        pp = product[product[key].isin(value)]
                        if len(pp) == 0:
                            suspicious['filters with no data'].setdefault(key, set()).update(value)
                else:
                    suspicious['kpi with no filters'].append(
                        'set {} kpi {} has no filters'.format(set_name, kpi['atomic']))
        return suspicious

    @staticmethod
    def split_filter(key):
        key_list = key.split(';')
        return key_list[0]

    @staticmethod
    def rename_filter(filter_):
        if FILTER_NAMING_DICT.has_key(filter_):
            return FILTER_NAMING_DICT[filter_]
        else:
            return filter_

    def retailer_channel_has_sales_data(self, set_name):
        sales = ParseMarsUsTemplates().get_mars_sales_data()
        sales = sales[(sales['retailer'] == self._data_provider.retailer) &
                      (sales['channel'] == self._data_provider.channel) &
                      (sales['set'] == set_name.upper())]
        if sales.empty:
            return False
        else:
            return True

    def retailer_channel_has_spt_sales_data(self, set_name):
        sales = ParseMarsUsTemplates().get_mars_spt_sales_data()
        sales = sales[(sales['retailer'] == self._data_provider.retailer) &
                      (sales['channel'] == self._data_provider.channel) &
                      (sales['set'] == set_name.upper())]
        if sales.empty:
            return False
        else:
            return True

    def is_relevant_retailer_channel(self):
        is_relevant_retailer = self._get_retailer_name in BDB_RETAILERS + SPT_RETAILERS
        is_relevant_channel = self._get_store_channel in BDB_CHANNELS + SPT_CHANNELS
        return is_relevant_retailer and is_relevant_channel

    def is_relevant_retailer_channel_bdb(self):
        is_relevant_retailer = self._get_retailer_name in BDB_RETAILERS
        is_relevant_channel = self._get_store_channel in BDB_CHANNELS
        return is_relevant_retailer and is_relevant_channel

    def is_relevant_retailer_channel_spt(self):
        is_relevant_retailer = self._get_retailer_name in SPT_RETAILERS
        is_relevant_channel = self._get_store_channel in SPT_CHANNELS
        return is_relevant_retailer and is_relevant_channel