from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
# from Trax.Utils.Logging.Logger import Log
import pandas as pd
import ast
from collections import OrderedDict
import pandas as pd
import simplejson
import re
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Const import Consts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AMER.CalculationsUtils.CalculationUtils import Eyelight
from KPIUtils_v2.Calculations.AMER.BlockAdjacencyCalculations import BlockAdjacency
import collections
import json
import numpy

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations
# from _mysql_exceptions import ProgrammingError
# from datetime import datetime
import math

__author__ = 'Nicolas Keeton'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
STATIC_ATOMIC = 'static.atomic_kpi'

# IN DB
MANUFACTUR = 'Manufacturer'
BRAND = 'Brand'
SUB_BRAND = 'Sub Brand'
SUB_CATEGORY = 'Sub Category'
CATEGORY = 'Category'
# SUBSEGMENT_KPI = 'Subsegment'
# SUBSEGMENT_SET = 'Purina- Subsegment'
# PRICE_SET = 'Purina- Price'
PRICE_KPI = 'Price Class'  # to be written as it is on the database

# In SCIF
# SCIF_SUBSEGMENT = 'Nestle_Purina_Subsegment'  # to be written as it is on the database
SCIF_SUB_CATEOGRY = 'Nestle_Purina_Sub-category'
# SCIF_SUB_BRAND = 'Nestle_Purina_Subbrand'
SCIF_PRICE = 'Nestle_Purina_Price_Class'
SCIF_CATEOGRY = 'Nestle_Purina_Category'
LINEAR_SIZE = u'gross_len_add_stack'
# gross_len_ign_stack
# gross_len_split_stack
# gross_len_add_stack
PURINA_KPI = [MANUFACTUR, BRAND, SUB_CATEGORY, CATEGORY, PRICE_KPI]
PET_FOOD_CATEGORY = 13
PURINA_SET = 'Purina'
OTHER = 'OTHER'


class ColdCutToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.block = Block(data_provider)
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.session_fk = self.session_info['pk'].values[0]
        self.kpi_results_queries = []
        self.kpi_static_queries = []
        self.own_manufacturer_fk = int(self.data_provider.own_manufacturer.param_value.values[0])

        self.adjacency = BlockAdjacency(self.data_provider, ps_data_provider=self.ps_data_provider, common=self.common,
                                        rds_conn=self.rds_conn)
        self.eyelight = Eyelight(self.data_provider, self.common, self.ps_data_provider)
        self.merged_scif_mpis = self.match_product_in_scene.merge(self.scif, how='left',
                                                                  left_on=['scene_fk', 'product_fk'],
                                                                  right_on=['scene_fk', 'product_fk'])
        # self.purina_scif = self.scif.loc[self.scif['category_fk'] == PET_FOOD_CATEGORY]
        self.targets = self.ps_data_provider.get_kpi_external_targets(
            key_fields=["KPI Type", "Location: JSON", "Config Params: JSON", "Dataset 1: JSON", "Dataset 2: JSON"])
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])
        self.custom_entity_table = self.get_kpi_custom_entity_table()

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        relevant_kpi_types = [
            # Consts.SOS,
            # Consts.SCENE_LOCATION,
            # Consts.HORIZONTAL_SHELF_POSITION,
            Consts.VERTICAL_SHELF_POSITION,
            # Consts.BLOCKING,
            # Consts.BLOCK_ADJ,
            # Consts.BLOCKING_ORIENTATION
            # Consts.BAY_POSITION, Consts.DIAMOND_POSITION
        ]

        targets = self.targets[self.targets[Consts.ACTUAL_TYPE].isin(relevant_kpi_types)]
        self._calculate_kpis_from_template(targets)
        self.save_results_to_db()
        pass

    def calculate_blocking(self, row, df):
        addtional_data = row['Config Params: JSON']
        location_data = row['Location: JSON']
        kpi_fk = row['kpi_fk']
        # population_data = {'population': row['Dataset 1: JSON']}
        population_data = row['Dataset 1: JSON']['include'][0]
        df.rename(columns={'pk': 'custom_entity_fk'}, inplace=True)
        result_dict_list = self._logic_for_blocking(kpi_fk, population_data, location_data, addtional_data)
        # if dataframe empty do we save the kpi?

        return result_dict_list

    def calculate_blocking_adj(self, row, df):
        additional_data = row['Config Params: JSON']
        location_data = row['Location: JSON']
        kpi_fk = row['kpi_fk']
        anchor_data = row['Dataset 1: JSON']['include'][0]
        target_data = row['Dataset 2: JSON']['include'][0]

        df.rename(columns={'pk': 'custom_entity_fk'}, inplace=True)
        is_adjacent_result = self._logic_for_adj(kpi_fk, anchor_data, target_data, location_data, additional_data)
        # if dataframe empty do we save the kpi?
        result = 0

        if is_adjacent_result:
            result = 1

        result_dict = {'kpi_fk': kpi_fk,
                       'numerator_id': self.own_manufacturer_fk,
                       'denominator_id': self.store_id,
                       # 'denominator_result': 1,
                       'result': result,
                       'score': 0}
        return result_dict

    def _logic_for_adj(self, kpi_fk, anchor_data, target_data, location_data, additional_data):
        result = self.adjacency.evaluate_block_adjacency(anchor_data, target_data, location=location_data,
                                                         additional=additional_data, kpi_fk=kpi_fk)

        return result

    def _logic_for_blocking(self, kpi_fk, population_data, location_data, addtional_data):
        result_dict_list = []
        block = self.block.network_x_block_together(population=population_data, location=location_data,
                                                    additional=addtional_data)

        for row in block.itertuples():
            scene_match_fks = list(row.cluster.nodes[list(row.cluster.nodes())[0]]['scene_match_fk'])
            self.eyelight.write_eyelight_result(scene_match_fks, kpi_fk)
        passed_block = block[block.is_block.isin([True])]

        numerator_result = 0
        result_value = "No"
        if passed_block.empty:
            pass
        else:
            numerator_result = 1
            result_value = "Yes"

        result = Consts.CUSTOM_RESULT[result_value]
        # numerator_id = df.custom_entity_fk.iloc[0]

        result_dict = {'kpi_fk': kpi_fk,
                       'numerator_id': self.own_manufacturer_fk, 'numerator_result': numerator_result,
                       'denominator_id': self.store_id,
                       'denominator_result': 1,
                       'result': result,
                       'score': 0}

        result_dict_list.append(result_dict)
        return result_dict_list

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        return output

    def calculate_blocking_orientation(self):
        pass

    def calculate_vertical_position(self, row, df):
        result_dict_list = []
        mpis = df  # get this from the external target filter_df method thingy
        scene_facings_df = mpis.groupby(['scene_fk', 'product_fk'], as_index=False)['facings'].max()
        scene_facings_df.rename(columns={'facings': 'scene_facings'}, inplace=True)
        shelf_df = mpis.groupby(['scene_fk', 'product_fk'], as_index=False)['shelf_number_from_bottom'].max()
        shelf_df.rename(columns={'shelf_number_from_bottom': 'shelf_count'}, inplace=True)

        pre_sort_mpis = pd.merge(mpis, scene_facings_df, how='left', on=['scene_fk', 'product_fk'])
        scene_facings_df_sorted = pre_sort_mpis.sort_values('scene_facings')
        mpis = scene_facings_df_sorted.drop_duplicates(['scene_fk', 'product_fk'], keep="last")

        mpis = pd.merge(mpis, shelf_df, how='left', on=['scene_fk', 'product_fk'])

        mpis['position'] = mpis.apply(self._calculate_vertical_position, axis=1)
        mpis['result_type_fk'] = mpis['position'].apply(lambda x: Consts.CUSTOM_RESULT.get(x, 0))
        mpis = mpis.groupby(['product_fk'], as_index=False)['result_type_fk'].agg(pd.Series.mode)

        for result in mpis.itertuples():
            custom_fk_result = result.result_type_fk

            if type(custom_fk_result) == numpy.ndarray:
                custom_fk_result = result.result_type_fk[0]

            result_item = {'kpi_fk': row.kpi_fk,
                           'numerator_id': result.product_fk, 'numerator_result': 1,
                           'denominator_id': self.store_id,
                           'denominator_result': 1,
                           'result': custom_fk_result,
                           'score': 0}

            result_dict_list.append(result_item)
        return result_dict_list

    def calculate_horizontal_position(self, row, df):
        result_dict_list = []
        mpis = df  # get this from the external target filter_df method thingy
        bay_df = mpis.groupby('scene_fk', as_index=False)['bay_number'].max()
        bay_df.rename(columns={'bay_number': 'bay_count'}, inplace=True)
        mpis = pd.merge(mpis, bay_df, how='left', on='scene_fk')
        mpis['position'] = mpis.apply(self._calculate_horizontal_position, axis=1)
        mpis['result_type_fk'] = mpis['position'].apply(lambda x: Consts.CUSTOM_RESULT.get(x, 0))
        mpis = mpis.groupby(['product_fk'], as_index=False)['result_type_fk'].agg(pd.Series.mode)

        for result in mpis.itertuples():
            custom_fk_result = result.result_type_fk

            if type(custom_fk_result) == numpy.ndarray:
                custom_fk_result = result.result_type_fk[0]

            result_item = {'kpi_fk': row.kpi_fk,
                           'numerator_id': result.product_fk, 'numerator_result': 1,
                           'denominator_id': self.store_id,
                           'denominator_result': 1,
                           'result': custom_fk_result,
                           'score': 0}

            result_dict_list.append(result_item)
        return result_dict_list

    @staticmethod
    def _calculate_horizontal_position(row):
        bay_count = row.bay_count
        if bay_count == 1:
            return 'Center'
        factor = round(bay_count / float(3))
        if row.bay_number <= factor:
            return 'Left'
        elif row.bay_number > (bay_count - factor):
            return 'Right'
        return 'Center'

    @staticmethod
    def _calculate_vertical_position(row):
        shelf_number = str(row.shelf_number_from_bottom)
        shelf_count = str(row.shelf_count)

        shelf_count_pos_map = Consts.shelf_map[shelf_count]
        pos_value = shelf_count_pos_map[shelf_number]

        return pos_value

    def calculate_facings_sos(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        df.dropna(subset=[numerator_type], inplace=True)
        result_dict_list = self._logic_for_sos(return_holder, df, numerator_type, denominator_type)
        return result_dict_list

    def _logic_for_sos(self, return_holder, df, numerator_type, denominator_type):
        result_list = []
        for num_item in self.merged_scif_mpis[numerator_type].dropna().unique().tolist():
            numerator_scif = self.merged_scif_mpis[self.merged_scif_mpis[numerator_type] == num_item]
            numerator_result = numerator_scif.facings.sum()
            denominator_result = self.merged_scif_mpis.facings.sum()

            product_fk = numerator_scif['product_fk'].iloc[0]

            sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

            result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                           'numerator_id': product_fk, 'numerator_result': numerator_result,
                           'denominator_id': self.store_id,
                           'denominator_result': denominator_result,
                           'result': sos_value}

            result_list.append(result_dict)
        return result_list

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SOS:
            pass
            # return self.calculate_facings_sos
        elif kpi_type == Consts.HORIZONTAL_SHELF_POSITION:
            return self.calculate_horizontal_position
        elif kpi_type == Consts.VERTICAL_SHELF_POSITION:
            return self.calculate_vertical_position
        # elif kpi_type == Consts.SHELF_POSITION:
        #     return self.calculate_shelf_position
        elif kpi_type == Consts.BLOCKING:
            return self.calculate_blocking
        elif kpi_type == Consts.BLOCK_ADJ:
            return self.calculate_blocking_adj

    def get_denominator(self, denominator_value):
        if denominator_value == 'store_fk':
            return self.store_id

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            try:

                calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.ACTUAL_TYPE])
                row = self.apply_json_parser(row)
                merged_scif_mpis = self._parse_json_filters_to_df(row)
                result_data = calculation_function(row, merged_scif_mpis)
                if result_data and isinstance(result_data, list):
                    for result in result_data:
                        self.results_df.loc[len(self.results_df), result.keys()] = result
                elif result_data and isinstance(result_data, dict):
                    self.results_df.loc[len(self.results_df), result_data.keys()] = result_data
            except:

                pass
                # Print log error for kpi
                # row['kpi_type'] ERROR for this

    def _parse_json_filters_to_df(self, row):
        JSON = row[row.index.str.contains('JSON') & (~ row.index.str.contains('Config Params'))]
        filter_JSON = JSON[~JSON.isnull()]
        filtered_scif_mpis = self.merged_scif_mpis
        for each_JSON in filter_JSON:
            final_JSON = {'population': each_JSON} if ('include' or 'exclude') in each_JSON else each_JSON
            filtered_scif_mpis = ParseInputKPI.filter_df(final_JSON, filtered_scif_mpis)
        if 'include_stacking' in row['Config Params: JSON'].keys():
            including_stacking = row['Config Params: JSON']['include_stacking'][0]
            filtered_scif_mpis[
                Consts.FINAL_FACINGS] = filtered_scif_mpis.facings if including_stacking == 'True' else filtered_scif_mpis.facings_ign_stack
            filtered_scif_mpis = filtered_scif_mpis[filtered_scif_mpis.stacking_layer == 1]
        return filtered_scif_mpis

    def apply_json_parser(self, row):
        json_relevent_rows_with_parse_logic = row[(row.index.str.contains('JSON')) & (row.notnull())].apply(
            self.parse_json_row)
        row = row[~ row.index.isin(json_relevent_rows_with_parse_logic.index)].append(
            json_relevent_rows_with_parse_logic)
        return row

    def parse_json_row(self, item):
        '''
        :param item: improper json value (formatted incorrectly)
        :return: properly formatted json dictionary
        The function will be in conjunction with apply. The function will applied on the row(pandas series). This is
            meant to convert the json comprised of improper format of strings and lists to a proper dictionary value.
        '''

        if item:
            try:
                container = self.prereq_parse_json_row(item)
            except:
                container = None
                pass  # add log warning
        else:
            container = None

        return container

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df[['result']].fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            result = simplejson.loads(simplejson.dumps(result, ignore_nan=True))
            self.common.write_to_db_result(**result)

    @staticmethod
    def prereq_parse_json_row(item):
        '''
        primarly logic for formatting the value of the json
        '''

        container = dict()
        try:
            container = ast.literal_eval(item)
        except:
            json_str = ",".join(item)
            json_str_fixed = json_str.replace("'", '"')
            container = json.loads(json_str_fixed)

        # container = dict(container)
        # if isinstance(item, list):
        #     container = OrderedDict()
        #     for it in item:
        #         # value = re.findall("[0-9a-zA-Z_]+", it)
        #         value = re.findall("'([^']*)'", it)
        #         if len(value) == 2:
        #             for i in range(0, len(value), 2):
        #                 container[value[i]] = [value[i + 1]]
        #         else:
        #             if len(container.items()) == 0:
        #                 print('issue')  # delete later
        #                 # raise error
        #                 # haven't encountered an this. So should raise an issue.
        #                 pass
        #             else:
        #                 last_inserted_value_key = container.items()[-1][0]
        #                 container.get(last_inserted_value_key).append(value[0])
        # else:
        #     container = ast.literal_eval(item)
        return container

    @staticmethod
    def _get_numerator_and_denominator_type(config_param, context_relevant=False):
        numerator_type = config_param['numerator_type'][0]
        denominator_type = config_param['denominator_type'][0]
        if context_relevant:
            context_type = config_param['context_type'][0]
            return numerator_type, denominator_type, context_type
        return numerator_type, denominator_type

    @staticmethod
    def calculate_percentage_from_numerator_denominator(numerator_result, denominator_result):
        try:
            ratio = numerator_result / denominator_result
        except Exception as e:
            Log.error(e.message)
            ratio = 0
        if not isinstance(ratio, (float, int)):
            ratio = 0
        return round(ratio * 100, 2)

    @staticmethod
    def _filter_df(df, filters, exclude=0):
        for key, val in filters.items():
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]

        return df

    def get_kpi_custom_entity_table(self):
        """
        :param entity_type: pk of entity from static.entity_type
        :return: the DF of the static.custom_entity of this entity_type
        """
        query = "SELECT pk, name, entity_type_fk FROM static.custom_entity;"
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_custom_entity_value(self, value):
        custom_fk = self.custom_entity_table['pk'][self.custom_entity_table['name'] == value].iloc[0]
        return custom_fk

    def commit_results(self):
        self.common.commit_results_data()
