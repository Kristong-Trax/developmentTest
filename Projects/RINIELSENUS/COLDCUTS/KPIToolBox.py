from Trax.Algo.Calculations.Core.DataProvider import Data
import ast
import pandas as pd
import simplejson
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from KPIUtils_v2.DB.CommonV2 import Common
from Trax.Utils.Logging.Logger import Log
from Const import Consts
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AMER.CalculationsUtils.CalculationUtils import Eyelight
from KPIUtils_v2.Calculations.AMER.BlockAdjacencyCalculations import BlockAdjacency
import json
import numpy

__author__ = 'Nicolas Keeton'


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
        self.rds_conn = self.ps_data_provider.rds_conn
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
            Consts.SOS,
            Consts.HORIZONTAL_SHELF_POSITION,
            Consts.VERTICAL_SHELF_POSITION,
            Consts.BLOCKING,
            Consts.BLOCK_ADJ,
            Consts.BLOCKING_ORIENTATION
        ]

        targets = self.targets[self.targets[Consts.ACTUAL_TYPE].isin(relevant_kpi_types)]
        self._calculate_kpis_from_template(targets)
        self.save_results_to_db()
        return

    def calculate_blocking(self, row, df):
        additional_data = row['Config Params: JSON']
        location_data = row['Location: JSON']
        kpi_fk = row['kpi_fk']
        population_data = row['Dataset 1: JSON']['include'][0]
        result_dict_list = self._logic_for_blocking(kpi_fk, population_data, location_data, additional_data)
        return result_dict_list

    def calculate_blocking_adj(self, row, df):
        result_dict_list = []
        additional_data = row['Config Params: JSON']
        location_data = row['Location: JSON']
        kpi_fk = row['kpi_fk']
        anchor_data = row['Dataset 1: JSON']['include'][0]
        target_data = row['Dataset 2: JSON']['include'][0]

        context_type = additional_data.get('context_type')
        if context_type:
            target_df = ParseInputKPI.filter_df(target_data, self.scif)
            target_values = target_df[context_type].unique().tolist()
            context_values = [v for v in df[context_type].unique().tolist() if v and pd.notna(v) and v in target_values]
            for context_value in context_values:
                anchor_data.update({context_type: [context_value]})
                target_data.update({context_type: [context_value]})
                result_dict = self._logic_for_adj(kpi_fk, anchor_data, target_data, location_data, additional_data,
                                                  eyelight_prefix='{}-'.format(context_value),
                                                  custom_entity=context_value)
                result_dict_list.append(result_dict)
        else:
            result_dict = self._logic_for_adj(kpi_fk, anchor_data, target_data, location_data, additional_data)
            result_dict_list.append(result_dict)
        return result_dict_list

    def _logic_for_adj(self, kpi_fk, anchor_data, target_data, location_data, additional_data, custom_entity=None,
                       eyelight_prefix=None):
        result = self.adjacency.evaluate_block_adjacency(anchor_data, target_data, location=location_data,
                                                         additional=additional_data, kpi_fk=kpi_fk,
                                                         eyelight_prefix=eyelight_prefix)
        result_type_fk = Consts.CUSTOM_RESULT['Yes'] if result and pd.notna(result) else Consts.CUSTOM_RESULT['No']
        result_dict = {
            'kpi_fk': kpi_fk,
            'numerator_id': self.own_manufacturer_fk,
            'denominator_id': self.store_id,
            'numerator_result': 1 if result else 0,
            'denominator_result': 1,
            'result': result_type_fk
        }
        if custom_entity:
            result_dict.update({'context_id': self.get_custom_entity_value(custom_entity)})

        return result_dict

    def _logic_for_blocking(self, kpi_fk, population_data, location_data, additional_data):
        result_dict_list = []
        additional_data.update({'use_masking_only': True})
        block = self.block.network_x_block_together(population=population_data, location=location_data,
                                                    additional=additional_data)

        for row in block.itertuples():
            scene_match_fks = list(row.cluster.nodes[list(row.cluster.nodes())[0]]['scene_match_fk'])
            self.eyelight.write_eyelight_result(scene_match_fks, kpi_fk)
        passed_block = block[block['is_block']]

        if passed_block.empty:
            numerator_result = 0
            result_value = "No"
        else:
            numerator_result = 1
            result_value = "Yes"

        result_type_fk = Consts.CUSTOM_RESULT[result_value]
        # numerator_id = df.custom_entity_fk.iloc[0]

        result_dict = {
            'kpi_fk': kpi_fk,
            'numerator_id': self.own_manufacturer_fk, 'numerator_result': numerator_result,
            'denominator_id': self.store_id,
            'denominator_result': 1,
            'result': result_type_fk
        }

        result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_blocking_orientation(self,  row, df):
        result_dict_list = []
        additional_data = row['Config Params: JSON']
        location_data = row['Location: JSON']
        kpi_fk = row['kpi_fk']
        population_data = row['Dataset 1: JSON']
        if population_data:
            population_data = population_data['include'][0]
        else:
            population_data = {}

        additional_data.update({'vertical_horizontal_methodology': ['bucketing', 'percentage_of_shelves'],
                                'shelves_required_for_vertical': .8,
                                'check_vertical_horizontal': True})

        numerator_type = additional_data.get('numerator_type')
        if numerator_type:
            numerator_values = [v for v in df[numerator_type].unique().tolist() if v and pd.notna(v)]
            for numerator_value in numerator_values:
                population_data.update({numerator_type: [numerator_value]})
                result_dict = self._logic_for_blocking_orientation(kpi_fk, population_data, location_data,
                                                                   additional_data, numerator_value)
                result_dict_list.append(result_dict)
        else:
            result_dict = self._logic_for_blocking_orientation(kpi_fk, population_data, location_data, additional_data)
            result_dict_list.append(result_dict)

        return result_dict_list

    def _logic_for_blocking_orientation(self, kpi_fk, population_data, location_data, additional_data,
                                        custom_entity=None):
        additional_data.update({'use_masking_only': True})
        block = self.block.network_x_block_together(population=population_data, location=location_data,
                                                    additional=additional_data)
        if custom_entity:
            prefix = '{}-'.format(custom_entity)
            numerator_id = self.get_custom_entity_value(custom_entity)
        else:
            prefix = None
            numerator_id = self.own_manufacturer_fk
        for row in block.itertuples():

            scene_match_fks = list(row.cluster.nodes[list(row.cluster.nodes())[0]]['scene_match_fk'])
            self.eyelight.write_eyelight_result(scene_match_fks, kpi_fk, prefix=prefix)
        passed_block = block[block['is_block']]

        if passed_block.empty:
            result_value = "Not Blocked"
        else:
            result_value = passed_block.orientation.iloc[0]

        result = Consts.CUSTOM_RESULT[result_value]
        result_dict = {'kpi_fk': kpi_fk,
                       'numerator_id': numerator_id,
                       'numerator_result': 1 if result_value != 'Not Blocked' else 0,
                       'denominator_id': self.store_id,
                       'denominator_result': 1,
                       'result': result}

        return result_dict

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        return output

    def calculate_vertical_position(self, row, df):
        result_dict_list = []
        mpis = df  # get this from the external target filter_df method thingy
        scene_facings_df = mpis.groupby(['scene_fk', 'product_fk'], as_index=False)['facings'].max()
        scene_facings_df.rename(columns={'facings': 'scene_facings'}, inplace=True)
        shelf_df = self.merged_scif_mpis.groupby(['scene_fk', 'bay_number'],
                                                 as_index=False)['shelf_number_from_bottom'].max()
        shelf_df.rename(columns={'shelf_number_from_bottom': 'shelf_count'}, inplace=True)

        pre_sort_mpis = pd.merge(mpis, scene_facings_df, how='left', on=['scene_fk', 'product_fk'])
        scene_facings_df_sorted = pre_sort_mpis.sort_values('scene_facings')
        mpis = scene_facings_df_sorted.drop_duplicates(['scene_fk', 'product_fk'], keep="last")

        mpis = pd.merge(mpis, shelf_df, how='left', on=['scene_fk', 'bay_number'])

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

        scene_facings_df = mpis.groupby(['scene_fk', 'product_fk'], as_index=False)['facings'].max()
        scene_facings_df.rename(columns={'facings': 'scene_facings'}, inplace=True)
        pre_sort_mpis = pd.merge(mpis, scene_facings_df, how='left', on=['scene_fk', 'product_fk'])

        bay_df = pre_sort_mpis.groupby('scene_fk', as_index=False)['bay_number'].max()
        bay_df.rename(columns={'bay_number': 'bay_count'}, inplace=True)
        mpis = pd.merge(pre_sort_mpis, bay_df, how='left', on='scene_fk')
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
        config_json = row['Config Params: JSON']
        numerator_type = config_json['numerator_type']
        df.dropna(subset=[numerator_type], inplace=True)
        result_dict_list = self._logic_for_sos(return_holder, df, numerator_type)
        return result_dict_list

    def _logic_for_sos(self, return_holder, df, numerator_type):
        result_list = []
        for num_item in df[numerator_type].unique().tolist():
            numerator_scif = df[df[numerator_type] == num_item]
            numerator_result = numerator_scif.facings.sum()
            denominator_result = df.facings.sum()
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
            return self.calculate_facings_sos
        elif kpi_type == Consts.HORIZONTAL_SHELF_POSITION:
            return self.calculate_horizontal_position
        elif kpi_type == Consts.VERTICAL_SHELF_POSITION:
            return self.calculate_vertical_position
        elif kpi_type == Consts.BLOCKING:
            return self.calculate_blocking
        elif kpi_type == Consts.BLOCK_ADJ:
            return self.calculate_blocking_adj
        elif kpi_type == Consts.BLOCKING_ORIENTATION:
            return self.calculate_blocking_orientation

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
            except Exception as e:
                Log.error('Unable to calculate {}: {}'.format(row[Consts.KPI_NAME], e))

    def _parse_json_filters_to_df(self, row):
        json = row[(row.index.str.contains('JSON')) &
                   (~row.index.str.contains('Config Params')) &
                   (~row.index.str.contains('Dataset 2'))]
        filter_json = json[~json.isnull()]
        filtered_scif_mpis = self.merged_scif_mpis
        for each_json in filter_json:
            final_json = {'population': each_json} if ('include' or 'exclude') in each_json else each_json
            filtered_scif_mpis = ParseInputKPI.filter_df(final_json, filtered_scif_mpis)
        if 'include_stacking' in row['Config Params: JSON'].keys():
            including_stacking = row['Config Params: JSON']['include_stacking'][0]
            filtered_scif_mpis[Consts.FINAL_FACINGS] = \
                filtered_scif_mpis.facings if including_stacking == 'True' else filtered_scif_mpis.facings_ign_stack
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
            except Exception as e:
                container = None
                Log.warning('{}: Unable to parse json for: {}'.format(e, item))
        else:
            container = None

        return container

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df['result'].fillna(0, inplace=True)
        self.results_df['score'].fillna(0, inplace=True)
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

    def get_kpi_custom_entity_table(self):
        """
        :param entity_type: pk of entity from static.entity_type
        :return: the DF of the static.custom_entity of this entity_type
        """
        query = "SELECT pk, name, entity_type_fk FROM static.custom_entity;"
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_custom_entity_value(self, value):
        try:
            custom_fk = self.custom_entity_table['pk'][self.custom_entity_table['name'] == value].iloc[0]
            return custom_fk
        except IndexError:
            Log.error('No custom entity found for: {}'.format(value))
            return None

    def commit_results(self):
        self.common.commit_results_data()
