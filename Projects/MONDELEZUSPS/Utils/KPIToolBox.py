from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.Utils.Parsers import ParseInputKPI
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block

from collections import OrderedDict
import pandas as pd
import simplejson
import os
import numpy as np
import re
import ast

from Projects.MONDELEZUSPS.Data.LocalConsts import Consts

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

MATCH_PRODUCT_IN_PROBE_FK = 'match_product_in_probe_fk'
MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK = 'match_product_in_probe_state_reporting_fk'
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'shelf_position.xlsx')
SHEETS = [Consts.SHELF_MAP]

__author__ = 'krishnat'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.common_v2 = CommonV2(self.data_provider)
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.templates = {}
        self.parse_template()
        self.shelf_number = self.templates['Shelf Map'].set_index('Num Shelves')
        self.block = Block(data_provider)
        self.match_product_in_probe_state_reporting = self.ps_data_provider.get_match_product_in_probe_state_reporting()
        self.assortment = Assortment(self.data_provider, self.output)
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.merged_scif_mpis = self.match_product_in_scene.merge(self.scif, how='left',
                                                                  left_on=['scene_fk', 'product_fk'],
                                                                  right_on=['scene_fk', 'product_fk'])
        self.gold_zone_scene_location_kpi = ['Lobby/Entrance', 'Main Alley/Hot Zone', 'Gold Zone End Cap',
                                             'Lobby/Main Entrance']
        self.custom_entity_table = self.get_kpi_custom_entity_table()
        self.final_custom_entity_table = self.custom_entity_table.copy()
        self.store_area = self.get_store_area_df()
        self.targets = self.ps_data_provider.get_kpi_external_targets()
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result', 'context_id',
                                                'denominator_id', 'denominator_result', 'result', 'score'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df[['result']].fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            result = simplejson.loads(simplejson.dumps(result, ignore_nan=True))
            self.write_to_db(**result)

    def main_calculation(self):
        # , Consts.SHARE_OF_SCENES, Consts.SCENE_LOCATION, Consts.SHELF_POSITION, Consts.BLOCKING, Consts.SHELF_POSITION
        relevant_kpi_types = [Consts.SHELF_POSITION]
        targets = self.targets[self.targets[Consts.KPI_TYPE].isin(relevant_kpi_types)]

        self._calculate_kpis_from_template(targets)
        self.save_results_to_db()
        return

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[Consts.KPI_TYPE])
            row = self.apply_json_parser(row)
            merged_scif_mpis = self._parse_json_filters_to_df(row)
            result_data = calculation_function(row, merged_scif_mpis)
            if result_data and isinstance(result_data, list):
                for result in result_data:
                    self.results_df.loc[len(self.results_df), result.keys()] = result
            elif result_data and isinstance(result_data, dict):
                self.results_df.loc[len(self.results_df), result_data.keys()] = result_data

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == Consts.SHARE_OF_SCENES:
            return self.calculate_share_of_scenes
        elif kpi_type == Consts.SCENE_LOCATION:
            return self.calculate_scene_location
        elif kpi_type == Consts.SHELF_POSITION:
            return self.calculate_shelf_position
        elif kpi_type == Consts.BLOCKING:
            return self.calculate_blocking
        elif kpi_type == Consts.DISTRIBUTION:
            return self.calculate_distribution
        elif kpi_type == Consts.BAY_POSITION:
            return self.calculate_bay_position

    def calculate_bay_position(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        result_dict_list = []
        for unqiue_denominator_id in set(df[denominator_type]):
            denominator_filtered_df = self._filter_df(df, {denominator_type: unqiue_denominator_id})
            denomi_df_grouped_facings = self._df_groupby_logic(denominator_filtered_df, ['scene_fk', numerator_type],
                                                               {'facings': 'sum'})
            relevant_scene_with_most_facings = \
                denomi_df_grouped_facings.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]
            scene_filtered_df = self._filter_df(denominator_filtered_df, {'scene_fk': relevant_scene_with_most_facings})
            count_of_bays_in_scene = scene_filtered_df.bay_number.max()

            ## logic for calculating bay number##
            bay_df = self._df_groupby_logic(scene_filtered_df, ['bay_number'], {'facings': 'sum'})
            relevant_bay_df = self._filter_df(bay_df, {'facings': bay_df.facings.max()})

            if len(relevant_bay_df) > 1:
                relevant_bay_number_container = relevant_bay_df.index
                bay_number = apply_tie_breaker_logic_for_bay_position(relevant_bay_number_container,
                                                                      count_of_bays_in_scene)

                def apply_tie_breaker_logic_for_bay_position(bay_number_container, count_of_bays_in_scene):
                    bay_number_df = pd.DataFrame()
                    bay_number_df['bay_number'] = bay_number_container
                    bay_number_df['bay_number_score'] = [np.square((num - ((count_of_bays_in_scene + 1) / 2))) for num
                                                         in bay_number_container]
                    final_bay_number_df = bay_number_df[
                        bay_number_df.bay_number_score == bay_number_df.bay_number_score.max()]
                    # if there is a tie with the max score between bay, we get the smallest bay #, else the bay number with the highest score
                    return_bay_number = final_bay_number_df.bay_number.min()
                    return return_bay_number
            else:
                bay_number = relevant_bay_df.index[0]

            ## logic for calculating bay number##

            #####logic for calculating bay result #######
            if bay_number != 1 or bay_number == count_of_bays_in_scene:
                result = 'Not Anchor'
            else:
                bay_number_df = self._filter_df(scene_filtered_df, {'bay_number': bay_number})

            # wait on logic for result
            a = 1

    def calculate_distribution(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        # a =  self.assortment.main_assortment_calculation()
        a = 1

    def calculate_shelf_position(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        df.dropna(subset=[numerator_type, denominator_type], inplace=True)
        result_dict_list = self._logic_for_shelf_position(df, return_holder, numerator_type, denominator_type)
        return result_dict_list

    def _logic_for_shelf_position(self, df, return_holder, numerator_type, denominator_type):
        '''
        "For each [numerator] within each [denominator] population
        Find the scene that contains the most [numerator] facings and consider only bays in that scene with [numerator] product
        Determine the maximum number of shelves in those relevant bays, which we'll call [shelves]
        Now find the [shelf] within those bays with the most [numerator] facings, which we'll call [shelf#]
        For determining shelf numbers, use the 'shelf number from bottom' method, where the bottom shelf is #1
        If multiple shelves tie here, use the highest shelf number (from bottom as mentioned above)
        Use the shelf map template [See Shelf Map tab] to determine the result value
        The first column lists possible total shelf figures, which correspond to [shelves]
        The remaining columns list what the value should be for each [shelf#] in the corresponding set
        So the intersection of the row where column A matches [shelves] and the column where row 3 matches [shelf#] holds the [KPI Result] value"
        '''
        result_dict_list = []
        key_dict = {'Bottom': 2, 'Middle': 3, 'Eye': 4, 'Top': 5}

        for unique_denominator_fk in set(df[denominator_type]):
            unique_template_scif_mpis = self._filter_df(df, {denominator_type: unique_denominator_fk})
            df_with_max_facings_by_scene = self._df_groupby_logic(unique_template_scif_mpis,
                                                                  ['scene_fk', numerator_type],
                                                                  {'facings': 'count'})

            relevant_scene_with_most_numerator_facings = \
                df_with_max_facings_by_scene.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]

            relevant_scif_mpis_scene_with_most_facings = self._filter_df(unique_template_scif_mpis, {
                'scene_fk': relevant_scene_with_most_numerator_facings})

            df_with_max_facings_by_bay = self._df_groupby_logic(relevant_scif_mpis_scene_with_most_facings,
                                                                ['bay_number', numerator_type], {'facings': 'count'})
            relevant_bay_with_most_numerator_facings = \
                df_with_max_facings_by_bay.agg(['max', 'idxmax']).loc['idxmax', 'facings'][0]
            final_df = self._filter_df(relevant_scif_mpis_scene_with_most_facings,
                                       {'bay_number': relevant_bay_with_most_numerator_facings})
            container_with_shelf_number = self._df_groupby_logic(final_df, ['shelf_number_from_bottom', numerator_type],
                                                                 {'facings': 'count'})
            max_shelf = final_df.shelf_number_from_bottom.max()
            shelf_number = container_with_shelf_number.agg(['max', 'idxmax']).loc['idxmax', 'facings'][
                0]  # shelf with the mose number of facings

            try:
                result = self.shelf_number.loc[max_shelf, shelf_number]
                result_by_id = key_dict.get(result)
            except:
                continue

            numerator_id = container_with_shelf_number.agg(['max', 'idxmax']).loc['idxmax', 'facings'][
                1]  # numerator_id with most facings in the bay
            if not isinstance(numerator_id, int):
                numerator_id = self._get_id_from_custom_entity_table(numerator_type, numerator_id)
            result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                           'numerator_id': numerator_id,
                           'numerator_result': shelf_number,
                           'denominator_id': unique_denominator_fk, 'denominator_result': max_shelf,
                           'result': result_by_id}
            result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_scene_location(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        scene_store_area_df = self.store_area
        scene_store_area_df['result'] = scene_store_area_df.name.apply(
            lambda x: 1 if x in self.gold_zone_scene_location_kpi else 0)
        result_dict_list = []

        for store_area_row in scene_store_area_df.itertuples():
            result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                           'numerator_id': store_area_row.pk,
                           'numerator_result': store_area_row.result,
                           'denominator_id': self.store_id, 'denominator_result': 1,
                           'result': store_area_row.result}
            result_dict_list.append(result_dict)
        return result_dict_list

    def calculate_blocking(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        numerator_type, denominator_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=False)
        df.dropna(subset=[numerator_type], inplace=True)
        result_dict_list = self._logic_for_blocking(return_holder, df, numerator_type, denominator_type)
        return result_dict_list

    def _logic_for_blocking(self, return_holder, df, numerator_type, denominator_type):
        result_dict_list = []
        for unique_denominator_id in set(df[denominator_type]):
            relevant_df = self._filter_df(df, {denominator_type: unique_denominator_id})
            for unique_scene_fk in set(relevant_df.scene_fk):
                scene_relevant_df = self._filter_df(relevant_df, {'scene_fk': unique_scene_fk})
                location = {Consts.SCENE_FK: unique_scene_fk}
                for unique_numerator_id in set(scene_relevant_df[numerator_type]):
                    relevant_filter = {numerator_type: [unique_numerator_id]}
                    block = self.block.network_x_block_together(population=relevant_filter, location=location,
                                                                additional={'calculate_all_scenes': False,
                                                                            'use_masking_only': True,
                                                                            'include_stacking': False})
                    passed_block = block[block.is_block.isin([True])]
                    if block.empty:
                        continue
                    elif passed_block.empty:
                        relevant_block = block.iloc[block.block_facings.astype(
                            int).idxmax()]
                        numerator_result = relevant_block.block_facings
                        denominator_result = relevant_block.total_facings
                        result = 0
                    else:
                        relevant_block = passed_block.iloc[passed_block.block_facings.astype(
                            int).idxmax()]
                        numerator_result = relevant_block.block_facings
                        denominator_result = relevant_block.total_facings
                        result = 1
                        self.mark_tags_in_explorer(relevant_block, return_holder[0])
                    if not isinstance(unique_numerator_id, int):
                        unique_numerator_id = self._get_id_from_custom_entity_table(numerator_type, unique_numerator_id)
                    result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                                   'numerator_id': unique_numerator_id, 'numerator_result': numerator_result,
                                   'denominator_id': unique_denominator_id,
                                   'denominator_result': denominator_result,
                                   'result': result}
                    result_dict_list.append(result_dict)
        return result_dict_list

    def mark_tags_in_explorer(self, relevant_block, mpipsr_name):
        probe_match_fk_list = [item for each in relevant_block.cluster.nodes.values() for item in
                               each['probe_match_fk']]
        if not probe_match_fk_list:
            return
        try:
            match_type_fk = \
                self.match_product_in_probe_state_reporting[
                    self.match_product_in_probe_state_reporting['name'] == mpipsr_name][
                    'match_product_in_probe_state_reporting_fk'].values[0]
        except IndexError:
            Log.warning('Name not found in match_product_in_probe_state_reporting table: {}'.format(mpipsr_name))
            return

        match_product_in_probe_state_values_old = self.common.match_product_in_probe_state_values
        match_product_in_probe_state_values_new = pd.DataFrame(columns=[MATCH_PRODUCT_IN_PROBE_FK,
                                                                        MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK])
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_FK] = probe_match_fk_list
        match_product_in_probe_state_values_new[MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK] = match_type_fk

        # self.common.match_product_in_probe_state_values = pd.concat([match_product_in_probe_state_values_old,
        #                                                                 match_product_in_probe_state_values_new])
        self.common.match_product_in_probe_state_values = self.common.match_product_in_probe_state_values.append(
            match_product_in_probe_state_values_new)

        return

    def calculate_share_of_scenes(self, row, df):
        return_holder = self._get_kpi_name_and_fk(row)
        facings_threshold = row['Config Params: JSON'].get('facings_threshold')[0]
        numerator_type, denominator_type, context_type = self._get_numerator_and_denominator_type(
            row['Config Params: JSON'], context_relevant=True)

        result_dict_list = self.logic_of_sos(return_holder, df, numerator_type, denominator_type,
                                             context_type, facings_threshold)
        return result_dict_list

    @staticmethod
    def logic_of_sos(return_holder, relevant_scif, numerator_type, denominator_type, context_type,
                     facings_threshold):
        result_dict_list = []

        for unique_context_fk in set(relevant_scif[context_type]):
            template_unique_scif = relevant_scif[relevant_scif[context_type].isin([unique_context_fk])]
            for unique_denominator_fk in set(template_unique_scif[denominator_type]):
                denominator_result = 0
                category_unique_scif = template_unique_scif[
                    template_unique_scif[denominator_type].isin([unique_denominator_fk])]
                for unique_scene in set(category_unique_scif.scene_fk):
                    numerator_result = 0
                    scene_unique_scif = category_unique_scif[category_unique_scif.scene_fk.isin([unique_scene])]
                    for unique_numerator_fk in set(scene_unique_scif[numerator_type]):
                        manufacturer_unique_scif = scene_unique_scif[
                            scene_unique_scif[numerator_type].isin([unique_numerator_fk])]
                        if manufacturer_unique_scif.drop_duplicates(subset=['product_fk']).facings.sum() >= int(
                                facings_threshold):
                            numerator_result = numerator_result + 1
                            denominator_result = denominator_result + 1
                if denominator_result != 0:
                    result = float(numerator_result) / denominator_result
                    result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1],
                                   'numerator_id': unique_numerator_fk,
                                   'numerator_result': numerator_result,
                                   'denominator_id': unique_denominator_fk, 'denominator_result': denominator_result,
                                   'context_id': unique_context_fk, 'result': result}
                    result_dict_list.append(result_dict)
        return result_dict_list

    def get_store_area_df(self):
        query = """
                 select st.pk, sst.scene_fk, st.name, sc.session_uid 
                 from probedata.scene_store_task_area_group_items sst
                 join static.store_task_area_group_items st on st.pk=sst.store_task_area_group_item_fk
                 join probedata.scene sc on sc.pk=sst.scene_fk
                 where sc.delete_time is null and sc.session_uid = '{}';
                 """.format(self.session_uid)

        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_entity_type_fk(self, numerator_type):
        query = """select pk, name, table_name from static.kpi_entity_type 
                    where name = '{}';""".format(numerator_type)
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def get_kpi_custom_entity_table(self):
        """
        :param entity_type: pk of entity from static.entity_type
        :return: the DF of the static.custom_entity of this entity_type
        """
        query = "SELECT pk, name, entity_type_fk FROM static.custom_entity;"
        df = pd.read_sql_query(query, self.rds_conn.db)
        return df

    def _parse_json_filters_to_df(self, row):
        JSON = row[row.index.str.contains('JSON') & (~ row.index.str.contains('Config Params'))]
        filter_JSON = JSON[~JSON.isnull()]

        filtered_scif_mpis = self.merged_scif_mpis
        for each_JSON in filter_JSON:
            final_JSON = {'population': each_JSON} if ('include' or 'exclude') in each_JSON else each_JSON
            filtered_scif_mpis = ParseInputKPI.filter_df(final_JSON, filtered_scif_mpis)
        return filtered_scif_mpis

    def apply_json_parser(self, row):
        json_relevent_rows_with_parse_logic = row[row.index.str.contains('JSON')].apply(self.parse_json_row)
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
            container = self.prereq_parse_json_row(item)
        else:
            container = None
        return container

    @staticmethod
    def prereq_parse_json_row(item):
        '''
        primarly logic for formatting the value of the json
        '''

        if isinstance(item, list):
            container = OrderedDict()
            for it in item:
                # value = re.findall("[0-9a-zA-Z_]+", it)
                value = re.findall("'([^']*)'", it)
                if len(value) == 2:
                    for i in range(0, len(value), 2):
                        container[value[i]] = [value[i + 1]]
                else:
                    if len(container.items()) == 0:
                        print('issue')  # delete later
                        # raise error
                        # haven't encountered an this. So should raise an issue.
                        pass
                    else:
                        last_inserted_value_key = container.items()[-1][0]
                        container.get(last_inserted_value_key).append(value[0])
        else:
            container = ast.literal_eval(item)
        return container

    def _get_kpi_name_and_fk(self, row):
        kpi_name = row[Consts.KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        return output

    def _get_id_from_custom_entity_table(self, numerator_type, string_numerator_id):
        '''
        :param numerator_type: Numerator type from the config json file
        :param string_numerator_id: the name of the numerator if the numerator type is a string

        Use the class variable to reference the data in the custom entity table. The goal of the method is to save the
        custom entity to the table if it doesn't exist. If it does exists, retreive the pk.
        '''

        # performs a query on static.kpi_entity_type table and then gets the relevant custom entity fk
        relevant_entity_type_fk = self.get_kpi_entity_type_fk(numerator_type).loc[0, 'pk']
        if self.final_custom_entity_table.empty:
            final_numerator_id = 1
            self.final_custom_entity_table.loc[final_numerator_id, self.final_custom_entity_table.columns] = [
                final_numerator_id, string_numerator_id, relevant_entity_type_fk]
            self._save_into_custom_entity_table(final_numerator_id, string_numerator_id,
                                                relevant_entity_type_fk)
        elif string_numerator_id not in self.final_custom_entity_table.name.values:
            final_numerator_id = np.amax(self.final_custom_entity_table.pk) + 1
            self.final_custom_entity_table.loc[final_numerator_id, self.final_custom_entity_table.columns] = [
                final_numerator_id, string_numerator_id, relevant_entity_type_fk]
            self._save_into_custom_entity_table(final_numerator_id, string_numerator_id,
                                                relevant_entity_type_fk)
        else:
            final_numerator_id = \
                self.final_custom_entity_table[self.final_custom_entity_table.name.isin([string_numerator_id])].iloc[
                    0, 0]

        return final_numerator_id

    def _save_into_custom_entity_table(self, final_numerator_id, string_numerator_id,
                                       relevant_entity_type_fk):
        query = """INSERT INTO static.custom_entity (pk,name, entity_type_fk) 
                                              values ({},"{}", {});""".format(final_numerator_id, string_numerator_id,
                                                                              relevant_entity_type_fk)
        cur = self.rds_conn.db.cursor()
        cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def _get_numerator_and_denominator_type(config_param, context_relevant=False):
        numerator_type = config_param['numerator_type'][0]
        denominator_type = config_param['denominator_type'][0]
        if context_relevant:
            context_type = config_param['context_type'][0]
            return numerator_type, denominator_type, context_type
        return numerator_type, denominator_type

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

    @staticmethod
    def _df_groupby_logic(df, grouby_columns, aggregation_dict):
        '''
        :param df: relevant dataframe
        :param grouby_columns: list of relevant columns that are grouped
        :param aggregation_dict: aggregation dictionary with relevant column and logic
                example: {'facings':'sum'}
        :return: returns dataframe with groupby logic applied
        '''

        if isinstance(grouby_columns, str):
            grouby_columns = list(grouby_columns)

        final_df = df.groupby(grouby_columns).agg(aggregation_dict)
        return final_df
