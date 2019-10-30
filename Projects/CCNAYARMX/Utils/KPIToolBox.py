from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2
# from BlockCalculations_v3 import Block


import pandas as pd
import numpy as np
import os
from datetime import datetime

from Projects.CCNAYARMX.Data.LocalConsts import Consts

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


__author__ = 'krishnat'

# Column Name
KPI_NAME = 'KPI Name'
TASK_TEMPLATE_GROUP = 'Task/ Template Group'
TEMPLATE_NAME = 'template_name'
MANUFACTURER_NAME = 'manufacturer_name'
PRODUCT_TYPE = 'product_type'
PRODUCT_SHORT_NAME = 'product_short_name'
PRODUCT_WEIGHT = 'product_weight'
STORE_ADDITIONAL_ATTRIBUTE_2 = 'store_additional_attribute_2'
TAMANDO_DEL_PRODUCTO = 'TAMANO DEL PRODUCTO'
IGNORE_STACKING = 'Ignore Stacking'
ACTIVATION_SCENE_TYPE = 'Activation Scene Type'
FACINGS_TARGET = 'facings_target'
BAY_COUNT_TARGET = 'bay_count_target'
SUB_CATEGORY = 'sub_category'
ITERATE_BY = 'iterate by'
RELEVANT_QUESTION_FK = 'relevant_question_fk'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'
DENOMINATOR_PARAM_1 = 'denominator param 1'
DENOMINATOR_VALUE_1 = 'denominator value 1'
NUMERATOR_ENTITY = 'Numerator Entity'
DENOMINATOR_ENTITY = 'Denominator Entity'

# Sheet names
SOS = 'SOS'
BLOCK_TOGETHER = 'Block Together'
SHARE_OF_EMPTY = 'Share of Empty'
BAY_COUNT = 'Bay Count'
PER_BAY_SOS = 'Per bay SOS'
SURVEY = 'Survey'
AVAILABILITY = 'Availability'
DISTRIBUTION = 'Distribution'

POS_OPTIONS = 'POS Options'
TARGETS_AND_CONSTRAINTS = 'Targets and Constraints'

# Scif Filters
BRAND_FK = 'brand_fk'
PRODUCT_FK = 'product_fk'
FACINGS = 'facings'
FACINGS_IGN_STACK = 'facings_ign_stack'
FINAL_FACINGS = 'final_facings'
MANUFACTURER_FK = 'manufacturer_fk'
PK = 'pk'
PRODUCT_NAME = 'product_name'
ADDITIONAL_ATTRIBUTE_2 = 'additional_attribute_2'
SESSION_ID = 'session_id'
SCENE_ID = 'scene_id'
SCENE_FK = 'scene_fk'
TEMPLATE_FK = 'template_fk'
TEMPLATE_GROUP = 'template_group'

# Match Product In Scene
BAY_NUMBER = 'bay_number'

# Read the sheet
SHEETS = [SOS, BLOCK_TOGETHER, SHARE_OF_EMPTY, BAY_COUNT, PER_BAY_SOS, SURVEY, AVAILABILITY, DISTRIBUTION]
POS_OPTIONS_SHEETS = [POS_OPTIONS, TARGETS_AND_CONSTRAINTS]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'CCNayarTemplatev0.5.3.xlsx')
POS_OPTIONS_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                         'CCNayar_POS_Options_v2.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.common_v2 = CommonV2(self.data_provider)
        self.ps_data_provider = PsDataProvider(data_provider)
        self.block = Block(data_provider)
        self.templates = {}
        self.parse_template()
        self.match_product_in_scene = self.data_provider['matches']
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.survey = Survey(self.data_provider, output=output, ps_data_provider=self.ps_data_provider,
                             common=self.common_v2)
        self.platformas_data = self.generate_platformas_data()
        self.assortment = Assortment(self.data_provider, common=self.common)

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)
        for sheet in POS_OPTIONS_SHEETS:
            self.templates[sheet] = pd.read_excel(POS_OPTIONS_TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        for i, row in self.templates[AVAILABILITY].iterrows():
            # self.calculate_sos(row)
            # self.calculate_block_together(row)
            # self.calculate_share_of_empty(row)
            # self.calculate_bay_count(row)
            # self.calculate_per_bay_sos(row)
            # self.calculate_survey(row)
            self.calculate_availability(row)
            self.calculate_assortment(row)
            self.store_wrong_data_for_parent_kpi_comunicacion()
            self.store_wrong_data_for_parent_kpi_enfriador()
            self.store_wrong_data_for_parent_kpi_plataformas()
            self.store_wrong_data_for_parent_kpi_portafolio_y_precios()
            self.store_wrong_data_for_parent_kpi_primera_posicion()
            self.store_wrong_data_for_parent_kpi_respeto()
            self.store_wrong_data_for_parent_kpi_acomodo_por_bloques()
            self.store_wrong_data_for_parent_kpi_bloques_colas_50()
            self.store_wrong_data_for_parent_kpi_bloques_frutales_25()
            self.store_wrong_data_for_parent_kpi_comidas()
            self.store_wrong_data_for_parent_kpi_plat_dinamicas_one()
            self.store_wrong_data_for_parent_kpi_plat_dinamicas_two()
        return

    def generate_platformas_data(self):
        platformas_data = pd.DataFrame(columns=['scene_id', 'Platform Name', 'POS Option Present',
                                                'Mandatory SKUs found', 'Minimum facings met', 'Coke purity'])
        for scene in self.scif[['scene_id', 'template_name']].drop_duplicates().itertuples():
            scene_scif = self.scif[self.scif['scene_id'] == scene.scene_id]
            product_names_in_scene = \
                set(scene_scif['product_name'].unique().tolist())

            relevant_pos_template = self.templates[POS_OPTIONS]
            relevant_pos_template = relevant_pos_template[
                relevant_pos_template['template_name'].str.encode('utf-8') == scene.template_name.encode('utf-8')]

            # check the 'POS Option' activation, i.e. 'copete'
            pos_option_found = 0  # False
            for index, relevant_row in relevant_pos_template.iterrows():
                groups = self._get_groups(relevant_row, 'POS Option')
                for group in groups:
                    if all(product in product_names_in_scene for product in group):
                        pos_option_found = 1  # True
                        break
            if not pos_option_found:
                continue

            platform_name = relevant_row['Platform Name']

            targets_and_constraints = self._get_relevant_targets_and_constraints(platform_name, scene.template_name)
            if targets_and_constraints.empty:
                # this is needed for the Precios en Cooler KPI
                platformas_data.loc[len(platformas_data), platformas_data.columns.tolist()] = [
                    scene.scene_id, platform_name, pos_option_found, 0, 0, 0
                ]
                continue

            # calculate the 'empaques' data
            assortment_groups = self._get_groups(relevant_row, 'Assortment')
            mandatory_skus_found = 1  # True
            for assortment in assortment_groups:
                if not any(product in product_names_in_scene for product in assortment):
                    mandatory_skus_found = 0  # False
                    break
            limited_product = self.sanitize_values(targets_and_constraints['max_facings_product_local_name'].iloc[0])
            if limited_product and limited_product is not np.nan:
                if scene_scif[scene_scif['product_name'].isin(limited_product)]['facings'].sum() > \
                        targets_and_constraints['max_facings'].iloc[0]:
                    mandatory_skus_found = 0
            # this should be refactored to be more programmatic
            if targets_and_constraints['Assortment_Facings_Constraints'].iloc[0] == 'Assortment_2>Assortment_1':
                assortment_1_facings = \
                    scene_scif[scene_scif['product_name'].isin[assortment_groups[0]]]['facings'].sum()
                assortment_2_facings = \
                    scene_scif[scene_scif['product_name'].isin[assortment_groups[1]]]['facings'].sum()
                if assortment_1_facings >= assortment_2_facings:
                    mandatory_skus_found = 0

            # calculate the 'botellas' data
            total_facings = scene_scif[scene_scif['product_name'].isin(
                [product for sublist in assortment_groups for product in sublist])]['facings'].sum()
            if total_facings > targets_and_constraints['Facings_target'].iloc[0]:
                minimum_facings_met = 1  # True
            else:
                minimum_facings_met = 0  # False

            # calculate the coke purity (coke SOS) of this scene
            coke_purity_for_scene = self._get_coke_purity_for_scene(scene_scif)

            platformas_data.loc[len(platformas_data), platformas_data.columns.tolist()] = [
                scene.scene_id, platform_name, pos_option_found, mandatory_skus_found,
                minimum_facings_met, coke_purity_for_scene
            ]
        return platformas_data

    @staticmethod
    def _get_coke_purity_for_scene(scene_scif):
        coke_facings = scene_scif[scene_scif['manufacturer_name'] == 'TCCC']['facings'].sum()
        total_facings = scene_scif['facings'].sum()
        if coke_facings > 0:
            return coke_facings / float(total_facings)
        else:
            return 0

    def _get_relevant_targets_and_constraints(self, platform_name, template_name):
        relevant_template = self.templates[TARGETS_AND_CONSTRAINTS]
        relevant_template = relevant_template[(relevant_template['Platform Name'] == platform_name) &
                                              (relevant_template['store_additional_attribute_2'] ==
                                               self.store_info['additional_attribute_2'].iloc[0]) &
                                              (relevant_template['template_name'] == template_name)]
        return relevant_template

    @staticmethod
    def _get_groups(series, root_string):
        groups = []
        for column in [col for col in series.index.tolist() if root_string in col]:
            if series[column] not in ['', np.nan]:
                groups.append([x.strip() for x in series[column].split(',')])
        return groups

    def calculate_assortment(self, row):
        self.store_assortment = self.assortment.store_assortment
        a = 1

    def calculate_sos(self, row):
        '''
        :param row: READS THE LINE FROM THE TEMPLATE
        :return: the sum of numerator scif[final_facings] over the sum of denominator scif[final_facings]
        '''

        # REMINDER Filter scif by additional scene type column
        # Waiting on Session with with scene type(template_name) with Enfriador Dedicado JDV

        # Table of Contents:
        # Step 1 to 2: Declaring all the relevant columns from Sheet SOS
        # Step 3 to 5: Filtering scif to get all the relevant columns
        # Step 6 to 9: Filtering the relevant columns with the relevant rows; the id and result of numerator and denominator were calculated
        # Step 10: Calculates the final result
        # Step 11: Saves to the DB

        # Step 1: Read the excel rows to process the information(Common among all the sheets)
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_value1 = row[NUMERATOR_VALUE_1]
        denominator_value1 = row[DENOMINATOR_VALUE_1]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        # Step 2: Import the values that are unique to the sheet SOS
        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        store_additional_attribute_2 = self.sanitize_values(row[STORE_ADDITIONAL_ATTRIBUTE_2])
        numerator_param1 = row[NUMERATOR_PARAM_1]
        denominator_param1 = row[DENOMINATOR_PARAM_1]
        ignore_stacking = row[IGNORE_STACKING]
        additional_scene_type = row[ACTIVATION_SCENE_TYPE]

        # Step 3: Declare the relevant scif column for the SOS KPI
        relevant_scif_columns = [PK, SESSION_ID, TEMPLATE_GROUP, PRODUCT_TYPE, FACINGS, FACINGS_IGN_STACK] + \
                                [denominator_entity, numerator_entity] + self.delete_filter_nan(
            [numerator_param1, denominator_param1])

        # Step 4: Output the relevant scif by the applying relevant_scif_columns
        filtered_scif = self.scif[relevant_scif_columns]

        # Step 5: Determine Whether to Facings or Facings_ign_stack
        if pd.isna(ignore_stacking):
            filtered_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
            filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
        elif ignore_stacking == 'Y':
            filtered_scif = filtered_scif.drop(columns=[FACINGS])
            filtered_scif = filtered_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})

        # Step 6: Filter the filtered scif through the product type
        if pd.notna(any([product_type])):
            filtered_scif = filtered_scif[filtered_scif[PRODUCT_TYPE].isin(product_type)]

        # Step 7: Filter the filtered scif through the template group
        filtered_scif = filtered_scif[filtered_scif[TEMPLATE_GROUP].isin(template_group)]

        # Step 8: Filter the filtered scif with the denominator param and denominator value
        if pd.notna(denominator_param1):
            denominator_scif = filtered_scif[filtered_scif[denominator_param1].isin([denominator_value1])]
            denominator_id = \
                self.all_products[self.all_products[denominator_param1].isin([denominator_value1])][
                    denominator_entity].mode()[0]

            denominator_result = denominator_scif[FINAL_FACINGS].sum()
        else:
            denominator_scif = filtered_scif
            denominator_id = denominator_scif[denominator_entity].mode()[0]
            denominator_result = denominator_scif[FINAL_FACINGS].sum()

        # Step 9: Filter the denominator scif wit the numerator param and numerator param
        if pd.notna(numerator_param1):
            # Sometimes the filter below overfilters, and the df is empty
            if (denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]).empty:
                numerator_id = self.scif[numerator_entity].mode()[0]
                numerator_result = 0
            else:
                numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]
                numerator_id = \
                    self.all_products[self.all_products[numerator_param1].isin([numerator_value1])][
                        numerator_entity].mode()[
                        0]
                numerator_result = numerator_scif[FINAL_FACINGS].sum()

        else:
            numerator_scif = denominator_scif
            numerator_id = numerator_scif[numerator_entity].mode()[0]
            numerator_result = numerator_scif[FINAL_FACINGS].sum()

        # Step 10: Calculate the final result
        result = (numerator_result / denominator_result) * 100

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'numerator_result': numerator_result,
                       'denominator_id': denominator_id, 'denominator_result': denominator_result,
                       'result': result}

        return result_dict

    def calculate_block_together(self, row):

        # Step 1: Read the excel rows to process the information (Common among all the sheets)
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = row[TASK_TEMPLATE_GROUP]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        # Step 2: Import values that unique to the sheet Block Together
        template_name = row[TEMPLATE_NAME]
        manufacturer_name = [row[MANUFACTURER_NAME]]
        tamano_del_producto = [row[TAMANDO_DEL_PRODUCTO]]
        sub_category = self.sanitize_values(row[SUB_CATEGORY])
        iterate_by = row[ITERATE_BY]

        # Step 3: Declare relevant_scif columns
        relevant_scif_columns = [PK, SESSION_ID, PRODUCT_FK, PRODUCT_NAME, TEMPLATE_GROUP, TEMPLATE_NAME,
                                 MANUFACTURER_NAME,
                                 TAMANDO_DEL_PRODUCTO, SUB_CATEGORY] + \
                                [denominator_entity, numerator_entity, denominator_entity, SCENE_FK]

        # Step 4:
        relevant_scif = self.scif[relevant_scif_columns]

        product_in_scene = self.match_product_in_scene[[PRODUCT_FK, BAY_NUMBER, SCENE_FK]]

        bay_count_scif = relevant_scif.merge(product_in_scene, on=[PRODUCT_FK, SCENE_FK], how='right')
        bay_count_scif = bay_count_scif.dropna()

        bay_count_scif = bay_count_scif[bay_count_scif[MANUFACTURER_NAME].isin(manufacturer_name)]

        bay_count_scif = bay_count_scif[bay_count_scif[SUB_CATEGORY].isin(sub_category)]

        if pd.notna(template_group):
            bay_count_scif = bay_count_scif[bay_count_scif[TEMPLATE_GROUP].isin([template_group])]

        if pd.notna(template_name):
            bay_count_scif = bay_count_scif[bay_count_scif[TEMPLATE_NAME].isin([template_name])]

        if pd.notna(tamano_del_producto):
            bay_count_scif = bay_count_scif[bay_count_scif[TAMANDO_DEL_PRODUCTO].isin(tamano_del_producto)]

        if bay_count_scif.empty:
            result = 'NULL'

        else:
            unique_bay_number = list(set(bay_count_scif[BAY_NUMBER]))

            if pd.isna(iterate_by):
                location_name = TEMPLATE_GROUP
                location_id = template_group

                location = {location_name: location_id}

                for j in unique_bay_number:
                    # Step 3: Establish the variable for the network_x_block_together
                    if pd.notna(tamano_del_producto):
                        relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
                                            TAMANDO_DEL_PRODUCTO: tamano_del_producto, BAY_NUMBER: [j]}
                    else:
                        relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
                                            BAY_NUMBER: [j]}

                    block = self.block.network_x_block_together(relevant_filters,
                                                                location=location,
                                                                additional={'minimum_block_ratio': 0.9,
                                                                            'calculate_all_scenes': True})
                    if False in block['is_block'].to_list():
                        result = 0
                        break
                    else:
                        result = 1

            else:
                location_name = TEMPLATE_NAME
                location_id = template_name
                location = {location_name: location_id}

                for j in unique_bay_number:
                    relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
                                        BAY_NUMBER: [j]}

                    sub_category_fk_in_session = list(set(bay_count_scif[iterate_by]))
                    sub_category_fk_in_session = filter(None, sub_category_fk_in_session)

                    for value in sub_category_fk_in_session:
                        population = {iterate_by: value}
                        final_relevant_filters = self.merge_two_dictionaries(relevant_filters, population)
                        block = self.block.network_x_block_together(relevant_filters,
                                                                    location=location,
                                                                    additional={'minimum_block_ratio': 0.9,
                                                                                'calculate_all_scenes': True})
                        if False in block['is_block'].to_list():
                            result = 0
                            break
                        else:
                            result = 1

        denominator_id = self.scif['template_fk'].mode()[0]

        numerator_id = self.get_sub_cat_fk_from_sub_cat(sub_category[0])

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}

        return result_dict

    def calculate_share_of_empty(self, row):

        # Table of Contents
        # Step 1 to 2: Declaring all the relevant columns from Sheet SOS
        # Step 3 to 6: Filtering scif to get all the relevant columns
        # Step 7 to 9: Calculating the Numerator Scif and Denominator Scif, and finding the results and id of both
        # Step 10: Calculates the results
        # Step 11: Saves to the database

        # Step 1: Read the excel rows to process the information (Common among all the sheets)
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_value1 = row[NUMERATOR_VALUE_1]
        denominator_value1 = row[DENOMINATOR_VALUE_1]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        # Step 2: Import values that unique to the sheet SOS
        ignore_stacking = row[IGNORE_STACKING]
        numerator_param1 = row[NUMERATOR_PARAM_1]
        denominator_param1 = row[DENOMINATOR_PARAM_1]

        # Step 3: Filter the self.scif by the columns required
        column_filter_for_scif = [PK, SESSION_ID, TEMPLATE_GROUP, FACINGS, FACINGS_IGN_STACK] + \
                                 [numerator_entity, denominator_entity] + \
                                 self.delete_filter_nan([numerator_param1, denominator_param1])

        # Step 4: Apply the filters to scif
        filtered_scif = self.scif[column_filter_for_scif]

        # Step 5: Determing where to use the facings or facings ignore stack column
        if pd.isna(ignore_stacking):
            filtered_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
            filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
        elif ignore_stacking == 'Y':
            filtered_scif = filtered_scif.drop(columns=[FACINGS])
            filtered_scif = filtered_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})

        # Step 6: Filtering the relevant columns with the relevant rows
        relevant_scif = filtered_scif[filtered_scif[TEMPLATE_GROUP].isin(template_group)]

        # Step 7: Filter through the denominator param column with the denominator value and calculate the result and id
        # For the KPI 31: Empty Denominator Param
        denominator_scif = relevant_scif
        denominator_result = relevant_scif[FINAL_FACINGS].sum()
        denominator_id = denominator_scif[denominator_entity].mode()[0]

        # Step 8: Filter through the numerator param column with numerator value and calculate the numerator result
        if denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])].empty:
            numerator_result = 0
        else:
            numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]
            numerator_result = numerator_scif[FINAL_FACINGS].sum()

        # Step 9: Find the numerator_id
        numerator_id = self.own_manuf_fk

        # Step 10: Calculate the result
        result = (numerator_result / denominator_result) * 100

        # Step 11. Save the results in the database
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       numerator_result=numerator_result, denominator_id=denominator_id,
                                       denominator_result=denominator_result,
                                       result=result)
        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'numerator_result': numerator_result,
                       'denominator_id': denominator_id, 'denominator_result': denominator_result,
                       'result': result}

        return result_dict

    def calculate_bay_count(self, row):

        # Step 1: Read the excel rows to process the information(Common among all the sheets)
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        # Step 2: Read the excel rows unique to Bay Count Sheets
        numerator_param_1 = row[NUMERATOR_PARAM_1]
        numerator_value_1 = row[NUMERATOR_VALUE_1]
        product_type = self.sanitize_values(row[PRODUCT_TYPE])

        # Step 3: Establish the targets in order to calculate
        store_type = self.store_info[ADDITIONAL_ATTRIBUTE_2].iloc[0]
        facings_target, bay_count_target = self.calculate_targets_for_bay_count_kpi(store_type)

        # Step 4: Declaring the scif columns
        relevant_scif_columns = [TEMPLATE_GROUP, PRODUCT_NAME, PRODUCT_TYPE, FACINGS_IGN_STACK, PRODUCT_FK] + [
            numerator_param_1,
            numerator_entity,
            denominator_entity, SCENE_FK]

        # Step 5: Indexing the relevant_scif_columns in self.scif
        relevant_scif = self.scif[relevant_scif_columns]

        # Step 6: Filter by the Template Group
        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_GROUP].isin(template_group)]

        # Step 7: Filter by the Product Type
        relevant_scif = relevant_scif[relevant_scif[PRODUCT_TYPE].isin(product_type)]

        relevant_scif = relevant_scif[relevant_scif.product_name != 'Soda Other']

        # Step 8: Calculate the bay count
        product_in_scene = self.match_product_in_scene[['product_fk', 'bay_number', 'scene_fk']]

        bay_count_scif = relevant_scif.merge(product_in_scene, on=['product_fk', 'scene_fk'], how='right')
        bay_count_scif = bay_count_scif.dropna()
        # [[BAY_NUMBER, MANUFACTURER_NAME, PRODUCT_FK, FACINGS_IGN_STACK]]

        if bay_count_scif.empty:
            result = 'NULL'
            denominator_id = self.scif[denominator_entity].mode()[0]
            numerator_id = self.scif[numerator_entity].mode()[0]

            # Step 10. Save the results in the database
            self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                           denominator_id=denominator_id, result=result)
            return

        group_by_bay_number_scif = bay_count_scif.groupby('bay_number').nunique()[MANUFACTURER_NAME]

        bay_count = 0
        for bay in range(1, len(group_by_bay_number_scif) + 1):
            if group_by_bay_number_scif[bay] == 1:
                if 'TCCC' in set(bay_count_scif[bay_count_scif[BAY_NUMBER].isin([bay])][MANUFACTURER_NAME]):
                    bay_count = bay_count + 1

        # Step 9: Filter by the numerator param
        relevant_scif = relevant_scif[relevant_scif[numerator_param_1].isin([numerator_value_1])]
        facings = relevant_scif[FACINGS_IGN_STACK].sum()

        # Step 10: Calculate the scoring
        if facings >= facings_target:
            result = 100
        else:
            result = 100 * (facings / facings_target)

        if bay_count < bay_count_target:
            result = result - (2 / 15 * 100)

        # Step 11: Calculate the numerator entity and denominator entity
        numerator_id = relevant_scif[MANUFACTURER_FK].mode()[0]
        denominator_id = relevant_scif[TEMPLATE_FK].mode()[0]

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}

        return result_dict

    def calculate_per_bay_sos(self, row):
        # Step 1: Read the excel rows to process the information(Common among all the sheets)

        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]
        # Step 2: Read the rows to process unique per bay sos
        template_group = row[TASK_TEMPLATE_GROUP]
        template_name = row[TEMPLATE_NAME]
        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        numerator_param_1 = row[NUMERATOR_PARAM_1]
        numerator_value_1 = row[NUMERATOR_VALUE_1]

        # Step 3: Declaring the relevant columns for scif
        relevant_scif_columns = [TEMPLATE_GROUP, TEMPLATE_NAME, PRODUCT_FK, PRODUCT_NAME, PRODUCT_TYPE,
                                 MANUFACTURER_NAME, SCENE_FK, FACINGS_IGN_STACK,
                                 numerator_entity, denominator_entity, 'category']

        # Step 4: Index self.scif to get relevant_scif
        relevant_scif = self.scif[relevant_scif_columns]

        # relevant_scif.rename(columns= {'scene_id': 'scene_fk'}, inplace= True)

        # Step 5:
        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_GROUP].isin([template_group])]
        # Step 6:
        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_NAME].isin([template_name])]

        # Step 7:
        relevant_scif = relevant_scif[relevant_scif[PRODUCT_TYPE].isin(product_type)]

        # Step 8: Drop Soda Other because it was recognition issue
        relevant_scif = relevant_scif[relevant_scif.product_name != 'Soda Other']

        # Step 9:
        product_in_scene = self.match_product_in_scene[['product_fk', 'bay_number', 'scene_fk']]
        # bay_count_scif = relevant_scif.merge(product_in_scene, on='product_fk', how = 'left')
        bay_count_scif = relevant_scif.merge(product_in_scene, on=['product_fk', 'scene_fk'], how='right')
        bay_count_scif = bay_count_scif.dropna()
        # [[BAY_NUMBER, MANUFACTURER_NAME, PRODUCT_FK]]

        if bay_count_scif.empty:
            result = 'NULL'
            denominator_id = self.scif[denominator_entity].mode()[0]
            numerator_id = self.scif[numerator_entity].mode()[0]

            self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                           denominator_id=denominator_id, result=result)
            return

        group_by_bay_number_scif = bay_count_scif.groupby('bay_number').nunique()[MANUFACTURER_NAME]

        bay_count = 0
        for bay in range(1, len(group_by_bay_number_scif) + 1):
            if group_by_bay_number_scif[bay] == 1:
                if 'TCCC' in set(bay_count_scif[bay_count_scif[BAY_NUMBER].isin([bay])][MANUFACTURER_NAME]):
                    bay_count = bay_count + 1
                    break

        # Step 8:
        if bay_count > 0:
            relevant_scif = relevant_scif[relevant_scif[numerator_param_1].isin([numerator_value_1])]
            facings_count = relevant_scif[FACINGS_IGN_STACK].sum()
            if facings_count >= 25:
                result = 1
        else:
            result = 0

        # Step 9:
        denominator_id = relevant_scif[denominator_entity].mode()[0]
        numerator_id = relevant_scif[numerator_entity].mode()[0]

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}

        return result_dict

    def calculate_survey(self, row):

        # Step 1:
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        relevant_question_fk = self.sanitize_values(row[RELEVANT_QUESTION_FK])
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        survey_result = self.calculate_relevant_survey_result(relevant_question_fk)

        denominator_id = self.scif[denominator_entity].mode()[0]
        numerator_id = self.scif[numerator_entity].mode()[0]

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': survey_result}

        return result_dict

    def calculate_availability(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        template_name = self.sanitize_values(row[TEMPLATE_NAME])
        store_additional_attribute_2 = row[STORE_ADDITIONAL_ATTRIBUTE_2]
        product_type = row[PRODUCT_TYPE]
        product_short_name = self.sanitize_values(row[PRODUCT_SHORT_NAME])
        relevant_question_fk = row[RELEVANT_QUESTION_FK]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        store_type = self.store_info[ADDITIONAL_ATTRIBUTE_2].iloc[0]
        target = self.calculate_targets_for_availability_kpi(store_type)

        relevant_scif = self.scif[[PK, TEMPLATE_GROUP, TEMPLATE_NAME, PRODUCT_TYPE, PRODUCT_SHORT_NAME]]

        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_GROUP].isin(template_group)]
        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_NAME].isin(template_name)]
        relevant_scif = relevant_scif[relevant_scif[PRODUCT_TYPE].isin([product_type])]
        relevant_scif = relevant_scif[relevant_scif[PRODUCT_SHORT_NAME].isin(product_short_name)]

        result = 0
        if not relevant_scif.empty:
            unique_product_name_category = set(relevant_scif[PRODUCT_SHORT_NAME])
            if "Coca-Cola POS Other" in unique_product_name_category: # for kpi number 56
                result = 1
            else: # for kpi number 57
                if "Totem 1 CSD's" in unique_product_name_category:
                    result = result + .4
                if "Totem 2 CSD's" in unique_product_name_category:
                    result = result + .4
                if "Totem 3 CSD's" in unique_product_name_category:
                    result = result + .2

        if pd.notna(relevant_question_fk):
            calculation = self.calculate_relevant_availability_survey_result(relevant_question_fk) + result
            if calculation >= target:
                result = 1
            else:
                result = 0

        denominator_id = self.scif[denominator_entity].mode()[0]
        numerator_id = self.scif[numerator_entity].mode()[0]

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}

        return result_dict

    def store_wrong_data_for_parent_kpi_enfriador(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Enfriador')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_portafolio_y_precios(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Portafolio y Precios')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_plataformas(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Plataformas')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_comunicacion(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Comunicacion')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_primera_posicion(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Primera posicion')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_respeto(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Respeto')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_acomodo_por_bloques(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Acomodo por Bloques')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_bloques_colas_50(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Bloques Colas 50%')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_bloques_frutales_25(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Bloques Frutales 25%')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_comidas(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Comidas')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_plat_dinamicas_one(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Plat. Dinamicas 1')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def store_wrong_data_for_parent_kpi_plat_dinamicas_two(self):
        kpi_fk = self.common.get_kpi_fk_by_kpi_name('Plat. Dinamicas 2')
        numerator_id = self.scif['manufacturer_fk'].mode()[0]
        denominator_id = self.scif['template_fk'].mode()[0]
        self.common.write_to_db_result(kpi_fk, numerator_id=numerator_id,
                                       denominator_id=denominator_id, result=0)

    def sanitize_values(self, item):
        if pd.isna(item):
            return item
        else:
            if type(item) == int:
                return str(item)
            else:
                items = [x.strip() for x in item.split(',')]
                return items

    def delete_filter_nan(self, filters):
        filters = [filter for filter in filters if str(filter) != 'nan']
        return list(filters)

    @staticmethod
    def merge_two_dictionaries(dictionary_one, dictionary_two):
        final_dictionary = dictionary_one.copy()
        final_dictionary.update(dictionary_two)
        return final_dictionary

    def get_sub_cat_fk_from_sub_cat(self, sub_category):
        return self.all_products[self.all_products['sub_category'] == 'COLAS']['sub_category_fk'].iloc[0]

    @staticmethod
    def calculate_targets_for_bay_count_kpi(store_size):
        if store_size == 'Chico':
            facings_target = 56
            bay_count_target = 1
        elif store_size == 'Mediano':
            facings_target = 100
            bay_count_target = 2
        elif store_size == 'Grande':
            facings_target = 150
            bay_count_target = 3

        return facings_target, bay_count_target

    def calculate_relevant_survey_result(self, relevant_question_fk):
        result = 'NULL'
        for question_fk in relevant_question_fk:
            if result == 0:
                break

            if self.survey.check_survey_answer(('question_fk', question_fk), ('Si')):
                result = 1
            else:
                result = 0

        return result

    def calculate_relevant_availability_survey_result(self, relevant_question_fk):
        result = 0
        for question_fk in relevant_question_fk:
            if self.survey.check_survey_answer(('question_fk', question_fk), ('Si', 1, 2)):
                result = result + 1
        return result

    @staticmethod
    def calculate_targets_for_availability_kpi(store_size):
        if store_size == 'Chico':
            target = 1
        elif store_size == 'Mediano':
            target = 2
        elif store_size == 'Grande':
            target = 3

        return target