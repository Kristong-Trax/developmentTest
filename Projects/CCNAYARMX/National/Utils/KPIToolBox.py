from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment

import pandas as pd
import numpy as np
import os
import re
from datetime import datetime

__author__ = 'krishnat'

# Column Name
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
TASK_TEMPLATE_GROUP = 'Task/ Template Group'
TEMPLATE_NAME = 'template_name'
MANUFACTURER_NAME = 'manufacturer_name'
PRODUCT_TYPE = 'product_type'
PRODUCT_SHORT_NAME = 'product_short_name'
PRODUCT_WEIGHT = 'product_weight'
STORE_ADDITIONAL_ATTRIBUTE_2 = 'store_additional_attribute_2'
TAMANDO_DEL_PRODUCTO = 'TAMANO DEL PRODUCTO'
IGNORE_STACKING = 'Ignore Stacking'
KPI_FK_LEVEL2 = 'kpi_fk_lvl2'
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
UNIQUE_PRODUCTS_TARGETS = 'Unique Products Targets'
EXTERNAL_SHEET_NAMES = 'External Sheet Names'
NUMERO_DE_PUERTAS = 'Numero De Puertas'

# Sheet names
KPIS = 'KPIs'
SOS = 'SOS'
BLOCK_TOGETHER = 'Block Together'
SHARE_OF_EMPTY = 'Share of Empty'
BAY_COUNT = 'Bay Count'
PER_BAY_SOS = 'Per bay SOS'
SURVEY = 'Survey'
AVAILABILITY = 'Availability'
DISTRIBUTION = 'Distribution'
COMBO = 'Combo'
SCORING = 'Scoring'
PLATFORMAS = 'Platformas'
PLATFORMAS_SCORING = 'Platformas Scoring'
AVAILABILITY_COMBO = 'Availability Combo'
PLATAFORMAS_ASSORTMENT = 'Plataformas Assortment'
PLATAFORMAS_CONSTRAINTS = 'Plataformas Constraints'
MERCADEO_ASSORTMENT = 'Mercadeo Assortment'
MERCADEO_CONSTRAINTS = 'Mercadeo Constraints'
BONUSES_AND_PENALTIES = 'Bonuses and Penalties'
CATTMAN_ASSORTMENT = 'Cattman Assortment'
EJECUCION_SOMBRA = 'Ejecucion Sombra'
EJECUCION_SOMBRA_PREREQUISITE = 'Ejecucion Sombra Prerequisite'
NUMERO_DE_PUERTAS = 'Numero De Puertas'
POS_OPTIONS = 'POS Options'
TARGETS_AND_CONSTRAINTS = 'Targets and Constraints'
PLATFORMAS_SCORING_PREREQ = 'Platformas Scoring Prereq'
ASSORTMENTS = 'Assortment'
CONSTRAINTS = 'Constraints'

# Scif Filters
BRAND_FK = 'brand_fk'
PRODUCT_FK = 'product_fk'
FACINGS = 'facings'
FACINGS_IGN_STACK = 'facings_ign_stack'
FINAL_FACINGS = 'final_facings'
MANUFACTURER_FK = 'manufacturer_fk'
PK = 'pk'
PRODUCT_NAME = 'product_short_name'
ADDITIONAL_ATTRIBUTE_2 = 'additional_attribute_2'
SESSION_ID = 'session_id'
SCENE_ID = 'scene_id'
SCENE_FK = 'scene_fk'
TEMPLATE_FK = 'template_fk'
TEMPLATE_GROUP = 'template_group'

# Match Product In Scene
BAY_NUMBER = 'bay_number'

# Read the sheet
SHEETS = [SOS, BLOCK_TOGETHER, SHARE_OF_EMPTY, BAY_COUNT, PER_BAY_SOS, SURVEY, AVAILABILITY, DISTRIBUTION,
          COMBO, SCORING, PLATFORMAS, PLATFORMAS_SCORING, PLATFORMAS_SCORING_PREREQ,KPIS, AVAILABILITY_COMBO, EJECUCION_SOMBRA,
          EJECUCION_SOMBRA_PREREQUISITE, NUMERO_DE_PUERTAS]
POS_OPTIONS_SHEETS = [POS_OPTIONS, TARGETS_AND_CONSTRAINTS]
PORTAFOLIO_SHEETS = [ASSORTMENTS]
GENERAL_ASSORTMENTS_SHEETS = [PLATAFORMAS_ASSORTMENT, PLATAFORMAS_CONSTRAINTS, CATTMAN_ASSORTMENT, MERCADEO_ASSORTMENT,
                              MERCADEO_CONSTRAINTS]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'CCNayarTemplate2020Nacionalv0.6.xlsx')
POS_OPTIONS_TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                         'CCNayar_POS_Options_v11.xlsx')
PORTAFOLIO_Y_PRECIOUS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                          'CCNayarNational_Portafolios_y_Precios.xlsx')
GENERAL_ASSORTMENTS_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                                        'CCNayar_Assortment_Templates_V3.xlsx')


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


class NationalToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.common_v2 = common
        self.ps_data_provider = PsDataProvider(data_provider)
        self.block = Block(data_provider)
        self.templates = {}
        self.parse_template()
        self.att2 = self.store_info['additional_attribute_2'].iloc[0]
        self.match_product_in_scene = self.data_provider['matches']
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.survey = Survey(self.data_provider, output=output, ps_data_provider=self.ps_data_provider,
                             common=self.common_v2)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.platformas_data = self.generate_platformas_data()
        self.prereq = self.calculate_prereq_platformas_scoring()
        self.assortment = Assortment(self.data_provider, common=self.common)
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)
        for sheet in POS_OPTIONS_SHEETS:
            self.templates[sheet] = pd.read_excel(POS_OPTIONS_TEMPLATE_PATH, sheet_name=sheet)
        for sheet in PORTAFOLIO_SHEETS:
            self.templates[sheet] = pd.read_excel(PORTAFOLIO_Y_PRECIOUS_PATH, sheet_name=sheet)
        for sheet in GENERAL_ASSORTMENTS_SHEETS:
            self.templates[sheet] = pd.read_excel(GENERAL_ASSORTMENTS_PATH, sheet_name=sheet)

    def main_calculation(self):
        relevant_kpi_template = self.templates[KPIS]
        relevant_kpi_template = relevant_kpi_template[(relevant_kpi_template[STORE_ADDITIONAL_ATTRIBUTE_2].isnull()) |
                                                      (relevant_kpi_template[STORE_ADDITIONAL_ATTRIBUTE_2].str.contains(
                                                          self.att2))
                                                      ]
        foundation_kpi_types = [BAY_COUNT, SOS, PER_BAY_SOS, BLOCK_TOGETHER, AVAILABILITY, SURVEY, DISTRIBUTION,
                                SHARE_OF_EMPTY, AVAILABILITY_COMBO, EJECUCION_SOMBRA]

        foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(foundation_kpi_types)]
        platformas_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE] == PLATFORMAS_SCORING]
        combo_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE] == COMBO]
        scoring_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE] == SCORING]

        self._calculate_kpis_from_template(foundation_kpi_template)
        self._calculate_kpis_from_template(platformas_kpi_template)
        self._calculate_kpis_from_template(combo_kpi_template)
        self._calculate_kpis_from_template(scoring_kpi_template)

        self.save_results_to_db()
        return

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df.loc[~self.results_df['identifier_parent'].isnull(), 'should_enter'] = True
        # set result to NaN for records that do not have a parent
        # identifier_results = self.results_df[self.results_df['result'].notna()]['identifier_result'].unique().tolist()
        # self.results_df['result'] = self.results_df.apply(
        #     lambda row: pd.np.nan if (pd.notna(row['identifier_parent']) and row[
        #         'identifier_parent'] not in identifier_results) else row['result'], axis=1)
        self.results_df['result'] = self.results_df.apply(
            lambda row: row['result'] if (
                    pd.notna(row['identifier_parent']) or pd.notna(row['identifier_result'])) else np.nan, axis=1)
        # get rid of 'not applicable' results
        self.results_df.dropna(subset=['result'], inplace=True)
        self.results_df.fillna(0, inplace=True)
        results = self.results_df.to_dict('records')
        for result in results:
            self.write_to_db(**result)

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self._get_calculation_function_by_kpi_type(row[KPI_TYPE])
            try:
                kpi_row = self.templates[row[KPI_TYPE]][
                    self.templates[row[KPI_TYPE]][KPI_NAME].str.encode('utf-8') == row[KPI_NAME].encode('utf-8')].iloc[
                    0]
            except IndexError:
                pass
            result_data = calculation_function(kpi_row)
            if result_data:
                if isinstance(result_data, dict):
                    weight = row['Score']
                    if weight and pd.notna(weight) and pd.notna(result_data['result']):
                        if row[KPI_TYPE] == SCORING and 'score' not in result_data.keys():
                            result_data['score'] = weight * result_data['result']
                        elif row[KPI_TYPE] != SCORING:
                            result_data['score'] = weight * result_data['result']
                    parent_kpi_name = self._get_parent_name_from_kpi_name(result_data['kpi_name'])
                    if parent_kpi_name and 'identifier_parent' not in result_data.keys():
                        result_data['identifier_parent'] = parent_kpi_name
                    if 'identifier_result' not in result_data.keys():
                        result_data['identifier_result'] = result_data['kpi_name']
                    if result_data['result'] <= 1:
                        result_data['result'] = result_data['result'] * 100
                    self.results_df.loc[len(self.results_df), result_data.keys()] = result_data
                else:  # must be a list
                    for result in result_data:
                        weight = row['Score']
                        if weight and pd.notna(weight) and pd.notna(result['result']):
                            if row[KPI_TYPE] == SCORING and 'score' not in result.keys():
                                result['score'] = weight * result['result']
                            elif row[KPI_TYPE] != SCORING:
                                result['score'] = weight * result['result']
                        parent_kpi_name = self._get_parent_name_from_kpi_name(result['kpi_name'])
                        if parent_kpi_name and 'identifier_parent' not in result.keys():
                            result['identifier_parent'] = parent_kpi_name
                        if 'identifier_result' not in result.keys():
                            result['identifier_result'] = result['kpi_name']
                        if result['result'] <= 1:
                            result['result'] = result['result'] * 100
                        self.results_df.loc[len(self.results_df), result.keys()] = result

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == SOS:
            return self.calculate_sos
        elif kpi_type == BAY_COUNT:
            return self.calculate_bay_count
        elif kpi_type == PER_BAY_SOS:
            return self.calculate_per_bay_sos
        elif kpi_type == BLOCK_TOGETHER:
            return self.calculate_block_together
        elif kpi_type == AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == SURVEY:
            return self.calculate_survey
        elif kpi_type == DISTRIBUTION:
            return self.calculate_assortment
        elif kpi_type == SHARE_OF_EMPTY:
            return self.calculate_share_of_empty
        elif kpi_type == COMBO:
            return self.calculate_combo
        elif kpi_type == SCORING:
            return self.calculate_scoring
        elif kpi_type == PLATFORMAS_SCORING:
            return self.calculate_platformas_scoring
        elif kpi_type == AVAILABILITY_COMBO:
            return self.calculate_availability_combo
        elif kpi_type == NUMERO_DE_PUERTAS:
            return self.calculate_numero_de_puertas
        elif kpi_type == EJECUCION_SOMBRA:
            return self.calculate_ejecucion_sombra

    def calculate_ejecucion_sombra(self, row):
        return_holder = self._get_kpi_name_and_fk(row)
        result = self.calculate_prereq_ejecucion()
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'result': 0}
        if result:
            brand_fk = self.all_products[self.all_products.brand_name.isin([result[0][2]])].brand_fk.iloc[0]  # denom id
            product_fk = self.all_products[self.all_products.product_short_name.isin([result[0][1]])].product_fk.iloc[
                0]  # num id
            result_dict['numerator_id'] = product_fk
            result_dict['denominator_id'] = brand_fk
            result = result[0][0]
            final_result = float(result) / 5
            result_dict['result'] = final_result
        return result_dict

    def calculate_prereq_ejecucion(self):
        prereq_ejecucion = self.templates[EJECUCION_SOMBRA_PREREQUISITE]
        relevant_scif = self.filter_df(self.scif, {'template_name': prereq_ejecucion.scene_type.iloc[0]})
        rival_brand_names_in_scif = relevant_scif[
            relevant_scif.brand_name.isin(prereq_ejecucion.brand_name.unique())].brand_name.unique()
        relevant_prereq_ejecucion = prereq_ejecucion.loc[prereq_ejecucion.brand_name.isin(rival_brand_names_in_scif)]

        container_for_result = []
        for row in relevant_prereq_ejecucion.itertuples():
            scif_df = relevant_scif[relevant_scif.product_short_name.isin([row.product_short_name])]
            if not scif_df.empty:
                points = 3 if scif_df.facings.iloc[0] >= row.minimum_CC_SKU_facings_required else 0
                rel_scif = scif_df[scif_df.product_short_name.isin([row.product_short_name_POSM])]
                if not rel_scif.empty:
                    points = 5 if rel_scif.facings.iloc[0] >= row.minimum_POS_facings_required else 3
                if not container_for_result:
                    container_for_result.append([points, row.product_short_name, row.brand_name])
                elif container_for_result[0][0] < points:
                    container_for_result.pop(0)
                    container_for_result.append([points, row.product_short_name, row.brand_name])
        return container_for_result

    def calculate_platformas_scoring(self, row):
        result_list = []
        result_container = self._get_kpi_name_and_fk(row)
        kpi_name = result_container[0]
        kpi_fk = result_container[1]
        df = self.prereq[self.prereq['KPI Name'] == kpi_name] if not self.prereq.empty else pd.DataFrame()
        if not df.empty:
            df = df.sort_values(by=['actual_score'], ascending=False).head(1)

        plat_template = self.templates[PLATFORMAS]
        plataformas = self.platformas_data
        relevant_platformas = plataformas[(plataformas['Platform Name'].isin(df['Platform'].values)) & (
                plataformas.consumed == 'no')] if not df.empty else pd.DataFrame()
        if not relevant_platformas.empty:
            relevant_platformas = df.merge(relevant_platformas, how='left', on='scene_id')

        for i, child_row in plat_template[plat_template[PARENT_KPI] == kpi_name].iterrows():
            child_kpi_fk = self.get_kpi_fk_by_kpi_type(child_row[KPI_NAME])
            if df.empty:
                child_result = 0
                scene_id = 0
                child_score = 0
            elif not relevant_platformas.empty:
                child_result = relevant_platformas[child_row['Data_Column']].iloc[0]
                if child_row['dependency_on_scoring'] == 'y':
                    if not np.all(relevant_platformas[
                                      ['Mandatory SKUs found', 'Minimum facings met', 'Survey Question']].values):
                        child_score = 0
                    else:
                        child_score = (child_row.parent_score_portion * child_result * df.Score).iloc[0]
                else:
                    child_score = (child_row.parent_score_portion * child_result * df.Score).iloc[0]
                scene_id = relevant_platformas['scene_id'].iloc[0]
                self.platformas_data.loc[relevant_platformas.index.values[0], 'consumed'] = 'yes'
            else:
                child_result = 0
                scene_id = 0
                child_score = 0

            child_result_dict = {'kpi_name': child_row[KPI_NAME], 'kpi_fk': child_kpi_fk,
                                 'numerator_id': self.own_manuf_fk, 'denominator_id': self.store_id,
                                 'denominator_result': scene_id,
                                 'result': child_result, 'score': child_score}
            result_list.append(child_result_dict)

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk}
        if df.empty:
            result_dict['result'] = 0
            result_dict['score'] = 0
        else:
            result_dict['denominator_id'] = df.scene_id.iloc[0]
            result_dict['result'] = df.result.iloc[0]
            result_dict['score'] = df.actual_score.iloc[0]

        result_list.append(result_dict)
        return result_list
        # results_list = []
        # kpi_name = row[KPI_NAME]
        # kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        # relevant_platforms = self.sanitize_values(row['Platform'])
        # relevant_platformas_data = \
        #     self.platformas_data[(self.platformas_data['Platform Name'].isin(relevant_platforms)) &
        #                          (self.platformas_data['consumed'] == 'no')]
        # platformas_template = self.templates[PLATFORMAS]
        # platformas_template = platformas_template[(platformas_template[PARENT_KPI] == kpi_name) &
        #                                           (platformas_template[STORE_ADDITIONAL_ATTRIBUTE_2].str.contains(
        #                                               self.att2))]
        # total_score = 0
        # for i, child_row in platformas_template.iterrows():
        #     child_kpi_fk = self.get_kpi_fk_by_kpi_type(child_row[KPI_NAME])
        #     if not relevant_platformas_data.empty:
        #         child_result = relevant_platformas_data[child_row['data_column']].iloc[0]
        #         scene_id = relevant_platformas_data['scene_id'].iloc[0]
        #         score = child_row['Score']
        #         total_score += score
        #         self.platformas_data.loc[relevant_platformas_data.index.values[0], 'consumed'] = 'yes'
        #     else:
        #         child_result = 0
        #         scene_id = 0
        #         score = 0
        #     result_dict = {'kpi_name': child_row[KPI_NAME], 'kpi_fk': child_kpi_fk,
        #                    'numerator_id': self.own_manuf_fk, 'denominator_id': self.store_id,
        #                    'denominator_result': scene_id,
        #                    'result': child_result, 'score': score}
        #     results_list.append(result_dict)
        #
        # if kpi_name != 'Precios en cooler-Nacional':
        #     if relevant_platformas_data.empty:
        #         result = total_score
        #         scene_id = 0
        #     else:
        #         result = total_score
        #         scene_id = relevant_platformas_data['scene_id'].iloc[0]
        #
        #     result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk,
        #                    'numerator_id': self.own_manuf_fk, 'denominator_id': self.store_id,
        #                    'denominator_result': scene_id,
        #                    'result': result, 'score': total_score}
        #     results_list.append(result_dict)

    def calculate_scoring(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        numerator_id = self.own_manuf_fk
        denominator_id = self.store_id

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id}

        component_kpis = self.sanitize_values(row['Component KPIs'])
        dependency_kpis = self.sanitize_values(row['Dependency'])
        relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
        passing_results = relevant_results[(relevant_results['result'] != 0) &
                                           (relevant_results['result'].notna()) &
                                           (relevant_results['score'] != 0)]
        nan_results = relevant_results[relevant_results['result'].isna()]
        if len(relevant_results) > 0 and len(relevant_results) == len(nan_results):
            result_dict['result'] = pd.np.nan
        elif row['Component aggregation'] == 'one-passed':
            if len(relevant_results) > 0 and len(passing_results) > 0:
                result_dict['result'] = 1
            else:
                result_dict['result'] = 0
        elif row['Component aggregation'] == 'sum':
            if len(relevant_results) > 0:
                result_dict['score'] = relevant_results['score'].sum()
                if 'result' not in result_dict.keys():
                    if row['score_based_result'] == 'y':
                        result_dict['result'] = 0 if result_dict['score'] == 0 else result_dict['score'] / row['Score']
                    elif row['composition_based_result'] == 'y':
                        result_dict['result'] = 0 if passing_results.empty else float(len(passing_results)) / len(
                            relevant_results)
                    else:
                        result_dict['result'] = result_dict['score']
            else:
                result_dict['score'] = 0
                if 'result' not in result_dict.keys():
                    result_dict['result'] = result_dict['score']
        if dependency_kpis and dependency_kpis is not pd.np.nan:
            dependency_results = self.results_df[self.results_df['kpi_name'].isin(dependency_kpis)]
            passing_dependency_results = dependency_results[dependency_results['result'] != 0]
            if len(dependency_results) > 0 and len(dependency_results) == len(passing_dependency_results):
                result_dict['result'] = 1
            else:
                result_dict['result'] = 0

        return result_dict

    def calculate_combo(self, row):
        component_kpis = self.sanitize_values(row['Prerequisite'])
        relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
        passing_results = relevant_results[(relevant_results['result'] != 0) &
                                           (relevant_results['result'].notna()) &
                                           (relevant_results['score'] != 0)]
        nan_results = relevant_results[relevant_results['result'].isna()]
        if len(passing_results) == 0 and not nan_results.empty:
            result = pd.np.nan
        elif len(relevant_results) > 0 and len(relevant_results) == len(passing_results):
            result = 1
        else:
            result = 0

        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        numerator_id = self.own_manuf_fk
        denominator_id = self.store_id

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}
        return result_dict

    def calculate_numero_de_puertas(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        template_name = self.sanitize_values(row[TEMPLATE_NAME])

        relevant_scif = self.scif[self.scif[TEMPLATE_NAME].isin(template_name)][
            [PK, SESSION_ID, TEMPLATE_FK, TEMPLATE_NAME, PRODUCT_FK, SCENE_FK]]

        product_in_scene = self.match_product_in_scene[['bay_number', 'scene_fk']]

        bay_count_scif = relevant_scif.merge(product_in_scene, on=['scene_fk'], how='right')
        bay_count_scif.dropna(inplace=True)

        for relevant_template_fk in set(bay_count_scif[TEMPLATE_FK]):
            # Result related the number of bays in a specifc template
            relevant_template_fk_scif = bay_count_scif[bay_count_scif[TEMPLATE_FK].isin([relevant_template_fk])]
            count_of_bays_in_template = 0

            for relevant_scene_fk in set(relevant_template_fk_scif[SCENE_FK]):
                bay_count = len(set(
                    relevant_template_fk_scif[relevant_template_fk_scif[SCENE_FK].isin([relevant_scene_fk])][
                        BAY_NUMBER]))
                count_of_bays_in_template = count_of_bays_in_template + bay_count

            self.common.write_to_db_result(fk=kpi_fk, numerator_id=relevant_template_fk,
                                           denominator_id=self.store_id,
                                           result=count_of_bays_in_template)

    def _get_parent_name_from_kpi_name(self, kpi_name):
        template = self.templates[KPIS]
        parent_kpi_name = \
            template[template[KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')][PARENT_KPI].iloc[0]
        if parent_kpi_name and pd.notna(parent_kpi_name):
            return parent_kpi_name
        else:
            return None

    def generate_platformas_data(self):
        platformas_data = pd.DataFrame(columns=['scene_id', 'Platform Name', 'POS option present',
                                                'Mandatory SKUs found', 'Minimum facings met', 'Coke purity',
                                                'consumed'])
        for scene in self.scif[['scene_id', 'template_name']].drop_duplicates().itertuples():
            scene_scif = self.scif[self.scif['scene_id'] == scene.scene_id]
            product_names_in_scene = \
                set(scene_scif['product_short_name'].unique().tolist())

            relevant_pos_template = self.templates[POS_OPTIONS]
            relevant_pos_template = relevant_pos_template[
                relevant_pos_template['template_name'].str.encode('utf-8') == scene.template_name.encode('utf-8')]

            # check the 'POS Option' activation, i.e. 'copete'
            pos_option_found = 0  # False
            for index, relevant_row in relevant_pos_template.iterrows():
                if pos_option_found:
                    break
                groups = self._get_groups(relevant_row.dropna(), 'POS Option')
                for group in groups:
                    if all(product in product_names_in_scene for product in group):
                        pos_option_found = 1  # True
                        platform_name = relevant_row['Platform Name']
                        platform_row = relevant_row.copy()
                        break
            if not pos_option_found:
                continue

            targets_and_constraints = self._get_relevant_targets_and_constraints(platform_name, scene.template_name)
            if targets_and_constraints.empty:
                # this is needed for the Precios en Cooler KPI
                platformas_data.loc[len(platformas_data), platformas_data.columns.tolist()] = [
                    scene.scene_id, platform_name, pos_option_found, 0, 0, 0, 'no'
                ]
                continue

            # calculate the 'empaques' data
            assortment_groups = self._get_groups(platform_row.dropna(), 'Assortment')
            if pd.notna(targets_and_constraints['excluding_invasion'].values[0]):
                removal_of_assortment_index = map(int, re.findall('\d',targets_and_constraints['excluding_invasion'].values[0]))[0] - 1
                assortment_groups.pop(removal_of_assortment_index)
            mandatory_skus_found = 1  # True
            for assortment in assortment_groups:
                if not any(product in product_names_in_scene for product in assortment):
                    mandatory_skus_found = 0  # False
                    break

            # this should be refactored to be more programmatic
            if targets_and_constraints['Assortment_Facings_Constraints'].iloc[0] == 'Assortment_2>Assortment_1':
                assortment_1_facings = \
                    scene_scif[scene_scif['product_short_name'].isin(assortment_groups[0])]['facings'].sum()
                assortment_2_facings = \
                    scene_scif[scene_scif['product_short_name'].isin(assortment_groups[1])]['facings'].sum()
                if assortment_1_facings >= assortment_2_facings:
                    mandatory_skus_found = 0

            # calculate the 'botellas' data
            assortment_groups = self._get_groups(platform_row.dropna(), 'Assortment')
            total_facings = scene_scif[scene_scif['product_short_name'].isin(
                [product for sublist in assortment_groups for product in sublist])]['facings'].sum()
            if total_facings >= targets_and_constraints['Facings_target'].iloc[0]:
                minimum_facings_met = 1  # True
            else:
                minimum_facings_met = 0  # False

            # calculate the coke purity (coke SOS) of this scene
            # Terrible logic. Need to change later
            coke_purity_assortment = np.delete(assortment_groups, -1) if pd.notna(
                targets_and_constraints.excluding_invasion.values[0]) else assortment_groups
            coke_purity_for_scene = self._get_coke_purity_for_scene(scene_scif, coke_purity_assortment)

            platformas_data.loc[len(platformas_data), platformas_data.columns.tolist()] = [
                scene.scene_id, platform_name, pos_option_found, mandatory_skus_found,
                minimum_facings_met, coke_purity_for_scene, 'no'
            ]

        platformas_data['passing_results'] = platformas_data['POS option present'] + \
                                             platformas_data['Mandatory SKUs found'] + \
                                             platformas_data['Minimum facings met'] + \
                                             platformas_data['Coke purity']
        platformas_data.sort_values(by=['passing_results'], ascending=False, inplace=True)
        unique_scenes_in_platformas_data = platformas_data.scene_id.values
        survey_results = self.get_survey_results_for_POS(unique_scenes_in_platformas_data, 28)
        platformas_data = platformas_data.merge(survey_results[['scene_fk', 'selected_option_text']], how='left',
                                                left_on='scene_id', right_on='scene_fk').drop(
            columns=['scene_fk']).fillna(0).rename(columns={'selected_option_text': 'Survey Question'})
        return platformas_data

    def get_survey_results_for_POS(self, unique_scenes, relevant_question):
        survey_answer = self.get_scene_survey_response()
        relevant_survey_ansewrs_for_scene = survey_answer[
            survey_answer.scene_fk.isin(unique_scenes) & survey_answer.question_fk.isin([relevant_question])]
        relevant_survey_ansewrs_for_scene.selected_option_text = np.where(
            relevant_survey_ansewrs_for_scene.selected_option_text == 'Si', 1, 0)
        return relevant_survey_ansewrs_for_scene

    def _get_coke_purity_for_scene(self, scene_scif, assortment_groups):
        sku_scif = scene_scif[scene_scif['product_type'].isin(['SKU'])]
        other_scif = scene_scif[scene_scif['product_type'].isin(['Other']) &
                                scene_scif['manufacturer_fk'] == self.own_manuf_fk]
        relevant_scif = pd.concat([sku_scif, other_scif])
        scene_products = relevant_scif['product_short_name'].unique().tolist()
        flat_assortment = [product for subgroup in assortment_groups for product in subgroup]
        if any(product not in flat_assortment for product in scene_products):
            return 0
        else:
            return 1

    def _get_relevant_targets_and_constraints(self, platform_name, template_name):
        relevant_template = self.templates[TARGETS_AND_CONSTRAINTS]

        relevant_template = relevant_template[(relevant_template['Platform Name'].str.encode('utf-8') ==
                                               platform_name.encode('utf-8')) &
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

    def calculate_availability_combo(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])

        relevant_scif = self.scif[self.scif[TEMPLATE_GROUP].isin(template_group)]
        if relevant_scif.empty:
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
            return result_dict

        relevant_product_names = set(relevant_scif['product_short_name'].unique().tolist())

        # check the 'POS Option' activation
        result = 1  # start as passing
        pos_option_found = 0  # False
        groups = self._get_groups(row, 'POS Option')
        for group in groups:
            if all(product in relevant_product_names for product in group):
                pos_option_found = 1  # True
                break
        if not pos_option_found:
            result = 0
        else:
            unique_product_targets = row[UNIQUE_PRODUCTS_TARGETS]
            sub_category_targets = self._get_target_mapping(unique_product_targets)

            for sub_category, target in sub_category_targets.iteritems():
                unique_skus_by_category = \
                    len(relevant_scif[relevant_scif['sub_category'] == sub_category][
                            'product_short_name'].unique().tolist())
                if unique_skus_by_category < target:
                    result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict

    @staticmethod
    def _get_target_mapping(column_value):
        target_dict = {}
        for pair in column_value.split(','):
            key, value = pair.split(';')
            target_dict.update({key: int(value)})
        return target_dict

    def calculate_assortment(self, row):
        return_holder = self._get_kpi_name_and_fk(row)
        kpi_name = return_holder[0]
        external_sheet_name = self.sanitize_values(row[EXTERNAL_SHEET_NAMES])
        relevant_scif = self._filter_scif(row, self.scif)

        if len(external_sheet_name) > 1:
            total_sum_of_bay_in_scenes = self.find_the_total_number_of_bays_in_relevant_scene(relevant_scif,
                                                                                              self.match_product_in_scene)
            assortment_sheet_name = external_sheet_name[0]
            constraints_sheet_name = external_sheet_name[1]

            final_assortment = self.filter_assortments(assortment_sheet_name, kpi_name)
            final_contraints = self.filter_constraints(self.templates[constraints_sheet_name], kpi_name,
                                                       total_sum_of_bay_in_scenes)

            result = self.calculate_assortment_passed_if_constraints(final_contraints, final_assortment, relevant_scif)
        else:
            assortment_sheet_name = external_sheet_name[0]
            final_assortment = self.filter_assortments(assortment_sheet_name, kpi_name, filter_att2=True)
            result = self.calculate_assortment_passsed_no_constraints(final_assortment, relevant_scif)

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': return_holder[1], 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict
        # kpi_name = row[KPI_NAME]
        # kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        # portafolio_y_precious_data = self.templates[ASSORTMENTS]
        # portafolio_y_precious_data = portafolio_y_precious_data[
        #     portafolio_y_precious_data[KPI_NAME].isin([kpi_name]) & portafolio_y_precious_data[
        #         STORE_ADDITIONAL_ATTRIBUTE_2].str.contains(self.att2)].iloc[0]
        #
        #
        # relevant_required_assortments = np.array(self._get_groups(portafolio_y_precious_data, 'assortment'))
        #
        # all_products_needed = self.sanitize_values(portafolio_y_precious_data.all_products_needed) if pd.notna(
        #     portafolio_y_precious_data.all_products_needed) else None
        #
        # two_unique_products_needed = portafolio_y_precious_data.two_unique_products_needed if pd.notna(
        #     portafolio_y_precious_data.two_unique_products_needed) else None
        #
        # result_dict = {}
        # for i in range(len(relevant_required_assortments)):
        #     result_of_current_assortment = sum(np.in1d(relevant_required_assortments[i], self.scif.product_short_name))
        #     if all_products_needed and 'assortment{}'.format(i + 1) in all_products_needed:
        #         result_dict['assortment{}'.format(i + 1)] = result_of_current_assortment
        #     elif two_unique_products_needed and 'assortment{}'.format(i + 1) in two_unique_products_needed:
        #         if result_of_current_assortment >= 2:
        #             restricted_result = 2
        #         elif result_of_current_assortment == 1:
        #             restricted_result = 1
        #         else:
        #             restricted_result = 0
        #         result_dict['assortment{}'.format(i + 1)] = restricted_result
        #     else:
        #         result_dict['assortment{}'.format(i + 1)] = 1 if result_of_current_assortment >= 1 else 0
        #
        # numerator_id = self.scif[PRODUCT_FK].iat[0]
        # denominator_id = self.scif.sub_category_fk.iat[0]
        #
        #
        # result = float(np.sum(result_dict.values())) / portafolio_y_precious_data.unique_facings_target
        #
        # result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
        #                'denominator_id': denominator_id,
        #                'result': result}
        #
        # return result_dict

    def filter_assortments(self, assortment_sheet_name, kpi_name, filter_att2=False):
        assortmemt_df = self.templates[assortment_sheet_name]
        assortmemt_df = assortmemt_df[assortmemt_df[KPI_NAME].isin([kpi_name])]
        if filter_att2:
            assortmemt_df = assortmemt_df[assortmemt_df.additional_attribute_2.str.contains(self.att2)]
        final_assortment = np.array(self._get_groups(assortmemt_df.iloc[0], 'assortment'))
        return final_assortment

    @staticmethod
    def calculate_assortment_passsed_no_constraints(final_assortment, scif):
        assortment_passed = 0
        for assortment in final_assortment:
            check_assortment_in_df = np.in1d(scif.product_short_name, assortment).sum()
            if check_assortment_in_df:
                assortment_passed = assortment_passed + 1
        result = float(assortment_passed) / len(final_assortment)
        return result

    @staticmethod
    def filter_constraints(constraints_template, kpi_name, total_sum_of_bay_in_scenes):
        constraints_df = constraints_template
        constraints_df = constraints_df[constraints_df[KPI_NAME].isin([kpi_name])]
        constraints_df = constraints_df[constraints_df.KO_doors.isin([5])] if total_sum_of_bay_in_scenes >= 5 else \
            constraints_df[constraints_df.KO_doors.isin([total_sum_of_bay_in_scenes])]

        relevant_columns_in_constraints_df = [item for item in constraints_df.columns if "assortment" in item]
        constraints_df = constraints_df[relevant_columns_in_constraints_df]
        final_constraints = constraints_df.values[0]
        return final_constraints

    @staticmethod
    def find_the_total_number_of_bays_in_relevant_scene(scif, mpis):
        total_bays = scif.merge(mpis, how='left', left_on='scene_id', right_on='scene_fk').groupby('scene_id').agg(
            {'bay_number': 'max'}).bay_number.sum()
        return total_bays

    @staticmethod
    def calculate_assortment_passed_if_constraints(constraints_df, assortment_df, scif):
        constraints_df = constraints_df[~ np.isnan(constraints_df)]
        assortment_passed = 0
        for facing_constraint, required_assortment in zip(constraints_df, assortment_df):
            total_facings_for_this_sum = scif[
                scif.product_short_name.isin(required_assortment)].facings.sum()
            if total_facings_for_this_sum >= facing_constraint:
                assortment_passed = assortment_passed + 1
        result = float(assortment_passed) / len(constraints_df)
        return result

    def calculate_sos(self, row):
        '''
        :param row: READS THE LINE FROM THE TEMPLATE
        :return: the sum of numerator scif[final_facings] over the sum of denominator scif[final_facings]
        '''

        # REMINDER Filter scif by additional scene type column
        # Waiting on Session with with scene type(template_name) with Enfriador Dedicado JDV

        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        template_name = self.sanitize_values(row[TEMPLATE_NAME])
        numerator_value1 = row[NUMERATOR_VALUE_1]
        denominator_value1 = row[DENOMINATOR_VALUE_1]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        target = row['target']
        numerator_param1 = row[NUMERATOR_PARAM_1]
        denominator_param1 = row[DENOMINATOR_PARAM_1]
        ignore_stacking = row[IGNORE_STACKING]
        additional_scene_type = row[ACTIVATION_SCENE_TYPE]

        if pd.notna(additional_scene_type):
            if additional_scene_type in set(self.scif.template_name):
                relevant_scif_columns = [PK, SESSION_ID, TEMPLATE_GROUP, TEMPLATE_NAME, PRODUCT_TYPE, FACINGS,
                                         FACINGS_IGN_STACK] + \
                                        [denominator_entity, numerator_entity] + self.delete_filter_nan(
                    [numerator_param1, denominator_param1])

                filtered_scif = self.scif[relevant_scif_columns]

                if pd.isna(ignore_stacking):
                    filtered_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
                    filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
                elif ignore_stacking == 'Y':
                    filtered_scif = filtered_scif.drop(columns=[FACINGS])
                    filtered_scif = filtered_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})

                if pd.notna(any([product_type])):
                    filtered_scif = filtered_scif[filtered_scif[PRODUCT_TYPE].isin(product_type)]

                if template_group is not pd.np.nan:
                    filtered_scif = filtered_scif[filtered_scif[TEMPLATE_GROUP].isin(template_group)]

                if template_name is not pd.np.nan:
                    filtered_scif = filtered_scif[filtered_scif[TEMPLATE_NAME].isin(template_name)]

                if filtered_scif.empty:
                    result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                    return result_dict

                if pd.notna(denominator_param1):
                    denominator_scif = filtered_scif[filtered_scif[denominator_param1].isin([denominator_value1])]

                    if denominator_scif.empty:
                        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                        return result_dict

                    denominator_id = \
                        self.all_products[self.all_products[denominator_param1].isin([denominator_value1])][
                            denominator_entity].mode()[0]

                    denominator_result = denominator_scif[FINAL_FACINGS].sum()
                else:
                    denominator_scif = filtered_scif
                    denominator_id = denominator_scif[denominator_entity].mode()[0]
                    denominator_result = denominator_scif[FINAL_FACINGS].sum()

                if pd.notna(numerator_param1):
                    # Sometimes the filter below overfilters, and the df is empty
                    if (denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]).empty:
                        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                        return result_dict
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

                result = (numerator_result / denominator_result)
                score = self.calculate_score_for_sos(target, result)

                result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                               'numerator_result': numerator_result,
                               'denominator_id': denominator_id, 'denominator_result': denominator_result,
                               'result': result, 'score': score}

                return result_dict

            else:

                result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                return result_dict
        else:
            relevant_scif_columns = [PK, SESSION_ID, TEMPLATE_GROUP, TEMPLATE_NAME, PRODUCT_TYPE, FACINGS,
                                     FACINGS_IGN_STACK] + \
                                    [denominator_entity, numerator_entity] + self.delete_filter_nan(
                [numerator_param1, denominator_param1])

            filtered_scif = self.scif[relevant_scif_columns]

            if pd.isna(ignore_stacking):
                filtered_scif = filtered_scif.drop(columns=[FACINGS_IGN_STACK])
                filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
            elif ignore_stacking == 'Y':
                filtered_scif = filtered_scif.drop(columns=[FACINGS])
                filtered_scif = filtered_scif.rename(columns={FACINGS_IGN_STACK: FINAL_FACINGS})

            if pd.notna(any([product_type])):
                filtered_scif = filtered_scif[filtered_scif[PRODUCT_TYPE].isin(product_type)]

            if template_group is not pd.np.nan:
                filtered_scif = filtered_scif[filtered_scif[TEMPLATE_GROUP].isin(template_group)]

            if template_name is not pd.np.nan:
                filtered_scif = filtered_scif[filtered_scif[TEMPLATE_NAME].isin(template_name)]

            if pd.notna(denominator_param1):
                denominator_scif = filtered_scif[filtered_scif[denominator_param1].isin([denominator_value1])]
                if denominator_scif.empty:
                    result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                    return result_dict
                denominator_id = \
                    self.all_products[self.all_products[denominator_param1].isin([denominator_value1])][
                        denominator_entity].mode()[0]

                denominator_result = denominator_scif[FINAL_FACINGS].sum()
            else:
                denominator_scif = filtered_scif
                if denominator_scif.empty:
                    result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                    return result_dict
                denominator_id = denominator_scif[denominator_entity].mode()[0]
                denominator_result = denominator_scif[FINAL_FACINGS].sum()

            if pd.notna(numerator_param1):
                # Sometimes the filter below overfilters, and the df is empty
                if (denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]).empty:
                    result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
                    return result_dict
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

            result = (numerator_result / denominator_result)
            score = self.calculate_score_for_sos(target, result)

            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'numerator_result': numerator_result,
                           'denominator_id': denominator_id, 'denominator_result': denominator_result,
                           'result': result, 'score': score}

            return result_dict

    def calculate_block_together(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]
        product_type = row[PRODUCT_TYPE]
        template_name = self.sanitize_values(row[TEMPLATE_NAME])
        manufacturer_name = [row[MANUFACTURER_NAME]]
        tamano_del_producto = row[TAMANDO_DEL_PRODUCTO]
        sub_category = self.sanitize_values(row[SUB_CATEGORY])
        iterate_by = row[ITERATE_BY]

        relevant_scif_columns = [PK, SESSION_ID, PRODUCT_FK, PRODUCT_NAME, TEMPLATE_GROUP, TEMPLATE_NAME, BRAND_FK,
                                 MANUFACTURER_NAME,
                                 TAMANDO_DEL_PRODUCTO, SUB_CATEGORY, PRODUCT_TYPE] + \
                                [denominator_entity, numerator_entity, denominator_entity, SCENE_FK]

        relevant_scif = self.scif[relevant_scif_columns]

        product_in_scene = self.match_product_in_scene[[PRODUCT_FK, BAY_NUMBER, SCENE_FK]]
        if relevant_scif.empty:
            result = pd.np.nan
            denominator_id = 0
            numerator_id = 0

            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id, 'result': result}
            return result_dict

        bay_count_scif = relevant_scif.merge(product_in_scene, on=[PRODUCT_FK, SCENE_FK], how='right')
        bay_count_scif = bay_count_scif.dropna()
        bay_count_scif = bay_count_scif[bay_count_scif[MANUFACTURER_NAME].isin(manufacturer_name)]
        bay_count_scif = bay_count_scif[bay_count_scif[SUB_CATEGORY].isin(sub_category)]
        denominator_id = self.scif['template_fk'].mode()[0]
        numerator_id = self.get_sub_cat_fk_from_sub_cat(sub_category[0])
        bay_count_scif = bay_count_scif[bay_count_scif[TEMPLATE_NAME].isin(template_name)]
        if pd.notna(product_type):
            bay_count_scif = bay_count_scif[bay_count_scif[PRODUCT_TYPE].isin([product_type])]

        if pd.notna(tamano_del_producto):
            bay_count_scif = bay_count_scif[bay_count_scif[TAMANDO_DEL_PRODUCTO].str.contains(tamano_del_producto)]

        relevant_product_names = list(set(bay_count_scif['product_short_name']))

        if bay_count_scif.empty:
            result = pd.np.nan

        else:
            unique_bay_number = list(set(bay_count_scif[BAY_NUMBER]))

            if pd.isna(iterate_by):
                location_name = TEMPLATE_NAME
                location_id = template_name

                location = {location_name: location_id}

                for j in unique_bay_number:
                    if pd.notna(tamano_del_producto):
                        relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
                                            TAMANDO_DEL_PRODUCTO: [tamano_del_producto], BAY_NUMBER: [j],
                                            PRODUCT_NAME: relevant_product_names}
                    else:
                        relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category}

                    block = self.block.network_x_block_together(relevant_filters,
                                                                location=location,
                                                                additional={'minimum_block_ratio': 0.9,
                                                                            'calculate_all_scenes': True,
                                                                            'minimum_facing_for_block': 1,
                                                                            'use_masking_only': True,
                                                                            'include_stacking': True})
                    # if pd.notna(row['tagging']):
                    #     probes_match = [node[1]['probe_match_fk'] for i in range(len(block.cluster)) for node in
                    #                     block.cluster.reset_index().drop(columns=['index']).iloc[i, 0].node(data=True)]
                    #     match_product_in_probe_state_fk = self._get_probe_state_by_kpi_level_2_fk(kpi_fk)
                    #     lst_to_save = [x for sublist in probes_match for x in sublist]
                    #     df_for_common = pd.DataFrame({self.common.MATCH_PRODUCT_IN_PROBE_FK: lst_to_save,
                    #                                   self.common.MATCH_PRODUCT_IN_PROBE_STATE_REPORTING_FK: match_product_in_probe_state_fk})
                    #     self.common.match_product_in_probe_state_values = \
                    #         self.common.match_product_in_probe_state_values.append(df_for_common)

                    if False in block['is_block'].to_list():
                        result = 0
                        break
                    else:
                        result = 1

            else:
                location_name = TEMPLATE_NAME
                location_id = template_name
                location = {location_name: location_id}

                block_result_list = []
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
                                                                                'calculate_all_scenes': True,
                                                                                'minimum_facing_for_block': 1})
                        if False in block['is_block'].to_list():
                            result = 0
                            break
                        else:
                            result = 1

                        numerator_id = value

                        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                                       'denominator_id': denominator_id,
                                       'result': result}
                        block_result_list.append(result_dict)
                return block_result_list

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}
        return result_dict
        # kpi_name = row[KPI_NAME]
        # kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        # numerator_entity = row[NUMERATOR_ENTITY]
        # denominator_entity = row[DENOMINATOR_ENTITY]
        #
        # template_name = self.sanitize_values(row[TEMPLATE_NAME])
        # manufacturer_name = [row[MANUFACTURER_NAME]]
        # tamano_del_producto = row[TAMANDO_DEL_PRODUCTO]
        # sub_category = self.sanitize_values(row[SUB_CATEGORY])
        # iterate_by = row[ITERATE_BY]
        #
        # relevant_scif_columns = [PK, SESSION_ID, PRODUCT_FK, PRODUCT_NAME, TEMPLATE_GROUP, TEMPLATE_NAME,
        #                          MANUFACTURER_NAME,
        #                          TAMANDO_DEL_PRODUCTO, SUB_CATEGORY] + \
        #                         [denominator_entity, numerator_entity, denominator_entity, SCENE_FK]
        #
        # relevant_scif = self.scif[relevant_scif_columns]
        #
        # product_in_scene = self.match_product_in_scene[[PRODUCT_FK, BAY_NUMBER, SCENE_FK]]
        # if relevant_scif.empty:
        #     result = pd.np.nan
        #     denominator_id = 0
        #     numerator_id = 0
        #
        #     result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
        #                    'denominator_id': denominator_id, 'result': result}
        #     return result_dict
        #
        # bay_count_scif = relevant_scif.merge(product_in_scene, on=[PRODUCT_FK, SCENE_FK], how='right')
        # bay_count_scif = bay_count_scif.dropna()
        #
        # bay_count_scif = bay_count_scif[bay_count_scif[MANUFACTURER_NAME].isin(manufacturer_name)]
        #
        # bay_count_scif = bay_count_scif[bay_count_scif[SUB_CATEGORY].isin(sub_category)]
        #
        # denominator_id = self.scif['template_fk'].mode()[0]
        #
        # numerator_id = self.get_sub_cat_fk_from_sub_cat(sub_category[0])
        #
        # bay_count_scif = bay_count_scif[bay_count_scif[TEMPLATE_NAME].isin(template_name)]
        #
        # if pd.notna(tamano_del_producto):
        #     bay_count_scif = bay_count_scif[bay_count_scif[TAMANDO_DEL_PRODUCTO].str.contains(tamano_del_producto)]
        #
        # relevant_product_names = list(set(bay_count_scif['product_short_name']))
        #
        # if bay_count_scif.empty:
        #     result = pd.np.nan
        #
        # else:
        #     unique_bay_number = list(set(bay_count_scif[BAY_NUMBER]))
        #
        #     if pd.isna(iterate_by):
        #         location_name = TEMPLATE_NAME
        #         location_id = template_name
        #
        #         location = {location_name: location_id}
        #
        #         for j in unique_bay_number:
        #             if pd.notna(tamano_del_producto):
        #                 relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
        #                                     TAMANDO_DEL_PRODUCTO: [tamano_del_producto], BAY_NUMBER: [j],
        #                                     PRODUCT_NAME: relevant_product_names}
        #             else:
        #                 relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
        #                                     BAY_NUMBER: [j]}
        #
        #             block = self.block.network_x_block_together(relevant_filters,
        #                                                         location=location,
        #                                                         additional={'minimum_block_ratio': 0.9,
        #                                                                     'calculate_all_scenes': True,
        #                                                                     'minimum_facing_for_block': 1})
        #             if False in block['is_block'].to_list():
        #                 result = 0
        #                 break
        #             else:
        #                 result = 1
        #
        #     else:
        #         location_name = TEMPLATE_NAME
        #         location_id = template_name
        #         location = {location_name: location_id}
        #
        #         block_result_list = []
        #         for j in unique_bay_number:
        #             relevant_filters = {MANUFACTURER_NAME: manufacturer_name, SUB_CATEGORY: sub_category,
        #                                 BAY_NUMBER: [j]}
        #
        #             sub_category_fk_in_session = list(set(bay_count_scif[iterate_by]))
        #             sub_category_fk_in_session = filter(None, sub_category_fk_in_session)
        #
        #             for value in sub_category_fk_in_session:
        #                 population = {iterate_by: value}
        #                 final_relevant_filters = self.merge_two_dictionaries(relevant_filters, population)
        #                 block = self.block.network_x_block_together(relevant_filters,
        #                                                             location=location,
        #                                                             additional={'minimum_block_ratio': 0.9,
        #                                                                         'calculate_all_scenes': True,
        #                                                                         'minimum_facing_for_block': 1})
        #                 if False in block['is_block'].to_list():
        #                     result = 0
        #                 else:
        #                     result = 1
        #
        #                 numerator_id = value
        #
        #                 result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
        #                                'denominator_id': denominator_id,
        #                                'result': result}
        #                 block_result_list.append(result_dict)
        #         return block_result_list
        #
        # result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
        #                'denominator_id': denominator_id,
        #                'result': result}
        #
        # return result_dict

    def calculate_share_of_empty(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        target = row['target']
        numerator_value1 = row[NUMERATOR_VALUE_1]
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        numerator_param1 = row[NUMERATOR_PARAM_1]
        denominator_param1 = row[DENOMINATOR_PARAM_1]

        column_filter_for_scif = [PK, SESSION_ID, TEMPLATE_GROUP, FACINGS] + \
                                 [numerator_entity, denominator_entity] + \
                                 self.delete_filter_nan([numerator_param1, denominator_param1])

        filtered_scif = self.scif[column_filter_for_scif]
        filtered_scif = filtered_scif.rename(columns={FACINGS: FINAL_FACINGS})
        relevant_scif = filtered_scif[filtered_scif[TEMPLATE_GROUP].isin(template_group)]

        # For the KPI 31: Empty Denominator Param
        denominator_scif = relevant_scif
        if denominator_scif.empty:
            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}
            return result_dict
        denominator_result = relevant_scif[FINAL_FACINGS].sum()
        denominator_id = denominator_scif[denominator_entity].mode()[0]
        numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]
        numerator_result = numerator_scif[FINAL_FACINGS].sum()

        numerator_id = self.own_manuf_fk
        result = (numerator_result / denominator_result)
        actual_result = self.calculate_score_for_sos(target, result)

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'numerator_result': numerator_result,
                       'denominator_id': denominator_id, 'denominator_result': denominator_result,
                       'result': actual_result}

        return result_dict

    def calculate_bay_count(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        template_group = self.sanitize_values(row[TEMPLATE_NAME])
        numerator_entity = row[NUMERATOR_ENTITY]
        denominator_entity = row[DENOMINATOR_ENTITY]

        numerator_param_1 = row[NUMERATOR_PARAM_1]
        numerator_value_1 = row[NUMERATOR_VALUE_1]
        product_type = self.sanitize_values(row[PRODUCT_TYPE])

        store_type = self.store_info[ADDITIONAL_ATTRIBUTE_2].iloc[0]
        bay_count_target = self.calculate_targets_for_bay_count_kpi(store_type)

        relevant_scif_columns = [TEMPLATE_NAME, PRODUCT_NAME, PRODUCT_TYPE, FACINGS_IGN_STACK, PRODUCT_FK] + [
            numerator_param_1,
            numerator_entity,
            denominator_entity, SCENE_FK]

        relevant_scif = self.scif[relevant_scif_columns]

        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_NAME].isin(template_group)]

        relevant_scif = relevant_scif[relevant_scif[PRODUCT_TYPE].isin(product_type)]

        relevant_scif = relevant_scif[relevant_scif.product_short_name != 'Soda Other']

        if relevant_scif.empty:
            result = pd.np.nan
            denominator_id = 0
            numerator_id = 0

            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id, 'result': result}
            return result_dict

        product_in_scene = self.match_product_in_scene[['product_fk', 'bay_number', 'scene_fk']]

        bay_count_scif = relevant_scif.merge(product_in_scene, on=['product_fk', 'scene_fk'], how='right')
        bay_count_scif = bay_count_scif.dropna()

        if bay_count_scif.empty:
            result = pd.np.nan
            denominator_id = self.scif[denominator_entity].mode()[0]
            numerator_id = self.scif[numerator_entity].mode()[0]

            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id, 'result': result}
            return result_dict

        group_by_bay_number_scif = bay_count_scif.groupby(['scene_fk', 'bay_number']).nunique()[MANUFACTURER_NAME]

        bay_count = 0
        for scene_fk, bay in group_by_bay_number_scif.index:
            if group_by_bay_number_scif[scene_fk, bay] == 1:
                if 'TCCC' in set(
                        bay_count_scif[(bay_count_scif[BAY_NUMBER] == bay) & (bay_count_scif[SCENE_FK] == scene_fk)][
                            MANUFACTURER_NAME]):
                    bay_count = bay_count + 1

        relevant_scif = relevant_scif[relevant_scif[numerator_param_1].isin([numerator_value_1])]

        result = 1 if bay_count >= bay_count_target else 0

        try:
            numerator_id = relevant_scif[MANUFACTURER_FK].mode()[0]
            denominator_id = relevant_scif[TEMPLATE_FK].mode()[0]
        except:
            numerator_id = self.scif[MANUFACTURER_FK].mode()[0]
            denominator_id = self.scif[TEMPLATE_FK].mode()[0]

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'numerator_result': bay_count, 'denominator_id': denominator_id,
                       'denominator_result': bay_count_target,
                       'result': result}

        return result_dict

    def calculate_per_bay_sos(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        template_group = self.sanitize_values(row[TASK_TEMPLATE_GROUP])
        numerator_entity = 'manufacturer_fk'
        denominator_entity = 'store_id'
        template_group = row[TASK_TEMPLATE_GROUP]
        template_name = row[TEMPLATE_NAME]
        product_type = self.sanitize_values(row[PRODUCT_TYPE])
        numerator_param_1 = row[NUMERATOR_PARAM_1]
        numerator_value_1 = row[NUMERATOR_VALUE_1]

        relevant_scif_columns = [TEMPLATE_GROUP, TEMPLATE_NAME, PRODUCT_FK, PRODUCT_NAME, PRODUCT_TYPE,
                                 MANUFACTURER_NAME, SCENE_FK, FACINGS_IGN_STACK,
                                 numerator_entity, denominator_entity, 'category']

        relevant_scif = self.scif[relevant_scif_columns]

        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_GROUP].isin([template_group])]
        relevant_scif = relevant_scif[relevant_scif[TEMPLATE_NAME].isin([template_name])]

        relevant_scif = relevant_scif[relevant_scif[PRODUCT_TYPE].isin(product_type)]

        relevant_scif = relevant_scif[relevant_scif.product_short_name != 'Soda Other']
        if relevant_scif.empty:
            result = pd.np.nan
            denominator_id = 0
            numerator_id = 0

            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id, 'result': result}
            return result_dict

        product_in_scene = self.match_product_in_scene[['product_fk', 'bay_number', 'scene_fk']]
        bay_count_scif = relevant_scif.merge(product_in_scene, on=['product_fk', 'scene_fk'], how='right')
        bay_count_scif = bay_count_scif.dropna()

        if bay_count_scif.empty:
            result = pd.np.nan
            denominator_id = self.scif[denominator_entity].mode()[0]
            numerator_id = self.scif[numerator_entity].mode()[0]

            result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                           'denominator_id': denominator_id, 'result': result}
            return result_dict

        group_by_bay_number_scif = bay_count_scif.groupby('bay_number').nunique()[MANUFACTURER_NAME]

        bay_count = 0
        for bay in list(group_by_bay_number_scif.index):
            if group_by_bay_number_scif[bay] == 1:
                if 'TCCC' in set(bay_count_scif[bay_count_scif[BAY_NUMBER].isin([bay])][MANUFACTURER_NAME]):
                    bay_count = bay_count + 1
                    break

        if bay_count > 0:
            relevant_scif = relevant_scif[relevant_scif[numerator_param_1].isin([numerator_value_1])]
            facings_count = relevant_scif[FACINGS_IGN_STACK].sum()
            if facings_count >= 25:
                result = 1
        else:
            result = 0

        denominator_id = relevant_scif[denominator_entity].mode()[0]
        numerator_id = relevant_scif[numerator_entity].mode()[0]

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}

        return result_dict

    def calculate_survey(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_question_fk = self.sanitize_values(row[RELEVANT_QUESTION_FK])
        survey_result = self.calculate_relevant_survey_result(relevant_question_fk)

        denominator_id = self.store_id
        numerator_id = self.own_manuf_fk

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': survey_result}

        return result_dict

    def calculate_availability(self, row):
        relevant_scif = self._filter_scif(row, self.scif)
        result = 0 if relevant_scif.empty else 1
        if pd.notna(row[RELEVANT_QUESTION_FK]):
            relevant_question_fk = self.sanitize_values(row[RELEVANT_QUESTION_FK])
            result = self.calculate_relevant_availability_survey_result(relevant_question_fk) + result
            result = float(result) / 3

        return_holder = self._get_kpi_name_and_fk(row, generic_num_dem_id=True)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id': return_holder[2],
                       'denominator_id': return_holder[3],
                       'result': result}
        return result_dict

    def _filter_scif(self, row, df):
        columns_in_scif = row.index[np.in1d(row.index, df.columns)]
        for column_name in columns_in_scif:
            if pd.notna(row[column_name]) and not df.empty:
                df = df[df[column_name].isin(self.sanitize_values(row[column_name]))]
        return df

    def _get_kpi_name_and_fk(self, row, generic_num_dem_id=False):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        output = [kpi_name, kpi_fk]
        if generic_num_dem_id:
            numerator_id = self.scif[row[NUMERATOR_ENTITY]].mode().iloc[0]
            denominator_id = self.scif[row[DENOMINATOR_ENTITY]].mode().iloc[0]
            output.append(numerator_id)
            output.append(denominator_id)
        return output

    @staticmethod
    def sanitize_values(item):
        if pd.isna(item):
            return item
        else:
            if type(item) == int:
                return str(item)
            else:
                items = [x.strip() for x in item.split(',')]
                return items

    @staticmethod
    def delete_filter_nan(filters):
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
            bay_count_target = 1
        elif store_size == 'Mediano':
            bay_count_target = 2
        elif store_size == 'Grande':
            bay_count_target = 3

        return bay_count_target

    def calculate_relevant_survey_result(self, relevant_question_fk):
        result = pd.np.nan
        survey_response_df = self.get_scene_survey_response()
        if survey_response_df.empty:
            return 0
        for question_fk in relevant_question_fk:
            if result == 0:
                break

            relevant_survey_response = survey_response_df[survey_response_df['question_fk'].isin([question_fk])]

            if question_fk in ['5', '6', '7']:
                if relevant_survey_response.iloc[0, 2] == "Si":
                    result = 0
                else:
                    result = 1

            else:
                if relevant_survey_response.iloc[0, 2] == "Si":
                    result = 1
                else:
                    result = 0

        return result

    def calculate_relevant_availability_survey_result(self, relevant_question_fk):
        result = 0
        survey_response_df = self.get_scene_survey_response()
        if survey_response_df.empty:
            return 0
        accepted_results = ['Si', 1, 2, u'1', u'2']

        for question_fk in relevant_question_fk:
            relevant_survey_response = survey_response_df[survey_response_df['question_fk'].isin([question_fk])]
            if not relevant_survey_response.empty:
                if relevant_survey_response.iloc[0, 2] in accepted_results:
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

    def get_scene_survey_response(self):
        query = """SELECT session_uid,question_fk,selected_option_text, scene_fk
                        FROM probedata.scene_survey_response res
                        LEFT JOIN probedata.scene sce ON res.scene_fk =  sce.pk
                        WHERE session_uid = '{}';""".format(self.session_uid)

        scene_survey_response = pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)
        return scene_survey_response

    @staticmethod
    def calculate_score_for_sos(target, result):
        if pd.notna(target):
            target = str(target)
            if len(target) > 3:
                min_target, max_target = target.split('-')
                if result * 100 >= int(min_target) and result * 100 <= int(max_target):
                    score = 1
                else:
                    score = 0
            else:
                if result * 100 >= int(target):
                    score = 1
                else:
                    score = 0
        else:
            score = 0
        return score

    @staticmethod
    def filter_df(df, filters, exclude=0):
        cols = set(df.columns)
        for key, val in filters.items():
            if key not in cols:
                return pd.DataFrame()
            if not isinstance(val, list):
                val = [val]
            if exclude:
                df = df[~df[key].isin(val)]
            else:
                df = df[df[key].isin(val)]
        return df

    def calculate_prereq_platformas_scoring(self):
        platformas_data = self.platformas_data
        platformas_data['template_name'] = platformas_data.scene_id.apply(
            lambda x: self.scif[self.scif.scene_id == x].template_name.iloc[0])

        pre_req_platformas = self.templates[PLATFORMAS_SCORING_PREREQ]
        pre_req_platformas = pre_req_platformas[pre_req_platformas.store_additional_attribute_2.str.contains(self.att2)]

        final_df = pd.DataFrame()
        for index, row in platformas_data.iterrows():
            relevant_template_name = row.template_name
            relevant_preplat = pre_req_platformas[pre_req_platformas['Platform'].str.contains(row['Platform Name'])]
            relevant_preplat = relevant_preplat[relevant_preplat.template_name.str.contains(relevant_template_name)]

            clump_plat = (row['Minimum facings met'] * .5 / 3 + row['Mandatory SKUs found'] * .5 / 3 + row[
                'Survey Question'] * .5 / 3) if not 0 in [row['Minimum facings met'], row['Mandatory SKUs found'], row[
                 'Survey Question']] else 0

            result = (row['POS option present'] * .25) + clump_plat + (row['Coke purity'] * .25)

            relevant_preplat['actual_score'] = relevant_preplat['Score'] * result
            relevant_preplat['result'] = result
            relevant_preplat['scene_id'] = row.scene_id
            relevant_preplat['Platform'] = row['Platform Name']
            final_df = final_df.append(relevant_preplat)
        return final_df
