# -*- coding: utf-8 -*-
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
from datetime import datetime

# Sheet names
KPIS = 'KPIs'
AVALIABILITY = 'POS Availability'
SOS = 'SOS'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
DISTRIBUTION_SCORING = 'Distribution Scoring'
ASSORTMENT = 'Assortment'
SCORING = 'Scoring'
TARGET = 'target'
PRODUCT_FK = 'product_fk'

# Excel Column Names
RELEVANT_ASSORTMNENT = 'Relevent Assortment'
ASSORTMENT1 = 'Assortment1'
ASSORTMENT2 = 'Assortment2'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'

# Dataframe Column Names
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
TASK_TEMPLATE_GROUP = 'Task/ Template Group'
TEMPLATE_NAME = 'template_name'
MANUFACTURER_NAME = 'manufacturer_name'
RELEVANT_QUESTION_FK = 'question_fk'
PRODUCT_SHORT_NAME = 'product_short_name'
PRODUCT_TYPE = 'product_type'
FACINGS_IGN_STACK = 'facings_ign_stack'

RESULT = 'result'

# Excel Sheets
SHEETS = [KPIS, AVALIABILITY, SOS, SHARE_OF_EMPTY, SURVEY, DISTRIBUTION, DISTRIBUTION_SCORING, ASSORTMENT, SCORING]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'Nayar Comidas Fondas Template_v3.xlsx')


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


class FONDASToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.common_v2 = common
        self.ps_data_provider = PsDataProvider(data_provider)
        self.store_type = self.store_info['store_type'].iloc[0]
        self.templates = {}
        self.parse_template()
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.template_fk = self.scif.template_fk.iloc[0]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.assortment_template = self.templates[ASSORTMENT]
        self.match_product_in_scene = self.data_provider['matches']
        self.important_survey = self.survey_response[self.survey_response.question_fk.isin([22])]
        # self.survey = Survey(self.data_provider, output=output, ps_data_provider=self.ps_data_provider,
        #                      common=self.common_v2)
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        # The kpis should only run if store type is Fondas_Rsr and /
        # store_additional_attribute is one of the following /
        # 'FONDA / LONCHERÍA / MERENDERO,RSR COMIDA MEXICANA / TACOS,RSR ASIAN,RSR SEAFOOD,RSR LOCAL FOOD,
        # RSR PIZZAS,RSR SANDWICHES / TORTERIA,RSR POLLO,RSR HAMBURGUESAS,RSR OTROS ALIMENTOS'
        store_type = 'Fondas-Rsr'
        store_additional_attribute = self.sanitize_values(
            'FONDA / LONCHERÍA / MERENDERO,RSR COMIDA MEXICANA / TACOS,RSR ASIAN,RSR SEAFOOD,RSR LOCAL FOOD,RSR PIZZAS,RSR SANDWICHES / TORTERIA,RSR POLLO,RSR HAMBURGUESAS,RSR OTROS ALIMENTOS')
        store_additional_attribute = [unicode(value, 'utf-8') for value in store_additional_attribute]

        if not(self.important_survey.empty) and self.store_info.additional_attribute_5.isin(store_additional_attribute)[0] and \
                self.store_info.store_type.isin([store_type])[0]:
            relevant_kpi_template = self.templates[KPIS]
            foundation_kpi_types = [SOS, SHARE_OF_EMPTY, DISTRIBUTION, SURVEY, AVALIABILITY]
            foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(foundation_kpi_types)]
            distribution_scoring_kpi_template = relevant_kpi_template[
                relevant_kpi_template[KPI_TYPE] == DISTRIBUTION_SCORING]
            scoring_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE] == SCORING]

            self._calculate_kpis_from_template(foundation_kpi_template)
            self._calculate_kpis_from_template(distribution_scoring_kpi_template)
            self._calculate_kpis_from_template(scoring_kpi_template)
            self.save_results_to_db()

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df.loc[~self.results_df['identifier_parent'].isnull(), 'should_enter'] = True
        # set result to NaN for records that do not have a parent
        # identifier_results = self.results_df[self.results_df['result'].notna()]['identifier_result'].unique().tolist()
        # self.results_df['result'] = self.results_df.apply(
        #     lambda row: pd.np.nan if pd.notna(row['identifier_parent']) and row[
        #         'identifier_parent'] not in identifier_results else row['result'], axis=1)
        # get rid of 'not applicable' results
        self.results_df.dropna(subset=['result'], inplace=True)
        self.results_df.fillna(0)
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
                        if (row[KPI_TYPE] == SCORING and 'score' not in result_data.keys()) or row[
                            KPI_TYPE] == DISTRIBUTION_SCORING:
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
                        if 'identifier_parent' not in result.keys():
                            parent_kpi_name = self._get_parent_name_from_kpi_name(result['kpi_name'])
                            result['identifier_parent'] = parent_kpi_name
                        if 'identifier_result' not in result.keys():
                            result['identifier_result'] = result['kpi_name']
                        if result['result'] <= 1:
                            result['result'] = result['result'] * 100
                        self.results_df.loc[len(self.results_df), result.keys()] = result

    def _get_calculation_function_by_kpi_type(self, kpi_type):
        if kpi_type == SOS:
            return self.calculate_sos
        elif kpi_type == AVALIABILITY:
            return self.calculate_availability
        elif kpi_type == SURVEY:
            return self.calculate_survey
        elif kpi_type == DISTRIBUTION:
            return self.calculate_assortment
        elif kpi_type == SHARE_OF_EMPTY:
            return self.calculate_share_of_empty
        elif kpi_type == SCORING:
            return self.calculate_scoring
        elif kpi_type == DISTRIBUTION_SCORING:
            return self.calculate_assortment_scoring

    def calculate_scoring(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        numerator_id = self.own_manuf_fk
        denominator_id = self.store_id

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id}

        component_kpis = self.sanitize_values(row['Component KPIs'])
        relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
        passing_results = relevant_results[(relevant_results['result'] != 0) &
                                           (relevant_results['result'].notna()) &
                                           (relevant_results['score'] != 0)]
        nan_results = relevant_results[relevant_results['result'].isna()]

        if len(relevant_results) > 0 and len(relevant_results) == len(nan_results):
            result_dict['result'] = pd.np.nan
        elif row['Component Aggregation'] == 'one-passed':
            if len(relevant_results) > 0 and len(passing_results) > 0:
                result_dict['result'] = 1
            else:
                result_dict['result'] = 0
        elif row['Component Aggregation'] == 'sum':
            if len(relevant_results) > 0:
                result_dict['score'] = relevant_results['score'].sum()
            else:
                result_dict['score'] = 0

            if row['Additional Component Aggregation'] == 'fraction':
                result_dict['result'] = float(sum(relevant_results.result.to_numpy() > 0)) / len(relevant_results)
            elif row['Additional Component Aggregation'] == 'match':
                result_dict['result'] = result_dict['score']

        if 'score' in result_dict.keys():
            if result_dict['result'] == result_dict['score'] and (
                    (result_dict['result'] > 0) and (result_dict['result'] < 1)):
                result_dict['score'] = result_dict['score'] * 100

        return result_dict

    def calculate_assortment_scoring(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        numerator_id = self.own_manuf_fk
        denominator_id = self.store_id

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id}
        component_kpis = self.sanitize_values(row['Component KPIs'])
        dependency_kpis = self.sanitize_values(row['Dependency'])
        relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
        count_of_component_kpis = len(component_kpis)
        potential_results = [1, .5, .5, .25] if count_of_component_kpis == 4 else [1, .5]
        holder_for_kpi_results = {}
        for potential_result, result_of_child_kpi in zip(potential_results, relevant_results.result):
            if result_of_child_kpi != 0:
                holder_for_kpi_results[potential_result] = result_of_child_kpi

        kpi_result = sorted(holder_for_kpi_results.keys(), reverse=True)[0] if holder_for_kpi_results.keys() else 0

        if dependency_kpis and dependency_kpis is not pd.np.nan:
            dependency_results = self.results_df[self.results_df['kpi_name'].isin(dependency_kpis)]
            passing_dependency_results = dependency_results[dependency_results['result'] != 0]
            if len(dependency_results) > 0 and len(dependency_results) == len(passing_dependency_results):
                kpi_result = 1
            else:
                kpi_result = 0
        result_dict['result'] = kpi_result
        return result_dict

    def calculate_share_of_empty(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        target = str(row[TARGET])
        template_group = row[TASK_TEMPLATE_GROUP]

        relevant_denominator_scif = self.filter_df(self.scif, {'template_group': template_group})
        if not relevant_denominator_scif.empty:
            denominator_result = relevant_denominator_scif[FACINGS_IGN_STACK].sum()
            relevant_numerator_scif = self.filter_df(relevant_denominator_scif,
                                                     {row[NUMERATOR_PARAM_1]: row[NUMERATOR_VALUE_1]})
            if not relevant_numerator_scif.empty:
                numerator_result = relevant_numerator_scif[FACINGS_IGN_STACK].sum()
                result = float(numerator_result) / float(denominator_result)
            else:
                numerator_result = 0
                result = 0
        else:
            denominator_result = 0
            numerator_result = 0
            result = 0

        score = self.calculate_score_for_sos(target, result)
        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'numerator_result': numerator_result,
                       'denominator_id': self.template_fk, 'denominator_result': denominator_result,
                       'result': result, 'score': score}

        return result_dict

    def calculate_sos(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        target = str(row[TARGET])
        template_group = row[TASK_TEMPLATE_GROUP]
        product_type = self.sanitize_values(row[PRODUCT_TYPE])

        relevant_denominator_scif = self.filter_df(self.scif,
                                                   {'template_group': template_group, PRODUCT_TYPE: product_type})
        if not relevant_denominator_scif.empty:
            denominator_result = relevant_denominator_scif[FACINGS_IGN_STACK].sum()
            relevant_numerator_scif = self.filter_df(relevant_denominator_scif,
                                                     {row[NUMERATOR_PARAM_1]: row[NUMERATOR_VALUE_1]})
            if not relevant_numerator_scif.empty:
                numerator_result = relevant_numerator_scif[FACINGS_IGN_STACK].sum()
                result = float(numerator_result) / float(denominator_result)
            else:
                numerator_result = 0
                result = 0
        else:
            denominator_result = 0
            numerator_result = 0
            result = 0

        score = self.calculate_score_for_sos(target, result)
        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'numerator_result': numerator_result,
                       'denominator_id': self.template_fk, 'denominator_result': denominator_result,
                       'result': result, 'score': score}

        return result_dict

    def calculate_availability(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)

        product_type = row[PRODUCT_TYPE]
        template_name = row[TEMPLATE_NAME]
        product_fk = self.sanitize_values(row[PRODUCT_FK])

        # Have to Join mpis with scif as there in issue in recognition of product_fk = '2805' in scif
        mpis_merge_with_scif = \
            self.match_product_in_scene[['scene_fk', PRODUCT_FK]].merge(
                self.scif[['scene_fk', TEMPLATE_NAME, PRODUCT_TYPE]], how='left', on='scene_fk')

        relevant_scif = self.filter_df(mpis_merge_with_scif, {TEMPLATE_NAME: template_name, PRODUCT_TYPE: product_type,
                                                              PRODUCT_FK: product_fk})
        result = 1 if not relevant_scif.empty else 0
        numerator_id = self.own_manuf_fk
        denominator_id = self.template_fk

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}
        return result_dict

    def calculate_survey(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        relevant_question_fk = row[RELEVANT_QUESTION_FK]

        relevant_survey_row = self.survey_response[self.survey_response.question_fk.isin([relevant_question_fk])]
        survey_answer = relevant_survey_row.selected_option_text.iat[0]

        numerator_id = self.own_manuf_fk
        denominator_id = self.store_id
        result = 1 if survey_answer == 'Si' else 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}
        return result_dict

    def calculate_assortment(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        parent_kpi_name = row[PARENT_KPI]
        result_dict_list = []
        relevant_assortment = self.sanitize_values(self.assortment_template.loc[
                                                       self.assortment_template[KPI_NAME] == parent_kpi_name, row[
                                                           RELEVANT_ASSORTMNENT]].iat[0])
        relevant_scif = self.filter_df(self.scif, {'template_group': row['template_group']})
        lst_result_of_assortment_exists = np.in1d(relevant_assortment, relevant_scif)
        final_result_of_current_assortment = any(lst_result_of_assortment_exists)
        relevant_survey_result = self.results_df.loc[self.results_df['kpi_name'] == 'Visible-Fondas-Rsr', RESULT][0]

        if row['Relevant_SKU'] == 'Y':
            kpi_sku_name = kpi_name + " - SKU"
            kpi_id = self.common.get_kpi_fk_by_kpi_type(kpi_sku_name)
            # if final_result_of_current_assortment:
            existing_prod_in_required_assortment = \
                np.take(relevant_assortment, np.where(lst_result_of_assortment_exists))[0]
            for assortment in relevant_assortment:
                result = 1 if assortment in existing_prod_in_required_assortment else 0
                product_fk = self.all_products.loc[self.all_products.product_short_name == assortment, 'product_fk'].iat[
                    0]
                sub_category_fk = \
                    self.all_products.loc[self.all_products.product_short_name == assortment, 'sub_category_fk'].iat[0]
                result_dict = {'kpi_name': kpi_sku_name, 'kpi_fk': kpi_id, 'numerator_id': product_fk,
                               'denominator_id': sub_category_fk,
                               'result': result, 'identifier_parent': kpi_name}
                result_dict_list.append(result_dict)

        numerator_id = self.scif.product_fk.iat[0] if not self.scif.product_fk.empty else 0
        denominator_id = self.scif.sub_category_fk.iat[0] if not self.scif.sub_category_fk.empty else 0

        '''The Asssortment Right column and Survey Right column was created to help encompass all the different types 
           of assortment kpis into one method. Some Assortment kpis require the assortment to pass and the survey to pass.
           Some assortment kpis require they survey to fail and assortments to pass. So if the assortment and the survey 
            are supposed to pass, the row['Assortment_Right'] and row['Survey_Right'] will be Y (indicating yes).
            If the assortment is supposed to pass and the survey is supposed to fail, the row['Assortment_Right'] will be 
            Y (indicating yes) and row['Survey_Right'] will be N(indicating no).
        '''
        if row['Assortment_Right'] == 'Y' and row['Survey_Right'] == 'Y':
            result = 1 if relevant_survey_result == 100 and final_result_of_current_assortment else 0
        elif row['Assortment_Right'] == 'Y' and row['Survey_Right'] == 'N':
            result = 1 if relevant_survey_result == 0 and final_result_of_current_assortment else 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result, 'identifier_result': kpi_name + " - SKU"}
        result_dict_list.append(result_dict)
        return result_dict_list

    def _get_parent_name_from_kpi_name(self, kpi_name):
        template = self.templates[KPIS]
        parent_kpi_name = \
            template[template[KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')][PARENT_KPI].iloc[0]
        if parent_kpi_name and pd.notna(parent_kpi_name):
            return parent_kpi_name
        else:
            return None

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
    def _get_groups(series, root_string):
        groups = []
        for column in [col for col in series.index.tolist() if root_string in col]:
            if series[column] not in ['', np.nan]:
                groups.append([x.strip() for x in series[column].split(',')])
        return groups

    @staticmethod
    def calculate_score_for_sos(target, result):
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
        return score
