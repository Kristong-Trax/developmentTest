from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.SurveyCalculations import Survey
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.DB.CommonV2 import Common as CommonV2

import pandas as pd
import numpy as np
import os
from datetime import datetime

__author__ = 'krishnat'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'CCNayarTemplate2020-Fondas Rsr.xlsx')
# Sheet names
KPIS = 'KPIs'
SOS = 'SOS'
POSM_AVAILABILITY = 'POSM Availability'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
COMBO = 'Combo'
SCORING = 'Scoring'

# Column Name
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
NUMERATOR_ENTITY = 'Numerator Entity'
DENOMINATOR_ENTITY = 'Denominator Entity'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'

SHEETS = [KPIS, SOS, SHARE_OF_EMPTY, SURVEY, POSM_AVAILABILITY, DISTRIBUTION, COMBO, SCORING]

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

class ComidasToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.ps_data_provider = PsDataProvider(data_provider)
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.templates = {}
        self.parse_template()
        self.store_type = self.store_info['store_type'].iloc[0]
        self.survey = Survey(self.data_provider, output, ps_data_provider=self.ps_data_provider, common=self.common)
        self.att2 = self.store_info['additional_attribute_2'].iloc[0]
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        if self.store_type == 'Fondas-Rsr':
            relevant_kpi_template = self.templates[KPIS]
            foundation_kpi_types = [SOS, SHARE_OF_EMPTY]
            foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(foundation_kpi_types)]
            combo_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE] == COMBO]
            scoring_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE] == SCORING]

            self._calculate_kpis_from_template(foundation_kpi_template)
            self._calculate_kpis_from_template(scoring_kpi_template)
            self._calculate_kpis_from_template(combo_kpi_template)
            self.save_results_to_db()

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
                    self.templates[row[KPI_TYPE]][KPI_NAME].str.encode('utf-8') == row[KPI_NAME].encode(
                        'utf-8')].iloc[
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
        elif kpi_type == SHARE_OF_EMPTY:
            return self.calculate_share_of_empty

    def calculate_distribution(self, row):
        return_holder = self._get_kpi_name_and_fk(row)
        a = 1

    def calculate_share_of_empty(self, row):
        target = row['target']
        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = row[NUMERATOR_VALUE_1]

        return_holder = self._get_kpi_name_and_fk(row)
        denominator_scif = self._filter_df_based_on_row(row, self.scif)
        numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]

        result_dict = {'kpi_name':return_holder[0], 'kpi_fk':return_holder[1], 'result':0}
        if not numerator_scif.empty:
            denominator_result = denominator_scif.facings_ign_stack.sum()
            numerator_result = numerator_scif.facings_ign_stack.sum()
            result = (numerator_result / denominator_result)
            result_dict['result'] = self.calculate_score_for_sos(target, result)

        return result_dict

    def calculate_sos(self, row):
        target = row['target']
        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = row[NUMERATOR_VALUE_1]

        return_holder = self._get_kpi_name_and_fk(row)
        denominator_scif = self._filter_df_based_on_row(row, self.scif)
        numerator_scif = denominator_scif[denominator_scif[numerator_param1].isin([numerator_value1])]

        result_dict = {'kpi_name':return_holder[0], 'kpi_fk':return_holder[1], 'result':0}
        if not numerator_scif.empty:
            denominator_result = denominator_scif.facings.sum()
            numerator_result = numerator_scif.facings.sum()
            result = (numerator_result / denominator_result)
            score = self.calculate_score_for_sos(target, result)
            result_dict['result'] = result
            result_dict['score'] = score

        return result_dict

    # def calculate_scoring(self, row):
    #     kpi_name = row[KPI_NAME]
    #     kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
    #     numerator_id = self.own_manuf_fk
    #     denominator_id = self.store_id
    #
    #     result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
    #                    'denominator_id': denominator_id}
    #
    #     component_kpis = self.sanitize_values(row['Component KPIs'])
    #     dependency_kpis = self.sanitize_values(row['Dependency'])
    #     relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
    #     passing_results = relevant_results[(relevant_results['result'] != 0) &
    #                                        (relevant_results['result'].notna()) &
    #                                        (relevant_results['score'] != 0)]
    #     nan_results = relevant_results[relevant_results['result'].isna()]
    #     if len(relevant_results) > 0 and len(relevant_results) == len(nan_results):
    #         result_dict['result'] = pd.np.nan
    #     elif row['Component aggregation'] == 'one-passed':
    #         if len(relevant_results) > 0 and len(passing_results) > 0:
    #             result_dict['result'] = 1
    #         else:
    #             result_dict['result'] = 0
    #     elif row['Component aggregation'] == 'sum':
    #         if len(relevant_results) > 0:
    #             result_dict['score'] = relevant_results['score'].sum()
    #             if 'result' not in result_dict.keys():
    #                 if row['score_based_result'] == 'y':
    #                     result_dict['result'] = 0 if result_dict['score'] == 0 else result_dict['score'] / row['Score']
    #                 elif row['composition_based_result'] == 'y':
    #                     result_dict['result'] = 0 if passing_results.empty else float(len(passing_results)) / len(
    #                         relevant_results)
    #                 else:
    #                     result_dict['result'] = result_dict['score']
    #         else:
    #             result_dict['score'] = 0
    #             if 'result' not in result_dict.keys():
    #                 result_dict['result'] = result_dict['score']
    #     if dependency_kpis and dependency_kpis is not pd.np.nan:
    #         dependency_results = self.results_df[self.results_df['kpi_name'].isin(dependency_kpis)]
    #         passing_dependency_results = dependency_results[dependency_results['result'] != 0]
    #         if len(dependency_results) > 0 and len(dependency_results) == len(passing_dependency_results):
    #             result_dict['result'] = 1
    #         else:
    #             result_dict['result'] = 0
    #
    #     return result_dict



    def _filter_df_based_on_row(self, row, df):
        columns_in_scif = row.index[np.in1d(row.index, df.columns)]
        for column_name in columns_in_scif:
            if pd.notna(row[column_name]):
                df = df[df[column_name].isin(self.sanitize_values(row[column_name]))]
            if df.empty:
                break
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

    def _get_parent_name_from_kpi_name(self, kpi_name):
        template = self.templates[KPIS]
        parent_kpi_name = \
            template[template[KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')][PARENT_KPI].iloc[0]
        if parent_kpi_name and pd.notna(parent_kpi_name):
            return parent_kpi_name
        else:
            return None

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
