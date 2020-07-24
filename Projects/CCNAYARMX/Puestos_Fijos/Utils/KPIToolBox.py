from datetime import datetime
import pandas as pd
import numpy as np
import operator as op
import os

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log

from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

__author__ = 'krishnat'

# Sheet names
KPIS = 'KPIs'
SOS = 'SOS'
POSM_AVAILABILITY = 'POSM Availability'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
COMBO = 'Combo'
SCORING = 'Scoring'
DISTRIBUTION_PREREQ = 'Distribution Prereq'
FINAL_SCORING = 'Final Scoring'

# Column Name
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
NUMERATOR_ENTITY = 'Numerator Entity'
DENOMINATOR_ENTITY = 'Denominator Entity'
NUMERATOR_PARAM_1 = 'numerator param 1'
NUMERATOR_VALUE_1 = 'numerator value 1'
TEMPLATE_GROUP = 'template_group'


TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'NayarPuestosFijos2020v1.xlsx')

SHEETS = [KPIS, SOS, POSM_AVAILABILITY, DISTRIBUTION, COMBO, SCORING, DISTRIBUTION_PREREQ, FINAL_SCORING]
COLUMNS = ['scene_match_fk', 'scene_fk', TEMPLATE_GROUP, 'template_fk', 'brand_fk', 'brand_name', 'manufacturer_fk',
           'manufacturer_name', 'category_fk', 'category', 'product_type', 'product_fk', 'facings']

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


class PuestosFijosToolBox(GlobalSessionToolBox):
    def __init__(self, data_provider, output, common):
        GlobalSessionToolBox.__init__(self, data_provider, output, common)
        self.ps_data_provider = PsDataProvider(data_provider)
        self.own_manufacturer = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.templates = {}
        self.parse_template()
        self.store_type = self.store_info['store_type'].iloc[0]
        self.survey = Survey(self.data_provider, output, ps_data_provider=self.ps_data_provider, common=self.common)
        self.att2 = self.store_info['additional_attribute_2'].iloc[0]
        self.products = self.data_provider[Data.PRODUCTS]
        self.survey = self.data_provider.survey_responses
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])



    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        if not self.store_type == 'Puestos Fijos':
            return
        relevant_kpi_template = self.templates[KPIS]
        # SOS, DISTRIBUTION, POSM_AVAILABILITY
        foundation_kpi_types = [SOS, DISTRIBUTION, POSM_AVAILABILITY]
        foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(foundation_kpi_types)]
        scoring_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(['Scoring'])]
        combo_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(['Combo'])]
        final_scoring_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin([FINAL_SCORING])]


        self._calculate_kpis_from_template(foundation_kpi_template)
        self._calculate_kpis_from_template(scoring_kpi_template)
        self._calculate_kpis_from_template(combo_kpi_template)
        self._calculate_kpis_from_template(final_scoring_kpi_template)

        self.save_results_to_db()


    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.results_df.loc[~self.results_df['identifier_parent'].isnull(), 'should_enter'] = True
        self.results_df['result'] = self.results_df.apply(
            lambda row: row['result'] if (
                    pd.notna(row['identifier_parent']) or pd.notna(row['identifier_result'])) else np.nan, axis=1)
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
                        if row[KPI_TYPE] in [SCORING,FINAL_SCORING] and 'score' not in result_data.keys():
                            result_data['score'] = weight * result_data['result']
                        elif row[KPI_TYPE] not in [SCORING,FINAL_SCORING]:
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
                            if row[KPI_TYPE] in [SCORING,FINAL_SCORING] and 'score' not in result.keys():
                                result['score'] = weight * result['result']
                            elif row[KPI_TYPE] not in [SCORING,FINAL_SCORING]:
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
        elif kpi_type == DISTRIBUTION:
            return self.calculate_distribution
        elif kpi_type == POSM_AVAILABILITY:
            return self.calculate_posm_availability
        elif kpi_type == COMBO:
            return self.calculate_combo
        elif kpi_type == SCORING:
            return self.calculate_scoring
        elif kpi_type == FINAL_SCORING:
            return self.calculate_final_scoring

    def calculate_final_scoring(self,row):
        return_holder = self._get_kpi_name_and_fk(row)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id': self.own_manufacturer,
                       'denominator_id': self.store_id, 'result': 0}
        component_kpis = self.sanitize_values(row['Component KPIs'])
        relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
        passing_results = relevant_results[(relevant_results['result'] != 0) &
                                           (relevant_results['result'].notna()) &
                                           (relevant_results['score'] != 0)]
        if row['Component aggregation'] == 'sum':
            if len(relevant_results) > 0 and len(passing_results) > 0:
                # result_dict['result'] = float(passing_results.result.sum()) / len(relevant_results)
                # result_dict['score'] = row.Score * result_dict['result']
                result_dict['score'] = relevant_results.score.sum()
                result_dict['result'] = result_dict['score']

            else:
                result_dict['result'] = 0

        return result_dict


    def calculate_scoring(self, row):
        return_holder = self._get_kpi_name_and_fk(row)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id':self.own_manufacturer, 'denominator_id':self.store_id, 'result':0}

        component_kpis = self.sanitize_values(row['Component KPIs'])
        relevant_results = self.results_df[self.results_df['kpi_name'].isin(component_kpis)]
        passing_results = relevant_results[(relevant_results['result'] != 0) &
                                           (relevant_results['result'].notna()) &
                                           (relevant_results['score'] != 0)]

        if row['Component aggregation'] == 'one-passed':
            if len(relevant_results) > 0 and len(passing_results) > 0:
                result_dict['score'] = row.Score * float(passing_results.result.sum())/100
                result_dict['result'] = result_dict['score']
            else:
                result_dict['result'] = 0
        elif row['Component aggregation'] == 'max':
            if len(relevant_results) > 0 and len(passing_results) > 0:
                result_dict['score'] = passing_results.score.max()
                result = float(result_dict['score']) / row.Score
                result_dict['result'] = result
            else:
                result_dict['result'] = 0
        elif row['Component aggregation'] == 'sum':
            if len(relevant_results) > 0 and len(passing_results) > 0:
                score_percentage = float(passing_results.result.sum()) / len(relevant_results)
                result_dict['score'] = float(row.Score * score_percentage)/100 if score_percentage != 0 else 0
                result_dict['result'] = result_dict['score']
            else:
                result_dict['result'] = 0

        return result_dict

    def calculate_combo(self,row):
        return_holder = self._get_kpi_name_and_fk(row)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id':self.own_manufacturer, 'denominator_id':self.store_id, 'result':0}
        relevant_scif = self._filter_df(self.scif, {row.b_filter:row.b_value})
        if not relevant_scif.empty:
            relevant_kpis = self.sanitize_values(row.a_value) #Portafolio Frio-Puestos,Comunicacion-Puestos,Puntos de Conexion-Puestos
            relevant_result = self.results_df[self.results_df.kpi_name.isin(relevant_kpis)]
            result_dict['result'] = 1 if relevant_result.score.sum() >= row.a_threshold else 0
        return result_dict

    def calculate_posm_availability(self,row):
        return_holder = self._get_kpi_name_and_fk(row)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id':self.own_manufacturer, 'denominator_id':self.store_id, 'result':0}

        product_fk = self.sanitize_values(row.product_fk)
        relevant_scif = self._filter_df(self.scif, {'product_fk':product_fk})
        if not relevant_scif.empty:
            result_dict['numerator_id'] = relevant_scif.manufacturer_fk.iat[0]
            result_dict['denominator_id'] = relevant_scif.template_fk.iat[0]
            result_dict['result'] = 1
            result_dict['score'] = row['KPI Total Points']
        return result_dict

    def calculate_sos(self,row):
        return_holder = self._get_kpi_name_and_fk(row)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id':self.own_manufacturer, 'denominator_id':self.store_id, 'result':0}
        other_product_type_logic = self._logic_for_checking_if_product_other_exisits_for_sos(self.scif)
        if other_product_type_logic:
            denominator_relevant_scif = self._filter_scif(row, self.scif)
            if denominator_relevant_scif.empty:
                result_dict['result'] = 1
            else:
                numerator_relevant_scif = self._filter_df(denominator_relevant_scif, {'manufacturer_name':'TCCC'})
                if not numerator_relevant_scif.empty:
                    denominator_result = denominator_relevant_scif.facings_ign_stack.sum()
                    numerator_result = numerator_relevant_scif.facings_ign_stack.sum()
                    result = float(numerator_result)/denominator_result

                    result_dict['denominator_result'] = denominator_result
                    result_dict['numerator_result'] = numerator_result
                    result_dict['result'] = result
        return result_dict

    def calculate_distribution(self,row):
        return_holder = self._get_kpi_name_and_fk(row)
        result_dict = {'kpi_name': return_holder[0], 'kpi_fk': return_holder[1], 'numerator_id':self.own_manufacturer, 'denominator_id':self.store_id, 'result':0}

        distribution_prereq = self.templates[DISTRIBUTION_PREREQ]
        relevant_distribution_prereq = distribution_prereq[distribution_prereq.store_additional_attribute_2.str.contains(self.att2)]
        max_score_potential = relevant_distribution_prereq['Score'].max()

        for index, prereq in relevant_distribution_prereq.iterrows():
            relevant_scif = self.scif[self.scif.template_group.str.encode('utf-8') == prereq.template_group.encode('utf-8')]
            relevant_a_df = self._filter_df(relevant_scif, {prereq.a_filter_1:prereq.a_value_1, prereq.a_filter_2:prereq.a_value_2})
            relevant_b_df = self._filter_df(relevant_scif, {prereq.b_filter: np.array(self.sanitize_values(prereq.b_value)).astype(int).tolist()})

            if not relevant_a_df.empty and not relevant_b_df.empty:
                unique_sub_category_fk_in_b_df = relevant_b_df[prereq.b_filter].unique()
                result_dict['numerator_result'] = len(unique_sub_category_fk_in_b_df)
                if len(unique_sub_category_fk_in_b_df) >= prereq.b_threshold_1:
                    result_dict['result'] = float(prereq.Score) / max_score_potential
                    result_dict['score'] = prereq.Score
                    break
            else:
                mandatory_survey_result = self.get_relevant_survey_result(self.survey, [prereq.mandatory_survey_question_fk])
                if mandatory_survey_result:
                    additional_survey_question_fks = np.array(self.sanitize_values(prereq.additional_survey_question_fk)).astype(int)
                    additional_survey_result = self.get_relevant_survey_result(self.survey, additional_survey_question_fks)
                    if additional_survey_result >= prereq.additional_survey_question_requirement_pass:
                        result_dict['result'] = float(prereq.Score) / max_score_potential
                        result_dict['score'] = prereq.Score
                        break

        return result_dict


    def _filter_scif(self, row, df):
        columns_in_scif = row.index[np.in1d(row.index, df.columns)]
        for column_name in columns_in_scif:
            if pd.notna(row[column_name]) and not df.empty:
                df = df[df[column_name].isin(self.sanitize_values(row[column_name]))]
        return df

    @staticmethod
    def sanitize_values(item):
        if pd.isna(item):
            return item
        else:
            if isinstance(item, int):
                return [str(item)]
            else:
                items = [x.strip() for x in item.split(',')]
                return items

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
    def _filter_df(df, filters, exclude=0):
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

    def _get_parent_name_from_kpi_name(self, kpi_name):
        template = self.templates[KPIS]
        parent_kpi_name = \
            template[template[KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')][PARENT_KPI].iloc[0]
        if parent_kpi_name and pd.notna(parent_kpi_name):
            return parent_kpi_name
        else:
            return None

    @staticmethod
    def _logic_for_checking_if_product_other_exisits_for_sos(scif):
        other_unique_product_types_in_scif = scif[scif.product_type == 'Other'].manufacturer_name.unique()
        if ('TCCC' in other_unique_product_types_in_scif and len(other_unique_product_types_in_scif) == 1) or not other_unique_product_types_in_scif:
            result = True
        else:
            result = False
        return result

    @staticmethod
    def get_relevant_survey_result(survey_response_df,relevant_question_fk):
        result = 0
        if survey_response_df.empty:
            return 0
        accepted_results = ['Si', 1, u'1', u'Si']
        for question_fk in relevant_question_fk:
            relevant_survey_response = survey_response_df[survey_response_df['question_fk'].isin([question_fk])]
            if not relevant_survey_response.empty:
                survey_answer = relevant_survey_response.loc[:,'selected_option_text'].iat[0]
                if survey_answer in accepted_results:
                    result = result + 1
        return result






