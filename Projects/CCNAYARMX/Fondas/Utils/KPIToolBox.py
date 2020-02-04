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
AVALIABILITY = 'Availability'
SOS = 'SOS'
SHARE_OF_EMPTY = 'Share of Empty'
SURVEY = 'Survey'
DISTRIBUTION = 'Distribution'
ASSORTMENT = 'Assortment'
SCORING = 'Scoring'

# Excel Column Names
RELEVANT_ASSORTMNENT = 'Relevent Assortment'
ASSORTMENT1 = 'Assortment1'
ASSORTMENT2 = 'Assortment2'

# Dataframe Column Names
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'
TASK_TEMPLATE_GROUP = 'Task/ Template Group'
TEMPLATE_NAME = 'template_name'
MANUFACTURER_NAME = 'manufacturer_name'
RELEVANT_QUESTION_FK = 'question_fk'

RESULT = 'result'

# Excel Sheets
SHEETS = [KPIS, AVALIABILITY, SOS, SHARE_OF_EMPTY, SURVEY, DISTRIBUTION, ASSORTMENT, SCORING]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data',
                             'Nayar Comidas Fondas Template.xlsx')


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
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.assortment_template = self.templates[ASSORTMENT]
        # self.survey = Survey(self.data_provider, output=output, ps_data_provider=self.ps_data_provider,
        #                      common=self.common_v2)
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        store_type = 'Fondas-Rsr'
        store_additional_attribute = self.sanitize_values(
            'FONDA / LONCHERÍA / MERENDERO,RSR COMIDA MEXICANA / TACOS,RSR ASIAN,RSR SEAFOOD,RSR LOCAL FOOD,RSR PIZZAS,RSR SANDWICHES / TORTERIA,RSR POLLO,RSR HAMBURGUESAS,RSR OTROS ALIMENTOS')
        store_additional_attribute = [unicode(value,'utf-8') for value in store_additional_attribute]
        if self.store_info.additional_attribute_5.isin(store_additional_attribute)[0] and self.store_info.store_type.isin([store_type])[0]:
            relevant_kpi_template = self.templates[KPIS]

            foundation_kpi_types = [DISTRIBUTION,SURVEY]
            foundation_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin(foundation_kpi_types)]
            # distribution_kpi_template = relevant_kpi_template[relevant_kpi_template[KPI_TYPE].isin([])]

        self._calculate_kpis_from_template(foundation_kpi_template)

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

        relevant_assortment = self.sanitize_values(self.assortment_template.loc[
            self.assortment_template[KPI_NAME] == parent_kpi_name, row[RELEVANT_ASSORTMNENT]].iat[0])
        relevant_scif = self.filter_df(self.scif, {'template_group': row['template_group']})
        result_of_current_assortment = any(np.in1d(relevant_assortment, relevant_scif))
        # relevant_survey_result = self.results_df.loc[self.results_df['kpi_name'] == 'Visible-Fondas-Rsr',RESULT][0]

        numerator_id = self.scif.product_fk.iat[0] if not self.scif.product_fk.empty else 0
        denominator_id = self.scif.sub_category_fk.iat[0] if not self.scif.sub_category_fk.empty else 0
        if result_of_current_assortment:
            result = 100
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id,
                       'result': result}
        return result_dict

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
