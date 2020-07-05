from __future__ import division

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
TEMPLATE_GROUP = 'template_group'

SHEETS = [KPIS, SOS, SHARE_OF_EMPTY, SURVEY, POSM_AVAILABILITY, DISTRIBUTION, COMBO, SCORING]
COLUMNS = ['scene_match_fk', 'scene_fk', TEMPLATE_GROUP, 'template_fk', 'brand_fk', 'brand_name', 'manufacturer_fk',
           'manufacturer_name', 'category_fk', 'category', 'product_type', 'product_fk', 'facings']
LOGIC = {
    'and': op.and_
}


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
        self.own_manufacturer = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.all_templates = self.data_provider[Data.ALL_TEMPLATES]
        self.project_templates = {}
        self.parse_template()
        self.store_type = self.store_info['store_type'].iloc[0]
        self.survey = Survey(self.data_provider, output, ps_data_provider=self.ps_data_provider, common=self.common)
        self.att2 = self.store_info['additional_attribute_2'].iloc[0]
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])

        self.products = self.data_provider[Data.PRODUCTS]
        scif = self.scif[['brand_fk', 'facings', 'product_type']].groupby(by='brand_fk').sum()
        self.mpis = self.matches \
            .merge(self.products, on='product_fk', suffixes=['', '_p']) \
            .merge(self.scene_info, on='scene_fk', suffixes=['', '_s']) \
            .merge(self.all_templates[['template_fk', TEMPLATE_GROUP]], on='template_fk') \
            .merge(scif, on='brand_fk')[COLUMNS]
        self.mpis['store_fk'] = self.store_id

        self.calculations = {
            COMBO: self.calculate_combo,
            POSM_AVAILABILITY: self.calculate_posm_availability,
            SCORING: self.calculate_scoring,
            SHARE_OF_EMPTY: self.calculate_share_of_empty,
            SOS: self.calculate_sos,
            SURVEY: self.calculate_survey,
        }

    def parse_template(self):
        for sheet in SHEETS:
            self.project_templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        if not self.store_type == 'Fondas-Rsr':
            return

        relevant_kpi_template = self.project_templates[KPIS]
        sos_kpi_template = self.filter_df(relevant_kpi_template, filters={KPI_TYPE: SOS})
        soe_kpi_template = self.filter_df(relevant_kpi_template, filters={KPI_TYPE: SHARE_OF_EMPTY})
        survey_kpi_template = self.filter_df(relevant_kpi_template, filters={KPI_TYPE: SURVEY})
        posm_kpi_template = self.filter_df(relevant_kpi_template, filters={KPI_TYPE: POSM_AVAILABILITY})
        combo_kpi_template = self.filter_df(relevant_kpi_template, filters={KPI_TYPE: COMBO})
        scoring_kpi_template = self.filter_df(relevant_kpi_template, filters={KPI_TYPE: SCORING})
        sub_scoring_kpi_template = self.filter_df(scoring_kpi_template, filters={KPI_NAME: scoring_kpi_template[PARENT_KPI]}, exclude=True)
        meta_scoring_kpi_template = self.filter_df(scoring_kpi_template, filters={KPI_NAME: scoring_kpi_template[PARENT_KPI]})

        self._calculate_kpis_from_template(sos_kpi_template)
        self._calculate_kpis_from_template(soe_kpi_template)
        self._calculate_kpis_from_template(survey_kpi_template)
        self.calculate_distribution()
        self._calculate_kpis_from_template(posm_kpi_template)
        self._calculate_kpis_from_template(sub_scoring_kpi_template)
        self._calculate_kpis_from_template(combo_kpi_template)
        self._calculate_kpis_from_template(meta_scoring_kpi_template)
        self.save_results_to_db()

    def _calculate_kpis_from_template(self, template_df):
        for i, row in template_df.iterrows():
            calculation_function = self.calculations.get(row[KPI_TYPE])
            try:
                kpi_row = self.project_templates[row[KPI_TYPE]][
                    self.project_templates[row[KPI_TYPE]][KPI_NAME].str.encode('utf-8') == row[KPI_NAME].encode('utf-8')
                    ].iloc[0]
            except IndexError:
                return

            result_data = calculation_function(kpi_row)
            if result_data:
                weight = row['Score']
                if weight and pd.notna(weight) and pd.notna(result_data['result']) and 'score' not in result_data:
                    result_data['score'] = weight * result_data['result']
                parent_kpi_name = self._get_parent_name_from_kpi_name(result_data['kpi_name'])
                if parent_kpi_name and 'identifier_parent' not in result_data.keys():
                    result_data['identifier_parent'] = parent_kpi_name
                if 'identifier_result' not in result_data:
                    result_data['identifier_result'] = result_data['kpi_name']
                if result_data['result'] <= 1:
                    result_data['result'] = result_data['result'] * 100
                if 'numerator_id' not in result_data:
                    result_data['numerator_id'] = self.own_manufacturer
                if 'denominator_id' not in result_data:
                    result_data['denominator_id'] = self.store_id
                self.results_df.loc[len(self.results_df), result_data.keys()] = result_data

    def calculate_distribution(self):
        distribution_template = self.project_templates[DISTRIBUTION] \
            .rename(columns={'store_additional_attribute_2': 'store_size'})
        distribution_template['additional_brands'] = distribution_template \
            .apply(lambda row: int(row['constraint'].split()[0]), axis=1)

        kpi_name = distribution_template.at[0, KPI_NAME]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)

        # anchor_brands = self.sanitize_values(distribution_template.at[0, 'a_value'])
        try:
            anchor_brands = [int(brand) for brand in distribution_template.at[0, 'a_value'].split(",")]
        except AttributeError:
            anchor_brands = [distribution_template.at[0, 'a_value']]

        try:
            template_groups = [template_group.strip() for template_group in distribution_template.at[0, TEMPLATE_GROUP].split(',')]
        except AttributeError:
            template_groups = [distribution_template.at[0, TEMPLATE_GROUP]]

        anchor_threshold = distribution_template.at[0, 'a_test_threshold_2']
        anchor_df = self.filter_df(self.mpis, filters={TEMPLATE_GROUP: template_groups, 'brand_fk': anchor_brands})
        if (anchor_df['facings'] >= anchor_threshold).empty:
            score = result = 0

        try:
            target_brands = [int(brand) for brand in distribution_template.at[0, 'b_value'].split(",")]
        except AttributeError:
            target_brands = [distribution_template.at[0, 'b_value']]

        target_threshold = distribution_template.at[0, 'b_threshold_2']
        target_df = self.filter_df(self.mpis, filters={TEMPLATE_GROUP: template_groups, 'brand_fk': target_brands})
        num_target_brands = len(target_df[target_df['facings'] >= target_threshold]['brand_fk'].unique())
        store_size = self.store_info.at[0, 'additional_attribute_2']

        distribution = self.filter_df(
            distribution_template,
            filters={'additional_brands': num_target_brands, 'store_size': store_size})

        if distribution.empty:
            max_constraints = distribution_template \
                .groupby(by=['store_size'], as_index=False) \
                .max()
            distribution = self.filter_df(max_constraints, filters={'store_size': store_size})

        score = distribution.iloc[0]['Score']
        parent_kpi = distribution.iloc[0][PARENT_KPI]
        max_score = self.filter_df(self.project_templates[KPIS], filters={KPI_NAME: parent_kpi}).iloc[0]['Score']
        result = score / max_score * 100
        numerator_result = len(self.filter_df(self.mpis, filters={
            TEMPLATE_GROUP: template_groups,
            'manufacturer_fk': self.own_manufacturer,
            'product_type': 'SKU'}))
        denominator_result = len(self.filter_df(self.mpis, filters={
            TEMPLATE_GROUP: template_groups,
            'product_type': ['SKU', 'Irrelevant']}))

        result_dict = {
            'kpi_name': kpi_name,
            'kpi_fk': kpi_id,
            'numerator_id': self.own_manufacturer,
            'numerator_result': numerator_result,
            'denominator_id': self.store_id,
            'denominator_result': denominator_result,
            'result': result,
            'score': score,
            'identifier_parent': parent_kpi,
            'identifier_result': kpi_name
        }

        self.results_df.loc[len(self.results_df), result_dict.keys()] = result_dict

    def calculate_share_of_empty(self, row):
        target = row['target']
        numerator_param1 = row[NUMERATOR_PARAM_1]
        numerator_value1 = row[NUMERATOR_VALUE_1]

        kpi_name = row[KPI_NAME]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_groups = row[TEMPLATE_GROUP].split(',')
        denominator_scif = self.filter_df(self.scif, filters={TEMPLATE_GROUP: template_groups})
        denominator_scif = self.filter_df(denominator_scif, filters={'product_type': 'POS'}, exclude=True)
        numerator_scif = self.filter_df(denominator_scif, filters={numerator_param1: numerator_value1})
        template_id = self.filter_df(self.all_templates, filters={TEMPLATE_GROUP: template_groups})['template_fk'].unique()[0]

        result_dict = {
            'kpi_name': kpi_name,
            'kpi_fk': kpi_id,
            'numerator_id': self.own_manufacturer,
            'denominator_id': template_id,
            'result': 0}

        if not numerator_scif.empty:
            denominator_result = denominator_scif.facings.sum()
            numerator_result = numerator_scif.facings.sum()
            result = (numerator_result / denominator_result)
            result_dict['numerator_result'] = numerator_result
            result_dict['denominator_result'] = denominator_result
            result_dict['result'] = self.calculate_sos_score(target, result)

        return result_dict

    def calculate_sos(self, row):
        kpi_name = row[KPI_NAME]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        template_groups = self.sanitize_values(row[TEMPLATE_GROUP])
        product_types = row['product_type'].split(",")

        den_df = self.filter_df(self.mpis, filters={TEMPLATE_GROUP: template_groups, 'product_type': product_types})
        num_param = row[NUMERATOR_PARAM_1]
        num_val = row[NUMERATOR_VALUE_1]
        num_df = self.filter_df(den_df, filters={num_param: num_val})

        try:
            ratio = len(num_df) / len(den_df)
        except ZeroDivisionError:
            ratio = 0

        target = row['target']
        result = self.calculate_sos_score(target, ratio)

        result_dict = {
            'kpi_name': kpi_name,
            'kpi_fk': kpi_id,
            'numerator_id': self.own_manufacturer,
            'denominator_id': num_df[row[DENOMINATOR_ENTITY]].mode().iloc[0],
            'result': result
        }

        return result_dict

    def calculate_posm_availability(self, row):
        # if dominant kpi passed, skip
        result = 100
        max_score = row['KPI Total Points']
        if row['Dominant KPI'] != 'Y':
            result = 50
            dom_kpi = self.filter_df(self.project_templates['POSM Availability'],
                                     filters={'Parent KPI': row['Parent KPI'], 'Dominant KPI': 'Y'}
                                     )
            dom_name = dom_kpi.iloc[0][KPI_NAME]
            max_score = dom_kpi.iloc[0]['KPI Total Points']
            dom_score = self.filter_df(self.results_df, filters={'kpi_name': dom_name}).iloc[0]['result']
            if dom_score > 0:
                result = 0

        kpi_name = row['KPI Name']
        kpi_fk = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        product_fks = [int(product) for product in str(row['product_fk']).split(',')]
        template_fks = self.get_template_fk(row['template_name'])
        filtered_df = self.filter_df(self.mpis, filters={'template_fk': template_fks, 'product_fk': product_fks})

        if filtered_df.empty:
            result = 0

        score = max_score * result / 100

        try:
            denominator_id = filtered_df['template_fk'].mode().iloc[0]
        except IndexError:
            denominator_id = template_fks[0]

        result_dict = {
            'kpi_fk': kpi_fk,
            'kpi_name': kpi_name,
            'denominator_id': denominator_id,
            'result': result,
            'score': score,
        }

        return result_dict

    def calculate_survey(self, row):
        """
        Determines whether the calculation passes based on if the survey response in `row` is 'Si' or 'No'.

        :param row: Row of template containing Survey question data.
        :return: Dictionary containing KPI results.
        """

        kpi_name = row[KPI_NAME]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        result = 1 if self.survey.get_survey_answer(row['KPI Question']).lower() == 'si' else 0
        result_dict = {
            'kpi_name': kpi_name,
            'kpi_fk': kpi_id,
            'numerator_id': self.own_manufacturer,
            'denominator_id': self.store_id,
            'result': result
        }

        return result_dict

    def calculate_scoring(self, row):
        kpi_name = row[KPI_NAME]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)
        component_kpi = [comp.strip() for comp in row['Component KPIs'].split(',')]
        component_df = self.filter_df(self.results_df, filters={'kpi_name': component_kpi})
        score = component_df['score'].sum()
        result = score if kpi_name == "ICE-Fondas-Rsr" else score / row['Score'] * 100

        result_dict = {
            'kpi_name': kpi_name,
            'kpi_fk': kpi_id,
            'numerator_id': self.own_manufacturer,
            'denominator_id': self.store_id,
            'result': result,
            'score': score,
        }

        return result_dict

    def calculate_combo(self, row):
        kpi_name = row[KPI_NAME]
        kpi_id = self.common.get_kpi_fk_by_kpi_name(kpi_name)

        a_filter = row['a_filter']
        a_value = row['a_value']

        component_kpi = [comp.strip() for comp in self.filter_df(self.project_templates['Scoring'], filters={KPI_NAME: a_value}).iloc[0]['Component KPIs'].split(",")]
        component_df = self.filter_df(self.results_df, filters={'kpi_name': component_kpi})
        a_test = row['a_test']
        a_score = component_df[a_test].sum()
        a_threshold = row['a_threshold']
        a_check = a_score >= a_threshold

        template_groups = row[TEMPLATE_GROUP]
        b_filter = row['b_filter']
        b_value = row['b_value'].split(",")
        b_threshold = row['b_threshold']
        b_check = len(self.filter_df(self.mpis, filters={TEMPLATE_GROUP: template_groups, b_filter: b_value})) >= b_threshold

        func = LOGIC.get(row['b_logic'].lower())
        result = int(func(a_check, b_check))

        result_dict = {
            'kpi_name': kpi_name,
            'kpi_fk': kpi_id,
            'result': result,
        }

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
        template = self.project_templates[KPIS]
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
                items = [item.strip() for item in item.split(',')]
                return items

    def save_results_to_db(self):
        self.results_df.drop(columns=['kpi_name'], inplace=True)
        self.results_df.rename(columns={'kpi_fk': 'fk'}, inplace=True)
        self.filter_df(self.results_df, filters={'identifier_parent': None}, func=pd.Series.notnull)['should_enter'] = True
        # self.results_df.loc[self.results_df['identifier_parent'].notnull(), 'should_enter'] = True
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

    @staticmethod
    def calculate_sos_score(target, result):
        """
        Determines whether `result` is greater than or within the range of `target`.

        :param target: Target value as either a minimum value or a '-'-separated range.
        :param result: Calculation result to compare to 1target1.
        :return: 1 if `result` >= `target` or is within the `target` range.
        """

        score = 0
        if pd.notna(target):
            target = [int(n) for n in str(target).split('-')]  # string cast redundant?
            if len(target) == 1:
                score = int(result*100 >= target[0])
            if len(target) == 2:
                score = int(target[0] <= result*100 <= target[1])
        return score

    @staticmethod
    def filter_df(df, filters, exclude=False, func=pd.Series.isin):
        """
        :param df: DataFrame to filter.
        :param filters: Dictionary of column-value list pairs to filter by.
        :param exclude:
        :param func: Function to determine inclusion.
        :return: Filtered DataFrame.
        """

        vert = op.inv if exclude else op.pos
        func = LOGIC.get(func, func)

        for col, val in filters.items():
            if not hasattr(val, '__iter__'):
                val = [val]
            try:
                if isinstance(val, pd.Series) and val.any() or pd.notna(val[0]):
                    df = df[vert(func(df[col], val))]
            except TypeError:
                df = df[vert(func(df[col]))]
        return df

    def get_fk(self, entity, df):
        df

    def get_template_fk(self, template_name):
        """
        :param template_name: Name of template.
        :return: ID of template.
        """

        template_df = self.filter_df(self.all_templates, filters={'template_name': template_name})
        template_fks = template_df['template_fk'].unique()

        return template_fks
