
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
from KPIUtils_v2.GlobalDataProvider.PsDataProvider import PsDataProvider
import pandas as pd
import os

from Projects.CCNAYARMX_SAND2.Data.LocalConsts import Consts

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

__author__ = 'huntery'

KPIS = 'KPIs'
PER_SCENE_SOS = 'Per Scene SOS'
SOS = 'SOS'
SURVEY = 'Survey'
SURVEY_PASSTHROUGH = 'Survey Passthrough'
AVAILABILITY = 'Availability'
PLATFORMAS = 'Platformas'
PLATFORMAS_SCORING = 'Platformas Scoring'
SCORING = 'Scoring'
ROLLBACKS = 'Rollbacks'
BAY_COUNT = 'Bay Count'
TOTEM = 'Totem'

PORTAFOLIO_PRODUCTS = 'Portafolio Products Details'

SHEETS = [PER_SCENE_SOS, SOS, SURVEY, SURVEY_PASSTHROUGH, AVAILABILITY, PLATFORMAS, PLATFORMAS_SCORING, ROLLBACKS,
          BAY_COUNT, SCORING, KPIS, PORTAFOLIO_PRODUCTS, TOTEM]

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'INSERTSTUFFHERE')

STORE_TYPE = 'store_type'
KPI_NAME = 'KPI Name'
KPI_TYPE = 'KPI Type'
PARENT_KPI = 'Parent KPI'


class ToolBox(GlobalSessionToolBox):

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.results_df = pd.DataFrame(columns=['kpi_name', 'kpi_fk', 'numerator_id', 'numerator_result',
                                                'denominator_id', 'denominator_result', 'result', 'score',
                                                'identifier_result', 'identifier_parent', 'should_enter'])
        self.templates = {}
        self.parse_template()
        self.own_manuf_fk = int(self.data_provider.own_manufacturer.param_value.values[0])
        self.scif = self.scif[self.scif['product_type'] != 'Irrelevant']
        self.scene_survey_results = self.get_scene_survey_response()
        self.session_survey_results = self.get_session_survey_response()
        self.custom_entities = self.ps_data_provider.get_custom_entities(1002)

    def parse_template(self):
        for sheet in SHEETS:
            self.templates[sheet] = pd.read_excel(TEMPLATE_PATH, sheet_name=sheet)

    def main_calculation(self):
        relevant_kpi_template = self.templates[KPIS]
        att2 = self.store_info['additional_attribute_2'].iloc[0]
        relevant_kpi_template = relevant_kpi_template[(relevant_kpi_template[STORE_TYPE].isnull()) |
                                                      (relevant_kpi_template[STORE_TYPE].str.contains(
                                                          att2))
                                                      ]
        foundation_kpi_types = [PER_SCENE_SOS, BAY_COUNT, SURVEY, SURVEY_PASSTHROUGH, AVAILABILITY, SOS,
                                ROLLBACKS]

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
        identifier_results = self.results_df[self.results_df['result'].notna()]['identifier_result'].unique().tolist()
        self.results_df['result'] = self.results_df.apply(
            lambda row: pd.np.nan if pd.notna(row['identifier_parent']) and row[
                'identifier_parent'] not in identifier_results else row['result'], axis=1)
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
        elif kpi_type == PER_SCENE_SOS:
            return self.calculate_per_scene_sos
        elif kpi_type == AVAILABILITY:
            return self.calculate_availability
        elif kpi_type == SURVEY:
            return self.calculate_survey
        elif kpi_type == SCORING:
            return self.calculate_scoring
        elif kpi_type == PLATFORMAS_SCORING:
            return self.calculate_platformas_scoring

    def _get_parent_name_from_kpi_name(self, kpi_name):
        template = self.templates[KPIS]
        parent_kpi_name = \
            template[template[KPI_NAME].str.encode('utf-8') == kpi_name.encode('utf-8')][PARENT_KPI].iloc[0]
        if parent_kpi_name and pd.notna(parent_kpi_name):
            return parent_kpi_name
        else:
            return None

    def calculate_assortment(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        sku_kpi_name = kpi_name + ' - SKU'
        sku_kpi_fk = self.get_kpi_fk_by_kpi_type(sku_kpi_name)
        results_list = []

        relevant_scif = self.scif.copy()

        template_name = self.does_exist(row, TEMPLATE_NAME)
        if template_name:
            relevant_scif = relevant_scif[relevant_scif['template_name'].isin(template_name)]

        sub_category = row['sub_category']

        relevant_template = self.templates[PORTAFOLIO_PRODUCTS]
        relevant_template = relevant_template[relevant_template['Client Subcategory'] == sub_category]

        passing_products = 0
        for i, sku_row in relevant_template.iterrows():
            facings = relevant_scif[relevant_scif['product_name'] == sku_row['Product Local Name']]['facings'].sum()
            product_fk = self._get_product_fk_from_name(sku_row['Product Local Name'])
            target = sku_row['Minimum Number of Facings']
            score = 1 if facings >= target else 0
            result_dict = {'kpi_name': sku_kpi_name, 'kpi_fk': sku_kpi_fk, 'numerator_id': product_fk,
                           'denominator_id': self.store_id, 'result': facings, 'score': score, 'target': target,
                           'identifier_parent': kpi_name}
            results_list.append(result_dict)
            if score == 1:
                passing_products += 1

        result = 1 if passing_products == len(relevant_template) else 0
        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        results_list.append(result_dict)
        return results_list

    def calculate_availability(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        template_name = row['Template_name']

        totem_products = self.templates[TOTEM]['POSM'].unique().tolist()

        relevant_scif = self.scif[self.scif['template_name'] == template_name]
        if relevant_scif.empty:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        relevant_scif = relevant_scif[relevant_scif['product_name'].isin(totem_products)]

        unique_products = len(relevant_scif['product_name'].unique().tolist())
        target = row['target']

        if unique_products >= target:
            result = 1
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict

    def calculate_sos(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        template_name = row['Template_name']

        denominator_scif = self.scif[(self.scif['template_name'] == template_name) &
                                     (self.scif['product_type'] != 'POS')]
        if denominator_scif.empty:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        denominator_id = denominator_scif['template_fk'].iloc[0]
        denominator_facings = denominator_scif['facings'].sum()

        sub_category = row['sub_category']
        if pd.notna(sub_category):
            numerator_scif = denominator_scif[denominator_scif['sub_category'] == sub_category]
            numerator_id = numerator_scif['sub_category_fk'].iloc[0]

        manufacturer_name = row['manufacturer_name']
        if pd.notna(manufacturer_name):
            numerator_scif = denominator_scif[denominator_scif['manufacturer_name'] == manufacturer_name]
            numerator_id = numerator_scif['manufacturer_fk'].iloc[0]

        numerator_facings = numerator_scif['facings'].sum()

        result = numerator_facings / float(denominator_facings)
        min_target, max_target = self._get_target_range(row['target'])

        if min_target <= result * 100 <= max_target:
            score = 1
        else:
            score = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': numerator_id,
                       'denominator_id': denominator_id, 'result': result, 'score': score}
        return result_dict

    @staticmethod
    def _get_target_range(target_range):
        min_target, max_target = [int(x) for x in target_range.split('-')]
        return min_target, max_target

    def calculate_per_scene_sos(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        template_name = row['Template_name']
        relevant_scif = self.scif[(self.scif['template_name'] == template_name) &
                                  (self.scif['product_type'] != 'POS')]
        if relevant_scif.empty:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        denominator_scif = relevant_scif.groupyby('scene_id', as_index=False)['facings'].sum()
        denominator_scif.rename(columns={'facings': 'denominator'}, inplace=True)

        numerator_scif = relevant_scif[relevant_scif['manufacturer_name'] == 'TCCC']
        numerator_scif = numerator_scif.groupby('scene_id', as_index=False)['facings'].sum()
        numerator_scif.rename(columns={'facings': 'numerator'}, inplace=True)

        merged_df = pd.merge(denominator_scif, numerator_scif, how='left', on='scene_id').fillna(0)
        merged_df['sos'] = (merged_df['numerator'] / merged_df['denominator'])

        min_target, max_target = self._get_target_range(row['target'])

        merged_df['passing'] = merged_df['sos'].apply(lambda x: min_target <= x <= max_target)

        failing_scenes = merged_df[~merged_df['passing']]
        if failing_scenes.empty:
            result = 1
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict

    def calculate_bay_count(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        template_name = self.does_exist(row, 'Template_name')
        relevant_scif = self.scif[self.scif['template_name'].isin(template_name)]
        if relevant_scif.empty:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        scenes = relevant_scif['scene_id'].unique().tolist()
        products = relevant_scif['product_fk'].unique().tolist()

        bay_mpis = self.matches[(self.matches['scene_id'].isin(scenes)) &
                                (self.matches['product_fk'].isin(products))]

        bays = len(bay_mpis['bay_number'].unique().tolist())
        target_mapping = self._get_target_mapping(row['targets'])
        target = target_mapping.get(self.store_info['additional_attribute_1'].iloc[0])
        if not target:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        if bays > target:
            result = 1
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict

    def calculate_planograma(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        template_name = self.does_exist(row, 'Template_name')
        relevant_scif = self.scif[self.scif['template_name'].isin(template_name)]
        if relevant_scif.empty:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        brands = self.does_exist(row, 'Brands')
        minimum_brands_required = row['minimum number of brands']

        relevant_scif = relevant_scif[relevant_scif['brand_name'].isin(brands)]

        unique_brands = relevant_scif['brand_name'].unique().tolist()

        if len(unique_brands) >= minimum_brands_required:
            result = 1
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict

    def calculate_communicacion(self, row):
        pass

    def calculate_cooler_cc(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
        brand_kpi_name = kpi_name + ' - Brand'
        brand_kpi_fk = self.get_kpi_fk_by_kpi_type(brand_kpi_name)
        results_list = []

        relevant_question_fks = self.does_exist(row, 'Survey Question PK')

        relevant_results = \
            self.scene_survey_results[self.scene_survey_results['question_fk'].isin(relevant_question_fks)]

        cooler_brands = relevant_results['selected_option_text'].unique().tolist()
        if cooler_brands:
            for cooler_brand_name in cooler_brands:
                if cooler_brand_name and pd.notna(cooler_brand_name):
                    cooler_brand_fk = self._get_cooler_brand_fk_by_cooler_brand_name(cooler_brand_name)
                    result_dict = {'kpi_name': brand_kpi_name, 'kpi_fk': brand_kpi_fk, 'numerator_id': cooler_brand_fk,
                                   'denominator_id': self.store_id, 'result': 1, 'identifier_parent': kpi_name}
                    results_list.append(result_dict)

        if len(cooler_brands) > 1:
            result = 1
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        results_list.append(result_dict)
        return results_list

    def _get_cooler_brand_fk_by_cooler_brand_name(self, cooler_brand_name):
        entity = self.custom_entities[self.custom_entities['name'] == cooler_brand_name]
        if entity.empty:
            return None
        else:
            return entity['pk'].iloc[0]

    @staticmethod
    def _get_target_mapping(column_value):
        target_dict = {}
        for pair in column_value.split(','):
            key, value = pair.split(';')
            target_dict.update({key: int(value)})
        return target_dict

    def calculate_survey(self, row):
        pass

    def calculate_scene_survey(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        relevant_question_fks = self.does_exist(row, 'Survey Question PK')
        required_answer = row['Required Answer']

        relevant_results = \
            self.scene_survey_results[self.scene_survey_results['question_fk'].isin(relevant_question_fks)]

        if relevant_results.empty:
            return {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'result': pd.np.nan}

        failing_results = relevant_results[relevant_results['selected_option_text'] != required_answer]
        if failing_results.empty:
            result = 1
        else:
            result = 0

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result}
        return result_dict

    def calculate_scoring_survey(self, row):
        kpi_name = row[KPI_NAME]
        kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)

        relevant_question_fks = self.does_exist(row, 'Survey Question PK')

        relevant_results = \
            self.session_survey_results[self.session_survey_results['question_fk'].isin(relevant_question_fks)]

        if relevant_results.empty:
            result = 0
            score = 0
        else:
            score = len(relevant_results['selected_option_text'].iloc[0]) / float(3)
            result = 1

        result_dict = {'kpi_name': kpi_name, 'kpi_fk': kpi_fk, 'numerator_id': self.own_manuf_fk,
                       'denominator_id': self.store_id, 'result': result, 'score': score}
        return result_dict

    def _get_product_fk_from_name(self, product_name):
        return self.all_products[self.all_products['product_name'] == product_name]['product_fk'].iloc[0]

    def get_scene_survey_response(self):
        query = """SELECT session_uid,question_fk,selected_option_text, sce.pk as 'scene_id'
                FROM probedata.scene_survey_response res
                LEFT JOIN probedata.scene sce ON res.scene_fk =  sce.pk
                WHERE session_uid = '{}';""".format(self.session_uid)

        scene_survey_response = pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)
        return scene_survey_response

    def get_session_survey_response(self):
        query = """SELECT session_uid,question_fk,selected_option_text
                        FROM probedata.survey_response res
                        WHERE session_uid = '{}';""".format(self.session_uid)

        session_survey_response = pd.read_sql_query(query, self.ps_data_provider.rds_conn.db)
        return session_survey_response

    @staticmethod
    def does_exist(kpi_line, column_name):
        """
        checks if kpi_line has values in this column, and if it does - returns a list of these values
        :param kpi_line: line from template
        :param column_name: str
        :return: list of values if there are, otherwise None
        """
        if column_name in kpi_line.keys() and kpi_line[column_name] != "":
            cell = kpi_line[column_name]
            if type(cell) in [int, float]:
                return [cell]
            elif type(cell) in [unicode, str]:
                return [x.strip() for x in cell.split(",")]
        return None


