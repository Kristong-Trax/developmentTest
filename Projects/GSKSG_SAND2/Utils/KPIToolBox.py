
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
import pandas as pd
import os
import numpy as np
from KPIUtils_v2.DB.CommonV2 import Common
# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
from KPIUtils_v2.Calculations.SOSCalculations import SOS
from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'jasmine'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

######################
relevant_cols = 'relevant columns'
template_kpi_type = 'type of condition'
GENERAL_COLS = ['template_name']
EXCLUDE = 0
SURVEY_QUEST = 'Survey Question Text'
#####################
MSL = 'MSL List'
#####################
STORE_LVL_1 = 'store_type'
STORE_LVL_2 = 'retailer_name'
STORE_LVL_3 = 'additional_attribute_1'
######################
SURVEY_SHEET = 'Survey'
######################
KPI_NAME_INDEX = 0
KPI_WEIGHT = 2
CONDITION_WEIGHT = 3
######################

TBD = 'tbd'

class GSKSGToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.common = Common(self.data_provider)
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.common.get_kpi_static_data()
        self.kpi_results_queries = []
        self.store_type = self.store_info[STORE_LVL_1].values[0]
        self.templates_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data')
        self.excel_file_path = os.path.join(self.templates_path, 'template.xlsx')
        self.survey_file = pd.read_excel(self.excel_file_path, sheetname=SURVEY_SHEET)
        self.msl_list = pd.read_excel(self.excel_file_path,
                   header=[[0, 1, 2]],
                   sheetname=MSL).dropna()

        self.calculations = {'SOS': self.calculate_sos, 'MSL': self.calculate_MSL, 'sequence': self.calculate_sequence,
                             'presence': self.calculate_presence, 'facings': self.calculate_facings,
                             'No facings': self.calculate_no_facings, 'Survey': self.calculate_survey}
        self.sequence = Sequence(data_provider, ignore_stacking=True)
        self.availability = Availability(data_provider, ignore_stacking=True)
        self.sos = SOS(data_provider, self.output)
        self.survey = Survey(data_provider, self.output)
        # self.survey_data = self.data_provider.survey_responses

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        template = self.get_relevant_calculations()
        self.handle_calculation(template)

        score = 0
        return score

    def is_template_relevant(self, template_names):
        """

        :param template_names: a string of template names seperated by ','
        :return:
        """
        in_session = set(self.scif['template_name'])
        in_template = set([val.strip(' \n') for val in str(template_names).split(',')])
        return 1 if (in_session & in_template) else 0

    def handle_calculation(self, kpis):
        # kpi_results = dict()



        kpi_results = pd.DataFrame(columns=['1st level', '2nd Level', '3rd Level','valid_template_name',
                                            'KPI Weight', 'Weight',
                                            'Score Method', 'Benchmark', 'Conditional Weight', 'result'])


        # for each level3:
        for i in xrange(len(kpis)):
            current_kpi = kpis.iloc[i]

            # check if relevant template name exists!!!!!!!!!!!!!!! =0 //1
            is_template_valid = self.is_template_relevant(current_kpi['template_name'])

            # will calculate only if there is at least ont template_name from template in session.
            result = self.calculate_atomic(current_kpi) if is_template_valid else 0
            if result is None:
                continue

            kpi_results = kpi_results.append({'1st level': current_kpi['1st level'],
                                              '2nd Level': current_kpi['2nd Level'],
                                              '3rd Level': current_kpi['3rd Level'],
                                              'KPI Weight': current_kpi['KPI Weight'],
                                              'Weight': current_kpi['Weight'],
                                              'Score Method': current_kpi['Score Method'],
                                              'Benchmark': current_kpi['Benchmark'],
                                              'Conditional Weight': current_kpi['Conditional Weight'],
                                              'result': result, 'valid_template_name': is_template_valid},
                                             ignore_index=True)

        kpi_results['Conditional Weight'] = kpi_results['Conditional Weight'].fillna(-1)

        aggs_res = kpi_results.groupby(['1st level', '2nd Level', '3rd Level', 'KPI Weight', 'Weight', 'Score Method',
                     'Benchmark', 'Conditional Weight'], as_index=False)[['result', 'valid_template_name']].sum()

        # if not && condinal weight is NA MSL conditional weight + this KPI weight


        ## write to db
        # ## asking if themplate isnt valid to write to db
        for i in xrange(len(aggs_res)):
            result = aggs_res.iloc[i]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result['3rd Level'])

            identifier_child_fk = self.common.get_dictionary(
                kpi_fk=kpi_fk,
                kpi_level2=self.common.get_kpi_fk_by_kpi_type(result['2nd Level']),
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result['1st level']))
            identifier_parent_fk = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(result['2nd Level']),
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result['1st level']))

            #numerator /  denominator understnad
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=TBD, score=result['result'],
                                           denominator_id=TBD,
                                           identifier_result=identifier_child_fk,
                                           identifier_parent=identifier_parent_fk,
                                           target=TBD, should_enter=True)

        ## if method binary change result to 100/0
        aggs_res.loc[(aggs_res['Score Method'] == 'Binary')&
                     (aggs_res['result'] >= aggs_res['Benchmark']), 'result_bin'] = 100
        aggs_res.loc[
            (aggs_res['Score Method'] == 'Binary') & (aggs_res['result'] < aggs_res['Benchmark']), 'result_bin'] = 0

        aggs_res.loc[
            (aggs_res['Benchmark'] == 'Pass'), 'result_bin'] = aggs_res['result']

        aggs_res['result_bin'] = np.where(aggs_res['Score Method'] == 'Proportional', aggs_res['result'],
                                          aggs_res['result_bin'])
        aggs_res['result_bin'] = aggs_res['result_bin']*(aggs_res['Weight']/100)

        aggs_res['valid_template_name'] = aggs_res['valid_template_name'].astype(float)

        # aggregating atomic results to kpi level
        aggs_res_level_2 = aggs_res.groupby(
            ['1st level', '2nd Level', 'KPI Weight', 'Conditional Weight'], as_index=False).agg(
            {'valid_template_name': 'max',  'result_bin': np.sum})

        # takes conditional weight for irrelevnat kpis that has it.
        aggs_res_level_2.loc[(aggs_res_level_2['valid_template_name'] == 0) &
                     (aggs_res_level_2['Conditional Weight'] != -1), 'result_bin'] = aggs_res_level_2['Conditional Weight']



        sets = aggs_res_level_2['1st level'].unique()

        # The kpis to 'take weight' from for the MSL.
        invalid_templates = aggs_res_level_2.loc[(aggs_res_level_2['valid_template_name'] == 0) &
                                        (aggs_res_level_2['Conditional Weight'] == -1) &
                                        (aggs_res_level_2['2nd Level'] != 'MSL')]

        invalid_templates = invalid_templates.groupby(['1st level'], as_index=False)[['KPI Weight']].sum()
        invalid_templates = invalid_templates.rename(columns={'KPI Weight': 'PLUS_WEIGHT'})
        invalid_templates['2nd Level'] = 'MSL'
        aggs_res_level_2 = aggs_res_level_2.merge(invalid_templates, on=['1st level', '2nd Level'], how='left')

        aggs_res_level_2 = aggs_res_level_2.loc[~(aggs_res_level_2['valid_template_name'] == 0) |
                                                ~(aggs_res_level_2['Conditional Weight'] == -1)]

        aggs_res_level_2['PLUS_WEIGHT'] = aggs_res_level_2['PLUS_WEIGHT'].fillna(0)
        aggs_res_level_2['KPI Weight'] += aggs_res_level_2['PLUS_WEIGHT']

        aggs_res_level_2.loc[aggs_res_level_2['valid_template_name'] == 0, 'result_bin'] = 1

        aggs_res_level_2['total_result'] =  aggs_res_level_2['KPI Weight'] * aggs_res_level_2['result_bin']

        ## write to db level 2 kpis

        for i in xrange(len(aggs_res_level_2)):
            result = aggs_res_level_2.iloc[i]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result['2nd Level'])

            identifier_child_fk = self.common.get_dictionary(
                kpi_fk=kpi_fk,
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result['1st level']))
            identifier_parent_fk = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(result['1st level']))

            # numerator /  denominator understnad
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=TBD, score=result['total_result'],
                                           denominator_id=TBD,
                                           identifier_result=identifier_child_fk,
                                           identifier_parent=identifier_parent_fk,
                                           target=TBD, should_enter=True)

        # fixind data for 1st level



        # aggregating to level 3:
        aggs_res_level_1 = aggs_res_level_2.groupby(
            ['1st level'], as_index=False)['total_result'].sum()

        # write to db

        for i in xrange(len(aggs_res_level_1)):
            result = aggs_res_level_1.iloc[i]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result['1st level'])

            identifier_child_fk = self.common.get_dictionary(
                kpi_fk=kpi_fk)

            # numerator /  denominator understnad
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=TBD, score=result['total_result'],
                                           denominator_id=TBD,
                                           identifier_result=identifier_child_fk,
                                           target=TBD, should_enter=True)






    def get_relevant_calculations(self):
        # Gets the store type name and the relevant template according to it.
        store_type = self.store_info['store_type'].values[0].title()

        # Gets the relevant kpis from template
        template = pd.read_excel(self.excel_file_path, sheetname=store_type)
        return template

    def calculate_atomic(self, row):
        # gets the atomic kpi's calculation type and run the relevant calculation according to it,
        kpi_type = row[template_kpi_type]

        # runs the relevant calculation
        calculation = self.calculations.get(kpi_type, '')
        if calculation or calculation == 0:
            return calculation(row)
        else:
            Log.info('kpi type {} does not exist'.format(kpi_type))
            return None

    def calculate_sos(self, row):
        # Calculates the sos kpi according to the template.
        filters, general_filters = self.get_filters(row)
        return self.sos.calculate_share_of_shelf(sos_filters=filters, **general_filters)

    def calculate_presence(self, row):

        return self.calculate_facings(row)

    def calculate_facings(self, row, no_facing=False):

        target = row['target'] if not pd.isnull(row['target']) else 0
        row_filter, general_filters = self.get_filters(row)
        row_filter.update(general_filters)
        result = self.availability.calculate_availability(**row_filter)
        if no_facing:
            return int(result < target)
        return int(result >= target)

    def calculate_no_facings(self, row):
        return self.calculate_facings(row, no_facing=True)

    def calculate_MSL(self, row):
        """This function gets the relevant assortment, and returns the number of shown is session out of assortment"""
        # ToDo: use the benchmark as needed if needed.

        target = row['target'] if not pd.isnull(row['target']) else 0

        # Gets relevant assortment from template according to store attributes.
        store_data = (self.store_type, self.store_info[STORE_LVL_2].values[0], self.store_info[STORE_LVL_3].values[0])

        # if store attribute are not defined in template, fail the kpi.
        if isinstance(self.msl_list.get(store_data, None), type(None)):
            Log.info('Store attribute {} is not in template.'.format(store_data))
            return 0

        # gets the assortment product's ean codes relevant for store
        store_assortment = self.msl_list[store_data]
        store_assortment = store_assortment[store_assortment == 1]
        products = store_assortment.keys()

        kpi_filters, general = self.get_filters(row)
        kpi_filters.update(general)
        total_products = len(products)
        exist_products = 0
        # Checks for each product if passed, if so, count it.
        for product in set(products):
            kpi_filters['product_ean_code'] = str(product)
            res = self.availability.calculate_availability(**kpi_filters)
            if res:
                exist_products += 1
        #     write to 4h level?

        res = float(exist_products) / total_products if total_products else 0
        return res >= target


    def calculate_sequence(self, row):
        sequence_filter, general_filters = self.get_filters(row)

        # running sequence kpi, allowing empty spaces, not allowing Irrelevant.
        # assuming should pass in ALL relevant scenes.
        # If an entity in sequence is missing (less than 1 facing)- will fail.

        # assuming sequence organs are defined by only one filter!
        if len(sequence_filter) == 1:
            key = sequence_filter.keys()[0]
            sequence_filter = (key, sequence_filter[key])
            result = self.sequence.calculate_product_sequence(sequence_filter, direction='left', **general_filters)
        else:
            result = None
            Log.info('More than 1 filter was applied for sequence organs- Not supported!')
        return result


#need to check below
    # def calculate_survey(self, row):
    #     group_of_question = self.survey_file[(self.survey_file['KPI Name'] == row['3rd Level']) & (self.store_info[STORE_LVL_1] in self.survey_file['Store Policy'])]
    #     target = group_of_question.iloc[0]['target']
    #     counter = 0
    #     for quest in group_of_question.itertuples():
    #         answer = self.survey_data[self.survey_data['question_text'] == quest['Survey Question Text']]
    #         if ~ self.quest[answer['selected_option_text'] == quest['Accepted Answers']].empty: #not empty
    #             counter = counter + 1
    #             if counter >= target:
    #                 return 100
    #     return 0

    def calculate_survey(self, row):
        """
        gets the relevant survey for atomic.
        assuming there is only one survey in atomic, if not- will calculate only the first.
        Handles the case where there are same atomics name in different store types.
        Contains may cause 'x' to be found in 'xz', therefore not enough as a check.
        """

        # Gets the atomic's survey
        atomic_name = row['3rd Level']
        rows = self.survey_file.loc[(self.survey_file['KPI Name'] == atomic_name)
                              & (self.survey_file['Store Policy'].str.contains(self.store_type, case=False))]
        rows['match_policy'] = rows.apply(self.ensure_policy, axis=1)
        rows = rows.loc[rows['match_policy'] == 1]

        if len(rows) > 1:
            Log.info('More than one survey question for atomic- calculating only first survey')

        # Get the survey relevant data
        survey_data = rows.iloc[0]
        question = survey_data['Survey Question Text']
        target_answer = survey_data['Compare to Target']

        # return whether the given answer matches the target answer.
        return self.survey.check_survey_answer(question, target_answer)

    def ensure_policy(self, row):
        # This checks if the store policy matches the store policy required
        relevant_stores = map(str.strip, map(str.upper, (str(row['Store Policy']).split(','))))
        return 1 if self.store_type.upper() in relevant_stores else 0

    def get_filters(self, row):
        filters = {}
        general_filters = {}
        # gets the relevant column names to consider in kpi
        cols = map(str.strip, str(row[relevant_cols]).split(','))
        for col in cols:
            # column must exist
            if col in row.keys():
                # handle the values in column
                if col == 'exclude':
                    excludes = self.handle_complex_data(row[col], exclude=True)
                    filters.update(excludes)
                    continue
                if col in ['target','Store Type']:
                    continue
                if col == 'denominator':
                    denom = self.handle_complex_data(row[col])
                    general_filters.update(denom)
                    continue
                elif self.is_string_a_list(str(row[col])):
                    value = map(str.strip, str(row[col]).split(','))
                else:
                    value = [row[col]]

                # add the filter to relevant dictionary
                if col in GENERAL_COLS:
                    general_filters[col] = value
                else:
                    filters[col] = value
            else:
                Log.info('attribute {} is not in template'.format(col))

        return filters, general_filters

    @staticmethod
    def is_string_a_list(str_value):
        # checks whether a string is representing a list of values
        return len(str_value.split(',')) > 0

    def handle_complex_data(self, value, exclude=False):
        # value is string of dictionary format with multi values, for example 'product_type:Irrelevant, Empty;
        # scene_id:34,54'

        exclude_dict = {}
        # gets the different fields
        fields = value.split(';')
        for field in fields:

            # gets the key and value of field
            field = field.split(':')
            key = field[0]
            if key == 'product_type':
                values = {'Irrelevant', 'Empty', 'POS', 'SKU', 'Other'} - set(map(str.strip, str(field[1]).split(',')))
                exclude_dict[key] = list(values)
            else:
                values = map(str.strip, str(field[1]).split(','))
                exclude_dict[key] = (values, EXCLUDE) if exclude else values

        return exclude_dict