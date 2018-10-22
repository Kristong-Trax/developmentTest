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
# Template columns in kpi's sheet:
ATOMIC = '3rd_level'
KPI = '2nd_level'
SET = '1st_level'
KPI_TYPE = 'type of condition'
COLS_TO_LOOK = 'relevant columns'
SCORE_METHOD = 'KPI Score Method'
STORE_TYPE = 'store_type'

######################
SURVEY_SHEET = 'Survey'
KPI_SHEET = 'KPIs'
######################
KPI_NAME_INDEX = 0
KPI_WEIGHT = 2
CONDITION_WEIGHT = 3
######################
MANUFACTURER_FK = 1  # GSKSK pk in table static_new.manufacturer
ORAL_FK = 1
PAIN_FK = 4
ORAL_KPI = 2054
ORANGE_SCORE =2056
######################
ORAL_CARE_LEVEL_1 = 'Orange Score for Oral Care'
PAIN_LEVEL_1 = 'Orange score for Pain'
ORAL_CARE = ' Oral Care'
PAIN=' Pain'


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

        self.calculations = {'SOS': self.calculate_sos, 'MSL': self.calculate_MSL, 'Sequence': self.calculate_sequence,
                             'Presence': self.calculate_presence, 'Facings': self.calculate_facings,
                             'No Facings': self.calculate_no_facings, 'Survey': self.calculate_survey}
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
        #self.common.commit_results_data_to_new_tables
        self.common.commit_results_data()

        score = 0
        return score


    def set_calculation_result_by_score_type(self, row):
        if ['Score Method'] == 'Binary':
            if row['result'] > 0:
                return 1
            else:
                return 0
        elif ['Score Method'] == 'Proportional':
            return row['result'] * 100 / row['Benchmark']

        else:
            return row['result']


    def is_template_relevant(self, template_names):
        """

        :param template_names: a string of template names seperated by ','
        :return: 1 if at least one of the template names given was in session, 0 otherwise.
        """
        in_session = set(self.scif['template_name'])
        in_template = set([val.strip(' \n') for val in str(template_names).split(',')])
        return 1 if (in_session & in_template) else 0

    def handle_calculation(self, kpis):

        kpi_results = pd.DataFrame(columns=[SET, KPI, ATOMIC, 'valid_template_name',
                                            'KPI Weight', 'Weight',
                                            'Score Method', 'Benchmark', 'Conditional Weight', 'result'])

        # calculating each calculation in template.
        for i in xrange(len(kpis)):
            current_kpi = kpis.iloc[i]

            # check if relevant template name exists in session.
            is_template_valid = self.is_template_relevant(current_kpi['template_name'])

            # calculates only if there is at least ont template_name from template in the session.
            result = self.calculate_atomic(current_kpi) if is_template_valid else (0, 0, 0)
            if result is None:
                continue

            if isinstance(result, tuple):
                scenes_passed = result[1]
                scenes_total = result[2]
                result = result[0]


            kpi_results = kpi_results.append({SET: current_kpi[SET],
                                              KPI: current_kpi[KPI],
                                              ATOMIC: current_kpi[ATOMIC],
                                              'kpi_type': current_kpi[KPI_TYPE],
                                              'KPI Weight': current_kpi['KPI Weight'],
                                              'Weight': current_kpi['Weight'],
                                              'Score Method': current_kpi['Score Method'],
                                              'KPI Score Method': current_kpi['KPI Score Method'],
                                              'Benchmark': current_kpi['Benchmark'],
                                              'Conditional Weight': current_kpi['Conditional Weight'],
                                              'result': result, 'valid_template_name': is_template_valid,
                                              'scenes_passed':scenes_passed,'scenes_total':scenes_total},
                                             ignore_index=True)

        kpi_results['Conditional Weight'] = kpi_results['Conditional Weight'].fillna(-1)

        # boolean_res = kpi_results.loc[kpi_results['Score Method'] == 'Binary']
        # boolean_res['result'] = boolean_res['result'].astype(bool)
        #
        # boolean_res = boolean_res.groupby([SET, KPI, ATOMIC, 'KPI Weight', 'Weight', 'KPI Score Method',
        #                                    'Benchmark', 'Conditional Weight'], as_index=False).agg({'result': 'all',
        #                                                                                             'valid_template_name': 'sum',
        #                                                                                             'scenes_passed':'sum' ,
        #                                                                                             'scenes_total':'sum'})
        #
        # kpi_results = kpi_results.loc[kpi_results['Score Method'] != 'Binary']
        #
        # aggs_res = kpi_results.groupby([SET, KPI, ATOMIC, 'KPI Weight', 'Weight', 'KPI Score Method',
        #                                                                           'Benchmark', 'Conditional Weight'],
        #                                as_index=False).agg({'result': 'sum', 'valid_template_name': 'sum','scenes_passed':'sum' ,
        #                                                                                             'scenes_total':'sum'})
        #
        # aggs_res = aggs_res.append(boolean_res, ignore_index=True)

        kpi_results = kpi_results.groupby([SET, KPI, ATOMIC, 'KPI Weight', 'Weight', 'KPI Score Method','Score Method',
                                                                               'Benchmark', 'Conditional Weight'], as_index=False).agg({'result': 'sum',
                                                                                                                                        'valid_template_name':'sum',
                                                                                                                                        'scenes_passed':'sum' ,
                                                                                                                                       'scenes_total':'sum'})

        # if not && condinal weight is NA MSL conditional weight + this KPI weight

        ## if method binary change result to 100/0

        # kpi_results.loc[(kpi_results['Score Method'] == 'Binary') &
        #                 (kpi_results['result'] >= kpi_results['Benchmark']), 'result'] = 1
        # kpi_results.loc[(kpi_results['Score Method'] == 'Binary') &
        #                 (kpi_results['result'] < kpi_results['Benchmark']), 'result'] = 0
        # kpi_results['result_bin'] = kpi_results['result']
        ### Changed ###
        # kpi_results.loc[(kpi_results['Score Method'] == 'Proportional')
        #                 & (kpi_results['result'] < kpi_results['Benchmark']), 'result_bin'] = 0

        # kpi_results.loc[kpi_results['Score Method'] == 'Proportional',
        #                 'result_bin'] = (kpi_results['result'] / kpi_results['Benchmark'])
        #


        # kpi_results.loc[
        #     (kpi_results['Benchmark'] == 'Pass'), 'result_bin'] = kpi_results['result']

        kpi_results['result_bin'] = kpi_results.apply(self.set_calculation_result_by_score_type, axis=1)
        kpi_results['result_bin'] = kpi_results['result_bin'] * kpi_results['Weight']

        kpi_results['valid_template_name'] = kpi_results['valid_template_name'].astype(float)


        ## write level3 to db
        store_fk = self.store_info['store_fk'][0]
        # ## asking if template isnt valid to write to db
        for i in xrange(len(kpi_results)):
            result = kpi_results.iloc[i]
            # kpi_fk = self.common.get_kpi_fk_by_kpi_type(result[ATOMIC])
            if result[SET] == PAIN_LEVEL_1:
                kpi_super_fk = self.common.get_kpi_fk_by_kpi_type(result[KPI]+PAIN)
                category_fk = PAIN_FK
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(result[ATOMIC]+PAIN)
            else:
                kpi_super_fk = self.common.get_kpi_fk_by_kpi_type(result[KPI]+ORAL_CARE)
                category_fk = ORAL_FK
                kpi_fk = self.common.get_kpi_fk_by_kpi_type(result[ATOMIC]+ORAL_CARE)

            #web db
            identifier_parent_fk_web = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(result[KPI]),
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result[SET]))

            #supervisor
            identifier_parent_fk_supervisor = self.common.get_dictionary(
                kpi_fk=kpi_super_fk,
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result[SET]))

            # numerator /  denominator understnad
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=MANUFACTURER_FK, result=result['result'],
                                           score=result['result_bin'],
                                           denominator_id=category_fk,
                                           identifier_parent=identifier_parent_fk_web,
                                           numerator_result=result['scenes_passed'],
                                           denominator_result=result['scenes_total'],
                                           weight=result['Weight']*100, should_enter=True)

            self.common.write_to_db_result(fk=kpi_fk, numerator_id=MANUFACTURER_FK, result=result['result'],
                                           score=result['result_bin'],
                                           denominator_id=category_fk,
                                           identifier_parent=identifier_parent_fk_supervisor,
                                           numerator_result=result['scenes_passed'],
                                           denominator_result=result['scenes_total'],
                                           weight=result['Weight']*100, should_enter=True)


        aggs_res_level_2 = kpi_results.groupby([SET, KPI, 'KPI Weight',SCORE_METHOD, 'Conditional Weight'], as_index=False).agg({
                                                                                                    'valid_template_name': 'max',
                                                                                                    'result_bin': np.sum ,
                                                                                                    'scenes_passed': 'sum',
                                                                                                    'scenes_total': 'sum'})
        # takes conditional weight for irrelevnat kpis that has it.
        aggs_res_level_2.loc[(aggs_res_level_2['valid_template_name'] == 0) &
                             (aggs_res_level_2['Conditional Weight'] != -1), 'result_bin'] = aggs_res_level_2[
            'Conditional Weight']

        # sets = aggs_res_level_2[SET].unique()

        # The kpis to 'take weight' from for the MSL.
        invalid_templates = aggs_res_level_2.loc[(aggs_res_level_2['valid_template_name'] == 0) &
                                                 (aggs_res_level_2['Conditional Weight'] == -1) &
                                                 (aggs_res_level_2[KPI] != 'MSL')]

        invalid_templates = invalid_templates.groupby([SET], as_index=False)[['KPI Weight']].sum()
        invalid_templates = invalid_templates.rename(columns={'KPI Weight': 'PLUS_WEIGHT'})
        invalid_templates[KPI] = 'MSL'
        aggs_res_level_2 = aggs_res_level_2.merge(invalid_templates, on=[SET, KPI], how='left')

        aggs_res_level_2 = aggs_res_level_2.loc[~(aggs_res_level_2['valid_template_name'] == 0) |
                                                ~(aggs_res_level_2['Conditional Weight'] == -1)]

        aggs_res_level_2['PLUS_WEIGHT'] = aggs_res_level_2['PLUS_WEIGHT'].fillna(0)
        aggs_res_level_2['KPI Weight'] += aggs_res_level_2['PLUS_WEIGHT']

        aggs_res_level_2.loc[aggs_res_level_2['valid_template_name'] == 0, 'result_bin'] = 1
        aggs_res_level_2.loc[aggs_res_level_2['valid_template_name'] == 0, 'result_bin'] = 1

        aggs_res_level_2.loc[(aggs_res_level_2[SCORE_METHOD] == 'Binary') &
                        (kpi_results['valid_template_name'] > 0), 'result_bin'] = 1
        aggs_res_level_2.loc[(aggs_res_level_2[SCORE_METHOD] == 'Binary') &
                                         (aggs_res_level_2['valid_template_name'] <= 0), 'result_bin'] = 0
        aggs_res_level_2['total_result'] = aggs_res_level_2['KPI Weight'] * aggs_res_level_2['result_bin']

        ## write to db level 2 kpis

        for i in xrange(len(aggs_res_level_2)):
            result = aggs_res_level_2.iloc[i]

            if result[SET] == PAIN_LEVEL_1:
                kpi_super_fk = self.common.get_kpi_fk_by_kpi_type(result[KPI] + PAIN)
                category_fk = PAIN_FK
            else:
                kpi_super_fk = self.common.get_kpi_fk_by_kpi_type(result[KPI] + ORAL_CARE)
                category_fk = ORAL_FK

            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result[KPI])
            identifier_child_super_fk = self.common.get_dictionary(
                kpi_fk=kpi_super_fk,
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result[SET]))

            identifier_child_fk = self.common.get_dictionary(
                kpi_fk=kpi_fk,
                kpi_level1=self.common.get_kpi_fk_by_kpi_type(result[SET]))

            identifier_parent_fk_supervisor = self.common.get_dictionary(
                kpi_fk=self.common.get_kpi_fk_by_kpi_type(result[SET]))

            identifier_parent_fk_web = self.common.get_dictionary(
                kpi_category=self.common.get_kpi_fk_by_kpi_type(result[SET]),kpi_fk=ORANGE_SCORE)


            #supervisor result to db
            self.common.write_to_db_result(fk=kpi_super_fk, numerator_id=MANUFACTURER_FK, result=result['result_bin'],
                                           score=result['total_result'],
                                           denominator_id=category_fk,
                                           numerator_result=result['scenes_passed'],
                                           denominator_result=result['scenes_total'],
                                           identifier_result=identifier_child_super_fk,
                                           identifier_parent=identifier_parent_fk_supervisor,
                                           weight=result['KPI Weight']*100, should_enter=True)

            # web result to db
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=MANUFACTURER_FK, result=result['result_bin'],
                                           score=result['total_result'],
                                           denominator_id=category_fk,
                                           numerator_result=result['scenes_passed'],
                                           denominator_result=result['scenes_total'],
                                           identifier_result=identifier_child_fk,
                                           identifier_parent=identifier_parent_fk_web,
                                           weight=result['KPI Weight']*100, should_enter=True)

        # fixind data for 1st level

        # aggregating to level 3:
        aggs_res_level_1 = aggs_res_level_2.groupby([SET], as_index=False)['total_result'].sum()

        # write to db

        for i in xrange(len(aggs_res_level_1)):
            result = aggs_res_level_1.iloc[i]
            kpi_fk = self.common.get_kpi_fk_by_kpi_type(result[SET])
            category_fk = ORAL_FK if kpi_fk == ORAL_KPI else PAIN_FK
            identifier_child_fk_web = self.common.get_dictionary(
                kpi_category=kpi_fk, kpi_fk=ORANGE_SCORE)
            identifier_child_fk_supervisor = self.common.get_dictionary(
                 kpi_fk=kpi_fk)

            # supervisor result to db
            self.common.write_to_db_result(fk=kpi_fk, numerator_id=MANUFACTURER_FK, score=result['total_result'],
                                           result=result['total_result'],denominator_id=store_fk,
                                           identifier_result=identifier_child_fk_supervisor
                                           ,should_enter=True)
            # web result to db
            self.common.write_to_db_result(fk=ORANGE_SCORE, numerator_id=MANUFACTURER_FK, score=result['total_result'],
                                           result=result['total_result'],
                                           denominator_id=category_fk,
                                           identifier_result=identifier_child_fk_web
                                           ,should_enter=True)


    def get_relevant_calculations(self):
        # Gets the store type name and the relevant template according to it.
        store_type = self.store_info['store_type'].values[0]
        # Gets the relevant kpis from template
        template = pd.read_excel(self.excel_file_path, sheetname=KPI_SHEET)
        template = template.loc[template[STORE_TYPE] == store_type]

        return template

    def calculate_atomic(self, row):
        # gets the atomic kpi's calculation type and run the relevant calculation according to it.
        kpi_type = row[KPI_TYPE]

        # runs the relevant calculation
        calculation = self.calculations.get(kpi_type, '')
        if calculation or calculation == 0:
            return calculation(row)
        else:
            Log.info('kpi type {} does not exist'.format(kpi_type))
            return None

    def calculate_sos(self, row):
        # Calculates the sos kpi according to the template.

        target = row['target'] if not pd.isnull(row['target']) else 0
        #
        # filters, general_filters = self.get_filters(row)
        templates = [val.strip(' \n') for val in str(row['template_name']).split(',')]
        valid_scenes = self.scif.loc[self.scif['template_name'].isin(templates)]['scene_id'].unique()
        scene_passed_count = 0
        for scene_id in valid_scenes:
            row['scene_id'] = scene_id
            filters, general_filters = self.get_filters(row)
            res = self.sos.calculate_share_of_shelf(sos_filters=filters, **general_filters)
            res = res >= target if not pd.isnull(row['target']) else res
            scene_passed_count = scene_passed_count+1 if res else scene_passed_count
        row = row.drop('scene_id')
        filters, general_filters = self.get_filters(row)
        res = self.sos.calculate_share_of_shelf(sos_filters=filters, **general_filters)
        return res, scene_passed_count, len(valid_scenes)

    def calculate_presence(self, row):

        return self.calculate_facings(row)

    def calculate_facings(self, row, no_facing=False):
        """This function calculates facing from given filter, and return True if at least
         one scene had facing as neede by target. Assuming row has a target (if not, target =0).
         returns whether kpi passed, the number of scenes passed and total scenes checked/"""

        facing_scenes_counted = 0
        no_facing_scenes_counted = 0
        passed = False
        target = row['target'] if not pd.isnull(row['target']) else 0
        row_filter, general_filters = self.get_filters(row)
        row_filter.update(general_filters)
        # Gets relevant scenes
        templates = [val.strip(' \n') for val in str(row['template_name']).split(',')]
        valid_scenes = self.scif.loc[self.scif['template_name'].isin(templates)]['scene_id'].unique()
        for scene_id in valid_scenes:
            # Checks for each product if found in scene, if so, 'count' it.
            row_filter['scene_id'] = scene_id
            result = self.availability.calculate_availability(**row_filter)
            if no_facing:
                if int(result < target):
                    no_facing_scenes_counted += 1
                    passed = True
            else:
                if int(result >= target):
                    facing_scenes_counted += 1
                    passed = True

        if no_facing:
            return passed, no_facing_scenes_counted, len(valid_scenes)
        return passed, facing_scenes_counted, len(valid_scenes)

    def calculate_no_facings(self, row):
        return self.calculate_facings(row, no_facing=True)

    def calculate_MSL(self, row):
        """This function gets the relevant assortment,
         and returns the number of shown is session out of assortment.
         counting by each scene number of product available out of MSL assortment.
         if at least one scene has result  > target, atomic passes.
         returns: whether at least one scene passed, number of scene passed, number of scene checked
         """

        target = row['target'] if not pd.isnull(row['target']) else 0

        scene_passed = False
        products_in_scenes = pd.DataFrame(columns=['product_ean_code', 'scene_id', 'result'])
        # Gets relevant assortment from template according to store attributes.
        store_data = (self.store_type, self.store_info[STORE_LVL_2].values[0], self.store_info[STORE_LVL_3].values[0])

        # if store attribute are not defined in template, fail the kpi.
        if isinstance(self.msl_list.get(store_data, None), type(None)):
            Log.info('Store attribute {} is not in template.'.format(store_data))
            return 0

        # gets the assortment product's ean codes relevant for store
        store_assortment = self.msl_list[store_data]
        store_assortment = store_assortment[store_assortment == 1]

        ##filter the products from the template by category
        # products = store_assortment.keys()
        # products = [str(prod) for prod in products ]
        category_fk = PAIN_FK if row[SET] == PAIN_LEVEL_1 else ORAL_FK
        # products = self.products.loc[self.products['product_ean_code'].isin(products)]
        # products = products.loc[products['category_fk'] == category_fk]['product_ean_code']

        products = self.scif.loc[(self.scif['in_assort_sc'] == 1) &
                                 (self.scif['category_fk'] == category_fk)]['product_ean_code']

        # (self.scif['in_assort_sc'] == 1) &
        # (self.scif['rlv_dist_sc'] == 1) &
        kpi_filters, general = self.get_filters(row)
        kpi_filters.update(general)
        total_products = len(products)

        # get the relevant scene by the template name given
        templates = [val.strip(' \n') for val in str(row['template_name']).split(',')]
        valid_scenes = self.scif.loc[self.scif['template_name'].isin(templates)]['scene_id'].unique()

        # save which products were in each relevant scene
        for scene_id in valid_scenes:
            # Checks for each product if found in scene, if so, 'count' it.
            for product in set(products):
                # kpi_filters['product_ean_code'] = str(product)
                # kpi_filters['scene_id'] = scene_id
                # res = self.availability.calculate_availability(**kpi_filters)
                product_in_scene = self.scif.loc[(self.scif['rlv_dist_sc'] == 1) &
                                                 (self.scif['scene_id'] == scene_id) &
                                                 (self.scif['product_ean_code'] == str(product))
                                                 ][['product_ean_code', 'scene_id', 'rlv_dist_sc']]
                res = 1 if not product_in_scene.empty else 0
                products_in_scenes = products_in_scenes.append({
                    'product_ean_code': product,
                    'scene_id': scene_id,
                    'result': res,
                }, ignore_index=True)

        sum_exist = len(products_in_scenes[products_in_scenes['result'] != 0]['product_ean_code'].unique())
        scene_passed_count = len(products_in_scenes[products_in_scenes['result'] != 0]['scene_id'].unique())
        # for scene_id in valid_scenes:
        #     in_scene = products_in_scenes.loc[products_in_scenes['scene_id'] == scene_id]
        #     exist_products = in_scene['result'].sum()
        res = float(sum_exist) / total_products if total_products else 0
        #     if res >= target:
        #         scene_passed = True
        #         scene_passed_count += 1
        # sum_exist = float(sum_exist) / total_products if total_products else 0

        return res, scene_passed_count, len(valid_scenes)

    def calculate_sequence(self, row):
        sequence_filter, general_filters = self.get_filters(row)

        # running sequence kpi, allowing empty spaces, not allowing Irrelevant.
        # assuming should pass in ALL relevant scenes.
        # If an entity in sequence is missing (less than 1 facing)- will fail.

        # assuming sequence organs are defined by only one filter!
        if len(sequence_filter) == 1:
            key = sequence_filter.keys()[0]
            sequence_filter = (key, sequence_filter[key])
            result = self.sequence.calculate_product_sequence(sequence_filter, direction='left', min_required_to_pass=1,
                                                              **general_filters)
        else:
            result = None
            Log.info('More than 1 filter was applied for sequence organs- Not supported!')
        return result, 0, 0

    def calculate_survey(self, row):
        """
        gets the relevant survey for atomic.
        assuming there is only one survey in atomic, if not- will calculate only the first.
        Handles the case where there are same atomics name in different store types.
        Contains may cause 'x' to be found in 'xz', therefore not enough as a check.
        """

        # Gets the atomic's survey
        atomic_name = row[ATOMIC]
        rows = self.survey_file.loc[(self.survey_file['KPI Name'] == atomic_name)
                                    & (self.survey_file['Store Policy'].str.contains(self.store_type, case=True))]
        rows['match_policy'] = rows.apply(self.ensure_policy, axis=1)
        rows = rows.loc[rows['match_policy'] == 1]

        if len(rows) > 1:
            Log.info('More than one survey question for atomic- calculating only first survey')

        # Get the survey relevant data
        survey_data = rows.iloc[0]
        question = survey_data[SURVEY_QUEST]
        target_answer = survey_data['Compare to Target']

        # return whether the given answer matches the target answer.
        return self.survey.check_survey_answer(question, target_answer), 0, 0

    def ensure_policy(self, row):
        # This checks if the store policy matches the store policy required
        relevant_stores = map(str.strip, map(str.upper, (str(row[STORE_TYPE]).split(','))))
        return 1 if self.store_type.upper() in relevant_stores else 0

    def get_filters(self, row):
        filters = {}
        general_filters = {}
        # gets the relevant column names to consider in kpi
        cols = map(str.strip, str(row[COLS_TO_LOOK]).split(','))
        for col in cols:
            # column must exist
            if col in row.keys():
                # handle the values in column
                if col == 'exclude':
                    excludes = self.handle_complex_data(row[col], exclude=True)
                    filters.update(excludes)
                    continue
                if col in ['target', STORE_TYPE]:
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
