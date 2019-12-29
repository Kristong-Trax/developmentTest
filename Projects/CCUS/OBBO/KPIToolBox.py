import os
from datetime import datetime

import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log

from Projects.CCUS.OBBO.Fetcher import OBBOQueries
from Projects.CCUS.OBBO.GeneralToolBox import OBBOGENERALToolBox
from Projects.CCUS.OBBO.ParseTemplates import parse_template

__author__ = 'Nimrod'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'Data', 'Template.xlsx')


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


class OBBOConsts(object):

    STORE_TYPE = 'Store Type'
    KPI_NAME = 'KPI Name'
    PROGRAM = 'Program'

    SR_NAME = 'LOCATION SR'
    DISPLAY_NAME = 'DISPLAY Name'
    DISPLAY_TYPE = 'DISPLAY TYPE'

    SR_TO_INCLUDE = 'SR to Include'
    SR_TO_EXCLUDE = 'SR to Exclude'
    OPTIONAL_SR = 'Optional SR'
    TEMPLATE_GROUP = 'Template Group'

    KPI_GROUP = 'KPI Group'
    CATEGORY_COUNT = 'Category Availability Count'
    POP = 'POP'
    POP_AND_SR = 'Mandatory POP&SR'

    PRODUCT_TYPE = 'Product Type'
    BRAND_NAME = 'Brand Name'
    ATT2 = 'att2'
    ATT3 = 'att3'

    REFERENCE_GROUP1 = 'Reference Group #1'
    REFERENCE_GROUP2 = 'Reference Group #2'
    CONDITION1 = 'Condition #1'
    CONDITION2 = 'Condition #2'

    SEPARATOR = ','


class OBBOToolBox(OBBOConsts):

    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3

    def __init__(self, data_provider, output):
        self.k_engine = BaseCalculationsScript(data_provider, output)
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scenes_info = self.data_provider[Data.SCENES_INFO].merge(self.data_provider[Data.ALL_TEMPLATES],
                                                                      how='left', on='template_fk', suffixes=['', '_y'])
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_type = self.data_provider[Data.STORE_INFO]['store_type'].iloc[0]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.scif = self.get_missing_attributes(self.data_provider[Data.SCENE_ITEM_FACTS])
        self.display_matches = self.get_match_display()
        self.custom_templates = {}
        self.sr_to_templates = self.get_sr_to_templates()
        self.tools = OBBOGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn, scif=self.scif)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []

    def get_match_display(self):
        """
        This function extracts the display matches data and saves it into one global data frame.
        The data is taken from probedata.match_display_in_scene.
        """
        query = OBBOQueries.get_match_display(self.session_uid)
        match_display = pd.read_sql_query(query, self.rds_conn.db)
        return match_display

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = OBBOQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_missing_attributes(self, df):
        query = OBBOQueries.get_missing_attributes_data()
        missing_attributes = pd.read_sql_query(query, self.rds_conn.db)
        merged_df = df.merge(missing_attributes, on='product_fk', how='left', suffixes=['', '_y'])
        return merged_df

    def get_sr_to_templates(self):
        sr_to_templates = {}
        template = self.get_custom_template('LOCATION SR')
        for index, sr in template.iterrows():
            sr_name = sr[self.SR_NAME]
            display_name = sr[self.DISPLAY_NAME]
            display_type = sr[self.DISPLAY_TYPE]
            if sr_name not in sr_to_templates.keys():
                sr_to_templates[sr_name] = []
            if display_name:
                sr_to_templates[sr_name].append(display_name)
            if display_type:
                display_names = self.display_matches[self.display_matches['display_type'] == display_type]
                sr_to_templates[sr_name].extend(display_names['display_name'].unique().tolist())
        return sr_to_templates

    def get_custom_template(self, name):
        if name not in self.custom_templates.keys():
            template = parse_template(TEMPLATE_PATH, sheet_name=name, columns_for_vertical_padding=[self.PROGRAM])
            if template.empty:
                template = parse_template(TEMPLATE_PATH, name, 2, columns_for_vertical_padding=[self.PROGRAM])
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        kpi_template = self.get_custom_template('KPIs')
        program_template = self.get_custom_template('PROGRAM')
        group_results = {}
        kpi_results = {}
        for i, program in program_template.iterrows():
            scenes = self.filter_scenes(program)
            if scenes is None:
                continue
            program_name = program[self.PROGRAM]
            program_optional_sr = program[self.OPTIONAL_SR]
            program_kpis = kpi_template[kpi_template[self.PROGRAM] == program[self.PROGRAM]]
            results = {name: 0 for name in program_kpis[self.KPI_NAME].tolist()}
            for scene in scenes:
                number_of_categories = self.get_number_of_categories(scene, program_name)
                if number_of_categories:
                    pop_status = self.get_pop_products_status(scene, program_name)
                    sr_status = None if not program_optional_sr else self.get_sr_status(scene, program_optional_sr)
                    for y, params in program_kpis.iterrows():
                        kpi_name = params[self.KPI_NAME]
                        if params[self.CATEGORY_COUNT] in number_of_categories:
                            if params[self.POP_AND_SR] == 'Y':
                                if params[self.POP] == pop_status and sr_status:
                                    results[kpi_name] += 1
                                    break
                            elif params[self.POP_AND_SR] == 'N':
                                if params[self.POP] == pop_status or sr_status:
                                    results[kpi_name] += 1
                                    break
                            else:
                                if params[self.POP] == pop_status:
                                    results[kpi_name] += 1
                                    break
            for name in results.keys():
                self.save_results(name, results[name])
                kpi_results[name] = results[name]
                kpi_group = program_kpis[program_kpis[self.KPI_NAME] == name].iloc[0][self.KPI_GROUP]
                if kpi_group not in group_results:
                    group_results[kpi_group] = []
                group_results[kpi_group].append(results[name])

        aggregation_template = self.get_custom_template('AGGREGATIONS')
        for i, aggregation in aggregation_template.iterrows():
            first_condition = aggregation[self.CONDITION1]
            group_scores = []
            for group in aggregation[self.REFERENCE_GROUP1].split(self.SEPARATOR):
                group_scores.extend(group_results.get(group, []))
            if not group_scores:
                continue
            if first_condition == 'SUM of results':
                score = sum(group_scores)
            elif first_condition == 'At least one result above 0':
                if group_scores.count(0) < len(group_scores):
                    score = 100
                else:
                    score = 0
            else:
                Log.warning("Condition '{}' is not valid".format(first_condition))
                continue
            if score > 0:
                second_condition = aggregation[self.CONDITION2]
                if second_condition:
                    group_scores = []
                    for group in aggregation[self.REFERENCE_GROUP2].split(self.SEPARATOR):
                        group_scores.extend(group_results.get(group, []))
                    if second_condition == 'All results are 0':
                        if group_scores.count(0) == len(group_scores):
                            score = 100
                        else:
                            score = 0
                    else:
                        Log.warning("Condition '{}' is not valid".format(first_condition))
                        continue
            kpi_name = aggregation[self.KPI_NAME]
            self.save_results(kpi_name, score)
            kpi_group = aggregation[self.KPI_GROUP]
            if kpi_group:
                if kpi_group not in group_results.keys():
                    group_results[kpi_group] = []
                group_results[kpi_group].append(score)

    def get_number_of_categories(self, scene, program):
        category_template = self.get_custom_template('CATEGORIES')
        data = category_template[category_template[self.PROGRAM] == program]
        number_of_categories = 0
        for i, category_data in data.iterrows():
            filters = dict(scene_id=scene)
            if category_data[self.ATT2]:
                filters['att2'] = category_data[self.ATT2].split(self.SEPARATOR)
            if category_data[self.ATT3]:
                filters['att3'] = category_data[self.ATT3].split(self.SEPARATOR)
            if self.tools.calculate_assortment(**filters):
                number_of_categories += 1
        if number_of_categories == 1:
            number_of_categories = ['1', '1+']
        elif number_of_categories >= 2:
            number_of_categories = ['2+', '1+']
        else:
            number_of_categories = None
        return number_of_categories

    def get_pop_products_status(self, scene, program):
        pop_template = self.get_custom_template('POP')
        data = pop_template[pop_template[self.PROGRAM] == program]
        status = False
        for i, row in data.iterrows():
            if self.tools.calculate_assortment(brand_name=row[self.BRAND_NAME],
                                               product_type=row[self.PRODUCT_TYPE],
                                               scene_id=scene):
                status = True
                break
        return 'Y' if status else 'N'

    def get_sr_status(self, scene, sr):
        status = True
        if sr in self.sr_to_templates.keys():
            sr_names = self.sr_to_templates.get(sr)
            if self.display_matches[(self.display_matches['scene_fk'] == scene) &
                                    (self.display_matches['display_name'].isin(sr_names))].empty:
                status = False
        return status

    def save_results(self, kpi_name, score):
        kpi_data = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name].iloc[0]
        self.write_to_db_result(kpi_data['atomic_kpi_fk'], score, level=self.LEVEL3)
        self.write_to_db_result(kpi_data['kpi_fk'], score, level=self.LEVEL2)

    def filter_scenes(self, program):
        if program[self.STORE_TYPE] and self.store_type not in program[self.STORE_TYPE].split(self.SEPARATOR):
            return None
        scenes = set(self.scenes_info['scene_fk'].unique())
        if program[self.TEMPLATE_GROUP]:
            scenes = scenes.intersection(
                self.scenes_info[self.scenes_info['template_group'].isin(
                    program[self.TEMPLATE_GROUP].split(self.SEPARATOR))]['scene_fk'].unique().tolist())
        if program[self.SR_TO_INCLUDE]:
            sr_names = []
            for sr_to_include in program[self.SR_TO_INCLUDE].split(self.SEPARATOR):
                sr_names.extend(self.sr_to_templates.get(sr_to_include, []))
            scenes = scenes.intersection(
                self.display_matches[self.display_matches['display_name'].isin(sr_names)]['scene_fk'].unique().tolist())
        elif program[self.SR_TO_EXCLUDE]:
            sr_names = []
            for sr_to_exclude in program[self.SR_TO_EXCLUDE].split(self.SEPARATOR):
                sr_names.extend(self.sr_to_templates.get(sr_to_exclude, []))
            scenes = scenes.difference(
                self.display_matches[self.display_matches['display_name'].isin(sr_names)]['scene_fk'].unique().tolist())
        return scenes

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = OBBOQueries.get_delete_session_results_query(self.session_uid, self.kpi_static_data)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
