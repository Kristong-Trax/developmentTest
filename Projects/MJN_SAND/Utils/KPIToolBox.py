
import pandas as pd
from datetime import datetime
import os
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Projects.MJN_SAND.Utils.ParseTemplates import parse_template
from Projects.MJN_SAND.Utils.Fetcher import MJNCN_SANDQueries
from Projects.MJN_SAND.Utils.GeneralToolBox import MJNCN_SANDGENERALToolBox

__author__ = 'Yasmin'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
FIRST_SKU_TAB = 'Must Have SKU'
SECOND_SKU_TAB = 'Must+Optional SKU'
FIRST = 1
SECOND = 2
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')

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


class MJNCN_SANDToolBox:
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
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = MJNCN_SANDGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.custom_templates = {}
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.scif_filters = {'Product EAN': 'product_ean_code', 'Template Name':'template_name', 'att1':'att1'}

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = MJNCN_SANDQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def get_custom_template(self, name):
        if name not in self.custom_templates.keys():
            template = parse_template(TEMPLATE_PATH, sheet_name=name)
            if template.empty:
                template = parse_template(TEMPLATE_PATH, name, 2)
            self.custom_templates[name] = template
        return self.custom_templates[name]

    def main_calculation(self, kpi_set_fk):
        """
        This function calculates the KPI results.
        """
        score = 0
        set_name = self.kpi_static_data.loc[self.kpi_static_data['kpi_set_fk'] == kpi_set_fk]['kpi_set_name'].iloc[0]
        kpi_template = parse_template(TEMPLATE_PATH, 'KPIs')
        kpi_template = kpi_template.loc[kpi_template['KPI Name'] == set_name]
        for index, row in kpi_template.iterrows():
            kpi_name = row['KPI Name']
            if row['KPI Type'] == 'Facing Count':
                relevant_tab = row['Custom Sheet']
                must_have_sku = FIRST if relevant_tab == FIRST_SKU_TAB else SECOND
                score = self.count_facing(relevant_tab, must_have_sku)
            elif row['KPI Type'] == 'Count of Scenes':
                score = self.count_scenes(kpi_name, 'Display')
            elif row['KPI Type'] == 'Count of POSM':
                score = self.count_posm(kpi_name, row['Scene Types'])
            elif row['KPI Type'] == 'Brand Blocking':
                score = self.calculate_block(row['Custom Sheet'])
        return score

    def count_facing(self, tab, relevant_kpi):
        template = self.get_custom_template(tab)
        template = template.loc[template['Store Type'] == self.store_type]
        passed_skus = pd.DataFrame(columns=['sku', 'score'])
        filters_to_get = ['Product EAN', 'Template Name'] if relevant_kpi == FIRST \
            else ['Product EAN', 'Template Name', 'att1']
        for i in xrange(len(template)):
            row = template.iloc[i]
            kpi_name = row['Product Name']
            filters = self.get_filters_from_row(row, filters_to_get)
            result = self.tools.calculate_assortment(**filters)
            score = 1 if result >= 1 else 0  # target is 1 facing
            self.write_to_level_2_and_3(kpi_name=kpi_name, atomic_name=kpi_name,
                                        result=result, score=score)
            passed_skus = passed_skus.append({'sku': row['Product Name'], 'score':score}, ignore_index=True)
        kpi_score = 10 if passed_skus['score'].sum(axis=0) == len(passed_skus) else 0
        return kpi_score

    def count_posm(self, kpi_name, scene_type):
        display_by_scene_type = self.get_custom_template('Scene Mapping')
        display_by_scene_type = display_by_scene_type.loc[display_by_scene_type['TYPE'] == scene_type]
        relevant_displays = display_by_scene_type['name'].tolist()
        filters = {'display_name': relevant_displays}
        result = self.tools.calculate_posm_count(**filters)
        score = 10 if result >= 1 else 0
        self.write_to_level_2_and_3(kpi_name=kpi_name, atomic_name=kpi_name,
                                    result=result, score=score)
        return score

    def count_scenes(self, kpi_name, scene_type):
        scenes_type = scene_type.split(',') if scene_type else scene_type
        filters = {'template_name': scenes_type}
        result = self.tools.calculate_number_of_scenes(**filters)
        score = 10 if result >= 1 else 0
        self.write_to_level_2_and_3(kpi_name=kpi_name, atomic_name=kpi_name,
                                    result=result, score=score)
        return score

    def calculate_block(self, tab):
        template = self.get_custom_template(tab)
        for index, row in template.iterrows():
            filters = {'brand_fk': row['fk']}
            blocked_scenes, total_scenes = self.tools.calculate_block_together(result_by_scene=True, **filters)
            score = 10 if blocked_scenes == total_scenes else 0
            self.write_to_level_2_and_3(kpi_name=row['Brand Name'], atomic_name=row['Brand Name'],
                                        result=score, score=score)
        return

    def get_filters_from_row(self, row, row_filters):
        """
        :param row: the row in template to get filters from
        :param row_filters: the filters needed (columns names in template)
        :return: a dictionary of filters in scif
        """
        filters = {}
        for current_filter in row_filters:
            if not pd.isnull(row[current_filter]):
                if current_filter in self.scif_filters:
                    if current_filter == 'att1':
                        filters[self.scif_filters[current_filter]] = int(row[current_filter])

                    else:
                        filters[self.scif_filters[current_filter]] = row[current_filter]
                else:
                    filters[current_filter] = row[current_filter]
        return filters

    def write_to_level_2_and_3(self, kpi_name, atomic_name, result, score):
        kpi_fk = self.kpi_static_data[self.kpi_static_data['kpi_name'] == kpi_name].iloc[0]['kpi_fk']
        atomic_fk = self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'] == atomic_name].iloc[0]['atomic_kpi_fk']

        self.write_to_db_result(atomic_fk, result=result, level=self.LEVEL3, score=score)
        self.write_to_db_result(kpi_fk, result=score, score=score, level=self.LEVEL2)

    def write_to_db_result(self, fk, result, level, score2=None, score=None, threshold=None):
            """
            This function the result data frame of every KPI (atomic KPI/KPI/KPI set),
            and appends the insert SQL query into the queries' list, later to be written to the DB.
            """
            attributes = self.create_attributes_dict(fk, result, level, score2=score2, score=score, threshold=threshold)
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

    def create_attributes_dict(self, fk, result, level, score2=None, score=None, threshold=None):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        float(format(score, '.2f')) if isinstance(score, float) else score
        float(format(result, '.2f')) if isinstance(result, float) else result
        # result = round(result, 2)
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            score_type = ''
            # attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
            #                             format(result, '.2f'), score_type, fk)],
            #                           columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
            #                                    'score_2', 'kpi_set_fk'])
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        result, score_type, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'score_2', 'kpi_set_fk'])

        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0].replace("'",
                                                                                                                "\\'")
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score, score2)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score',
                                               'score_2'])
        elif level == self.LEVEL3:

            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0].replace("'", "\\'")
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            if not score and not threshold:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, None, 0)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk', 'threshold',
                                                   'score'])
            else:
                attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                            self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                            result, kpi_fk, fk, threshold, score)],
                                          columns=['display_text', 'session_uid', 'kps_name', 'store_fk',
                                                   'visit_date',
                                                   'calculation_time', 'result', 'kpi_fk', 'atomic_kpi_fk',
                                                   'threshold',
                                                   'score'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()
    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        cur = self.rds_conn.db.cursor()
        delete_queries = MJNCN_SANDQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
