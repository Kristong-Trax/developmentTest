from datetime import datetime
import pandas as pd
import os
from Trax.Algo.Calculations.Core.CalculationsScript import BaseCalculationsScript
from Trax.Algo.Calculations.Core.DataProvider import Data
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.Programs.Utils.Fetcher import NEW_OBBOQueries
from Projects.CCUS.Programs.Utils.GeneralToolBox import NEW_OBBOGENERALToolBox
from Projects.CCUS.Programs.Utils.ParseTemplates import parse_template
from KPIUtils.GlobalDataProvider.PsDataProvider import PsDataProvider

__author__ = 'Ortal'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'
TEMPLATE_GROUP = ['Programs', 'Display']
ATT_5_LIST = ['National-Meals', 'Customer-Meals', 'Local-Meals', 'No POP', 'National-Beauty',
              'Local-Beauty', 'Customer-Beauty', 'National-Wellness', 'Local-Wellness', 'Customer-Wellness',
              'National-Sports', 'Local-Sports', 'Customer-Sports', 'National-Refreshment', 'Local-Refreshment',
              'Customer-Refreshment', 'National-HomeMediaLeisure', 'Cutomer-HomeMediaLeisure', 'Local-HomeMediaLeisure',
              'All Other-Hydration']
STORE_TYPE_LIST = ['LS', 'CR', 'Drug', 'Value']
TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template_v8.xlsx')
CCNA = 'CCNA'


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


class PROGRAMSToolBox:
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
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = NEW_OBBOGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.pop_data = parse_template(TEMPLATE_PATH, 'POP')
        self.adj_data = parse_template(TEMPLATE_PATH, 'Adjacency')
        self.pathway_data = parse_template(TEMPLATE_PATH, 'Pathway')
        self.store_types = parse_template(TEMPLATE_PATH, 'store types')
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_type = self.store_info['store_type'].iloc[0]
        self.ps_data_provider = PsDataProvider(self.data_provider, self.output)
        self.store_areas = self.ps_data_provider.get_store_area_df()

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI Data and saves it into one global Data frame.
        The Data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = NEW_OBBOQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self):
        """
        This function calculates the KPI results.
        """
        scenes = self.scif['scene_fk'].unique().tolist()
        if scenes:
            for scene in scenes:
                scene_data = self.scif.loc[self.scif['scene_fk'] == scene]
                pop_result = self.calculate_pop(scene_data)
                self.calculate_Adjacency(scene_data)
                self.calculate_Pathway(pop_result, scene_data)
                del scene_data
        return

    def calculate_pop(self, scene_data):
        store_list = self.store_types['store types'].tolist()
        for store_type in store_list:
            if self.store_type in store_type:
                pop_new_data = self.pop_data.loc[self.pop_data['store type'] == store_type]
                for index, row in pop_new_data.iterrows():
                    template_group = [str(g) for g in row['Template group'].split(',')]
                    store_areas = [str(g.replace('\n', '')) for g in row['store area'].split(',')]
                    store_areas_in_scene = list(
                        self.store_areas.loc[
                            self.store_areas['scene_fk'] == scene_data['scene_fk'].values[0]]['store_area_name'].values)
                    if scene_data['template_group'].values[0] in template_group or \
                                    set(store_areas) & set(store_areas_in_scene):
                        ean_codes_list = row['product ean code'].split(',')
                        filters = {'product_ean_code': ean_codes_list,
                                   'scene_fk': scene_data['scene_fk'].unique().tolist()}
                        result = self.tools.calculate_availability(**filters)
                        if result > 0:
                            self.write_to_db_result(name='{} POP'.format(scene_data['scene_fk'].values[0]),
                                                    result=row['result'],
                                                    score=1, level=self.LEVEL3)
                            return row['pop result']
                del pop_new_data
                break
        self.write_to_db_result(name='{} POP'.format(scene_data['scene_fk'].values[0]), result='No POP',
                                score=0, level=self.LEVEL3)
        return

    def calculate_Adjacency(self, scene_data):
        for i, row in self.adj_data.iterrows():
            if scene_data['template_name'].values[0] in [str(g.replace('\n', '')) for g in
                                                         row['Template name'].split(',')]:
                if self.scif[(self.scif['scene_fk'] == scene_data['scene_fk'].values[0]) & (self.scif[
                        'manufacturer_name'] == CCNA)].empty:
                    continue
                Adjacency_data = self.adj_data.iloc[i]
                store_areas = [str(g.replace('\n', '')) for g in Adjacency_data['store area'].split(',')]
                store_areas_in_scene = list(
                    self.store_areas.loc[
                        self.store_areas['scene_fk'] == scene_data['scene_fk'].values[0]]['store_area_name'].values)
                if store_areas_in_scene:
                    if set(store_areas) & set(store_areas_in_scene):
                        if Adjacency_data['result'] is not None:
                            self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_fk'].values[0]),
                                                    result=Adjacency_data['result'],
                                                    score=1, level=self.LEVEL3)
                            return
                else:
                    if set(Adjacency_data['Template group'].split(',')) & set(
                            scene_data['template_group'].unique().tolist()):
                        if Adjacency_data['result'] is not None:
                            self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_fk'].values[0]),
                                                    result=Adjacency_data['result'],
                                                    score=1, level=self.LEVEL3)
                            return
                            # if set(Adjacency_data['Template group'].split(',')) & set(scene_data['template_group'].unique().tolist()) or \
                            #             set(store_areas) & set(store_areas_in_scene):
                            #     if Adjacency_data['result'] is not None:
                            #         self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_id'].values[0]),
                            #                                 result=Adjacency_data['result'],
                            #                                 score=1, level=self.LEVEL3)
                            #         return
            else:
                continue
        self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_fk'].values[0]), result='N/A',
                                score=0, level=self.LEVEL3)
        return
        # Adjacency_data = self.adj_data.loc[self.adj_data['Template name'] == scene_data['template_name'].values[0]]
        # if not Adjacency_data.empty:
        #     store_areas = [str(g.replace('\n', '')) for g in Adjacency_data['store area'].values[0].split(',')]
        #     store_areas_in_scene = list(
        #         self.store_areas.loc[
        #             self.store_areas['scene_fk'] == scene_data['scene_fk'].values[0]]['store_area_name'].values)
        #     if 'Programs' in scene_data['template_group'].unique().tolist() or \
        #                     set(store_areas) & set(store_areas_in_scene):
        #         if Adjacency_data['result'].values[0] is not None:
        #             self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_id'].values[0]),
        #                                     result=Adjacency_data['result'].values[0],
        #                                     score=1, level=self.LEVEL3)
        #             return
        #         else:
        #             self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_id'].values[0]), result='N/A',
        #                                     score=0, level=self.LEVEL3)
        # else:
        #     self.write_to_db_result(name='{} Adjacency'.format(scene_data['scene_id'].values[0]), result='N/A',
        #                             score=0, level=self.LEVEL3)
        # return

    def calculate_Pathway(self, pop_result, scene_data):
        result = 0
        store_list = self.store_types['store types'].tolist()
        for store_type in store_list:
            if self.store_type in store_type:
                if pop_result:
                    Pathway_data = self.pathway_data.loc[(self.pathway_data['pop result'] == pop_result) & (
                        self.pathway_data['store type'] == self.store_type)]
                    pathways = Pathway_data['result'].unique().tolist()
                    for pathway in pathways:
                        path_data = Pathway_data.loc[Pathway_data['result'] == pathway]
                        if ',' in path_data['store type']:
                            store_type_list = [str(g) for g in path_data['store type'].split(',')]
                        else:
                            store_type_list = [str(g) for g in path_data['store type']]
                        if self.store_type in store_type_list:
                            template_group = [str(g) for g in path_data['Template group'].values[0].split(',')]
                            store_areas = [str(g.replace('\n', '')) for g in
                                           path_data['store area'].values[0].split(',')]
                            store_areas_in_scene = list(
                                self.store_areas.loc[
                                    self.store_areas['scene_fk'] == scene_data['scene_fk'].values[0]][
                                    'store_area_name'].values)
                            if scene_data['template_group'].values[0] in template_group or \
                                            set(store_areas) & set(store_areas_in_scene):
                                if path_data['Template name'].values[0]:
                                    template_name = [str(g) for g in path_data['Template name'].values[0].split(',')]
                                    if template_name:
                                        if scene_data['template_name'].values[0] not in template_name:
                                            continue
                                        else:
                                            result = self.check_path_way(path_data, scene_data)
                                            if result == 1:
                                                return
                        del path_data
                    del Pathway_data

        if not result:
            self.write_to_db_result(name='{} Pathway'.format(scene_data['scene_fk'].values[0]), result='No Pathway',
                                    score=0, level=self.LEVEL3)
        return False

    def check_path_way(self, path_data, scene_data):
        filters = {'scene_fk': scene_data['scene_fk'].values[0]}
        filters2 = {}
        filters3 = {}
        filters4 = {}
        result1 = 0
        result2 = 0
        result3 = 0
        result4 = 0
        result = 0
        filters[path_data['param1'].values[0]] = [str(g) for g in path_data['value1'].values[0].split(",")]
        if path_data['param2'].values[0]:
            if path_data['param2'].values[0] == path_data['param1'].values[0]:
                filters2 = {path_data['param2'].values[0]: [str(g) for g in path_data['value2'].values[0].split(",")],
                            'scene_fk': scene_data['scene_fk'].values[0]}
            else:
                filters[path_data['param2'].values[0]] = [str(g) for g in path_data['value2'].values[0].split(",")]
        if path_data['param3'].values[0]:
            if path_data['param3'].values[0] == path_data['param2'].values[0]:
                filters3 = {path_data['param3'].values[0]: [str(g) for g in path_data['value3'].values[0].split(",")],
                            'scene_fk': scene_data['scene_fk'].values[0]}
            else:
                filters[path_data['param3'].values[0]] = [str(g) for g in path_data['value3'].values[0].split(",")]
        if path_data['param4'].values[0]:
            if path_data['param4'].values[0] == path_data['param3'].values[0]:
                filters4 = {path_data['param4'].values[0]: [str(g) for g in path_data['value4'].values[0].split(",")],
                            'scene_fk': scene_data['scene_fk'].values[0]}
            else:
                filters[path_data['param4'].values[0]] = [str(g) for g in path_data['value4'].values[0].split(",")]
        if path_data['Target'].values[0]:
            target = float(path_data['Target'].values[0])
        else:
            target = 1
        if path_data['calculation_type'].values[0] == 'availability':
            result1 = self.tools.calculate_availability(**filters)
            if result1 > 0:
                result1 = 1
            if filters2:
                result2 = self.tools.calculate_availability(**filters2)
                if result2 > 0:
                    result2 = 1
            if filters3:
                result3 = self.tools.calculate_availability(**filters3)
                if result3 > 0:
                    result3 = 1
            if filters4:
                result4 = self.tools.calculate_availability(**filters4)
                if result4 > 0:
                    result4 = 1
            if result1 + result2 + result3 + result4 >= target:
                result = 1
                self.write_to_db_result(name='{} Pathway'.format(scene_data['scene_fk'].values[0]),
                                        result=path_data['result'].values[0],
                                        score=1, level=self.LEVEL3)
                return result

        if path_data['calculation_type'].values[0] == 'assortment':
            result1 = self.tools.calculate_assortment()
            if result1 >= target:
                result = 1
                self.write_to_db_result(name='{} Pathway'.format(scene_data['scene_fk'].values[0]),
                                        result=path_data['result'].values[0],
                                        score=1, level=self.LEVEL3)
                return result
        return result

    def write_to_db_result(self, score, level, result=None, name=None):
        """
        This function creates the result Data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(score, level, result, name)
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

    def create_attributes_dict(self, score, level, result=None, name=None):
        """
        This function creates a Data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 28]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        format(score, '.2f'), 28)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL3:
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == 28]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, 242, result)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'kpi_fk', 'result'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self, kpi_set_fk=None):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        atomic_pks = tuple()
        if kpi_set_fk is not None:
            query = NEW_OBBOQueries.get_atomic_pk_to_delete(self.session_uid, kpi_set_fk)
            kpi_atomic_data = pd.read_sql_query(query, self.rds_conn.db)
            atomic_pks = tuple(kpi_atomic_data['pk'].tolist())
        cur = self.rds_conn.db.cursor()
        if atomic_pks:
            delete_queries = NEW_OBBOQueries.get_delete_session_results_query(self.session_uid, kpi_set_fk, atomic_pks)
            for query in delete_queries:
                cur.execute(query)
        for query in self.kpi_results_queries:
            cur.execute(query)
        self.rds_conn.db.commit()
