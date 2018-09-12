from KPIUtils_v2.Utils.Decorators.Decorators import log_runtime
import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.Queries import Queries

__author__ = 'Ilan_Elyashiv'


class Common(object):

    KPI_SESSION_RESULTS_TABLE = 'report.kpi_level_2_results'
    KPI_SCENE_RESULTS_TABLE = 'report.scene_kpi_results'
    HIERARCHY_SESSION_TABLE = "report.kpi_hierarchy"
    HIERARCHY_SCENE_TABLE = "report.scene_kpi_hierarchy"
    SESSION_RESULT_FK = "session_result_fk"
    SCENE_RESULT_FK = "scene_result_fk"
    PARENT_FK = "session_parent_fk"
    IDENTIFIER_PARENT = "identifier_parent"
    IDENTIFIER_RESULT = "identifier_result"
    SHOULD_ENTER = "should_enter"
    QUERY = "query"
    SWITCH = 'switch'
    COLUMNS = [SESSION_RESULT_FK, PARENT_FK, SHOULD_ENTER, IDENTIFIER_PARENT, IDENTIFIER_RESULT, SCENE_RESULT_FK, QUERY,
               SWITCH]
    FICTIVE_FK = 0
    SESSION = 'session'
    SCENE = 'scene'
    SCENE_SESSION = 'scene_session'


    def __init__(self, data_provider, kpi=None):
        self.data_provider = data_provider
        self.kpi = kpi
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.data_provider.session_id
        self.scene_id = self.data_provider.scene_id
        self.store_id = self.data_provider[Data.STORE_FK]
        self.visit_date = self.data_provider[Data.VISIT_DATE] if self.data_provider[Data.STORE_FK] else None
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.kpi_static_data = self.get_kpi_static_data()
        self.queries = Queries
        self.kpi_results = pd.DataFrame(columns=self.COLUMNS)
        self.current_pk = 0

    def get_kpi_static_data(self):
        """
        This function extracts the static new KPI data (new tables) and saves it into one global data frame.
        The data is taken from static.kpi_level_2.
        """
        query = Queries.get_new_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    @staticmethod
    def get_dictionary(kpi_fk=None, brand_fk=None, sub_brand_fk=None, manufacturer_fk=None, brand_name=None,
                       sub_brand_name=None, product_fk=None, product_ean_code=None, name=None, template_fk=None,
                       **kwargs):
        output_dict = {}
        if kpi_fk:
            output_dict["kpi_fk"] = kpi_fk
        if brand_fk:
            output_dict["brand_fk"] = brand_fk
        if template_fk:
            output_dict["template_fk"] = template_fk
        if sub_brand_fk:
            output_dict["sub_brand_fk"] = sub_brand_fk
        if manufacturer_fk:
            output_dict["manufacturer_fk"] = manufacturer_fk
        if brand_name:
            output_dict["brand_name"] = brand_name
        if sub_brand_name:
            output_dict["sub_brand_name"] = sub_brand_name
        if product_fk:
            output_dict["product_fk"] = product_fk
        if product_ean_code:
            output_dict["product_ean_code"] = product_ean_code
        if name:
            output_dict["name"] = name
        for column in kwargs:
            output_dict[column] = kwargs[column]
        return output_dict

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            if not query:
                continue
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries

    def get_kpi_fk_by_kpi_name(self, kpi_name):
        """
        convert kpi name to kpi_fk
        :param kpi_name: string
        :return: fk
        """
        assert isinstance(kpi_name, (unicode, basestring)), "name is not a string: %r" % kpi_name
        try:
            return self.kpi_static_data[self.kpi_static_data['client_name'] == kpi_name]['pk'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_name))
            return None

    def get_kpi_fk_by_kpi_type(self, kpi_type):
        """
        convert kpi name to kpi_fk
        :param kpi_type: string
        :return: fk
        """
        assert isinstance(kpi_type, (unicode, basestring)), "name is not a string: %r" % kpi_type
        try:
            return self.kpi_static_data[self.kpi_static_data['type'] == kpi_type]['pk'].values[0]
        except IndexError:
            Log.info("Kpi name: {} is not equal to any kpi name in static table".format(kpi_type))
            return None

    def create_attributes_dict(self, by_scene, kpi_fk, numerator_id, numerator_result, denominator_id,
                               denominator_result, result, score, score_after_actions,
                               denominator_result_after_actions, numerator_result_after_actions,
                               weight, kpi_level_2_target_fk, context_id, parent_fk, target):
        """
        This function creates a data frame with all attributes needed for saving in KPI results new tables.
        """
        if by_scene:
            attributes = pd.DataFrame([(
                kpi_fk, self.scene_id, numerator_id, numerator_result, denominator_id, denominator_result, result,
                score, weight, context_id, target)], columns=[
                'kpi_level_2_fk', 'scene_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                'denominator_result', 'result', 'score', 'weight', 'context_id', 'target'])
        else:
            attributes = pd.DataFrame([(
                kpi_fk, self.session_id, numerator_id, numerator_result, denominator_id, denominator_result, result, score,
                score_after_actions, denominator_result_after_actions, numerator_result_after_actions, weight,
                kpi_level_2_target_fk, context_id, parent_fk, target)], columns=[
                'kpi_level_2_fk', 'session_fk', 'numerator_id', 'numerator_result', 'denominator_id',
                'denominator_result', 'result', 'score', 'score_after_actions', 'denominator_result_after_actions',
                'numerator_result_after_actions', 'weight', 'kpi_level_2_target_fk', 'context_id', 'parent_fk', 'target'])
        return attributes.to_dict()

    def write_to_db_result(self, fk, numerator_id=0, numerator_result=0, result=0,
                           denominator_id=0, denominator_result=0, score=0, score_after_actions=0,
                           denominator_result_after_actions=None, numerator_result_after_actions=0,
                           weight=None, kpi_level_2_target_fk=None, context_id=None, parent_fk=None, target=None,
                           identifier_parent=None, identifier_result=None, should_enter=False, by_scene=False,
                           scene_result_fk=None, switch=None):
        """
            This function creates the result data frame of new tables KPI,
            and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        table = self.KPI_SESSION_RESULTS_TABLE
        if by_scene:
            table = self.KPI_SCENE_RESULTS_TABLE
        attributes = self.create_attributes_dict(by_scene, kpi_fk=fk, numerator_id=numerator_id,
                                                 numerator_result=numerator_result, denominator_id=denominator_id,
                                                 denominator_result=denominator_result, result=result, score=score,
                                                 score_after_actions=score_after_actions,
                                                 denominator_result_after_actions=denominator_result_after_actions,
                                                 weight=weight, kpi_level_2_target_fk=kpi_level_2_target_fk,
                                                 context_id=context_id, parent_fk=parent_fk, target=target,
                                                 numerator_result_after_actions=numerator_result_after_actions)
        query = insert(attributes, table)
        if fk == self.FICTIVE_FK:
            query = ''
        new_result = {
            self.SESSION_RESULT_FK: self.current_pk, self.SHOULD_ENTER: should_enter, self.IDENTIFIER_PARENT: identifier_parent,
            self.SCENE_RESULT_FK: scene_result_fk, self.QUERY: query, self.IDENTIFIER_RESULT: identifier_result, self.SWITCH: switch}
        self.current_pk += 1
        self.kpi_results = self.kpi_results.append(new_result, ignore_index=True)

    def refresh_parents(self):
        for i, line in self.kpi_results[~self.kpi_results[self.IDENTIFIER_RESULT].isnull()].iterrows():
            cur_fk, cur_dict = line[self.SESSION_RESULT_FK], line[self.IDENTIFIER_RESULT]
            self.kpi_results.loc[self.kpi_results[self.IDENTIFIER_PARENT] == cur_dict, self.PARENT_FK] = cur_fk

    def refresh_pks(self, last_pk):
        self.kpi_results[self.SESSION_RESULT_FK] += last_pk
        self.kpi_results[self.PARENT_FK] += last_pk

    def get_insert_queries_hierarchy(self, result_entity, scene_session_hierarchy):
        relevant_df = self.kpi_results[self.kpi_results[self.SHOULD_ENTER] == True]
        queries = []
        table = self.HIERARCHY_SESSION_TABLE
        if result_entity == self.SCENE:
            table = self.HIERARCHY_SCENE_TABLE
        for i, line in relevant_df.iterrows():
            result_fk = line[self.SESSION_RESULT_FK]
            parent_result_fk = line[self.PARENT_FK]
            scene_kpi_results_fk = line[self.SCENE_RESULT_FK]
            switch = line[self.SWITCH]
            if result_entity == self.SCENE:
                attributes = pd.DataFrame([(result_fk, parent_result_fk)],
                                          columns=['scene_kpi_results_fk', 'scene_kpi_results_parent_fk']).to_dict()
            elif scene_session_hierarchy:
                attributes = pd.DataFrame([(parent_result_fk, scene_kpi_results_fk)],
                                      columns=['session_kpi_results_parent_fk',
                                               'scene_kpi_results_fk']).to_dict()
            elif result_entity == self.SESSION:
                if not switch:
                    attributes = pd.DataFrame([(result_fk, parent_result_fk, scene_kpi_results_fk)],
                                          columns=['session_kpi_results_fk', 'session_kpi_results_parent_fk',
                                                   'scene_kpi_results_fk']).to_dict()
                else:
                    attributes = pd.DataFrame([(parent_result_fk, scene_kpi_results_fk, None)],
                                              columns=['session_kpi_results_parent_fk',
                                                       'scene_kpi_results_fk', 'session_kpi_results_fk']).to_dict()
            else:
                Log.error('Cannot Calculate results per {}'.format(result_entity))
                return
            query = insert(attributes, table)
            queries.append(query)
        return queries

    @log_runtime('Saving to DB')
    def commit_results_data(self, result_entity=SESSION, scene_session_hierarchy=False):
    # def commit_results_data(self, by_scene=False, scene_session_hierarchy=False):
        """
        We need to "save place" (transaction) for all the queries, enter the first pk to refresh_pks and
        then create queries function, and commit all those queries (in the tree, only the necessary ones)
        """
        self.refresh_parents()
        insert_queries = self.merge_insert_queries(self.kpi_results[self.QUERY].tolist())
        delete_old_session_specific_tree_query = ''
        if result_entity == self.SCENE:
            delete_old_session_specific_tree_query = self.queries.get_delete_specific_tree_queries(self.scene_id, self.HIERARCHY_SESSION_TABLE)
            delete_old_tree_query = self.queries.get_delete_tree_scene_queries(self.scene_id, self.HIERARCHY_SCENE_TABLE)
            delete_query = self.queries.get_delete_scene_results_query_from_new_tables(self.scene_id)
        elif result_entity == self.SESSION:
            delete_old_tree_query = self.queries.get_delete_tree_queries(self.session_id,  self.HIERARCHY_SESSION_TABLE)
            delete_query = self.queries.get_delete_session_results_query_from_new_tables(self.session_id)
        else:
            Log.error('Cannot Calculate results per {}'.format(result_entity))
            return
        local_con = ProjectConnector(self.project_name, DbUsers.CalculationEng)
        cur = local_con.db.cursor()
        if delete_old_session_specific_tree_query:
            cur.execute(delete_old_session_specific_tree_query)
        cur.execute(delete_old_tree_query)
        cur.execute(delete_query)
        Log.info('Start committing results')
        cur.execute(insert_queries[0] + ";")
        cur.execute(self.queries.get_last_id())
        last_id = cur.fetchmany()
        self.refresh_pks(int(last_id[0][0]))
        insert_tree_queries = self.get_insert_queries_hierarchy(result_entity, scene_session_hierarchy)
        if insert_tree_queries:
            insert_tree_queries = self.merge_insert_queries(insert_tree_queries)[0] + ";"
            cur.execute(insert_tree_queries)
        local_con.db.commit()
