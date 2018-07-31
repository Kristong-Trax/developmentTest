import pandas as pd
from KPIUtils_v2.DB.CommonV2 import Common
from KPIUtils_v2.DB.Queries import Queries
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log

SESSION = 'session'
SCENE = 'scene'
SCENE_SESSION = 'scene_session'

class CCBZA_SANDSceneCommon(Common):

    def __init__(self, data_provider, scene_list):
        # super(CCBZA_SANDSceneCommon, self).__init__(data_provider)
        Common.__init__(self, data_provider)
        self.scene_list = scene_list

    def commit_results_data(self, result_entity=SESSION, scene_session_hierarchy=False):
        self.refresh_parents()
        insert_queries = self.merge_insert_queries(self.kpi_results[self.QUERY].tolist())
        delete_old_session_specific_tree_query = ''
        if result_entity == self.SCENE:
            delete_old_session_specific_tree_query = self.get_delete_specific_tree_queries(self.scene_list,
                                                                                                   self.HIERARCHY_SESSION_TABLE)
            delete_old_tree_query = self.get_delete_tree_scene_queries(self.scene_list,
                                                                               self.HIERARCHY_SCENE_TABLE)
            delete_query = self.get_delete_scene_results_query_from_new_tables(self.scene_list)
        elif result_entity == self.SESSION:
            delete_old_tree_query = self.queries.get_delete_tree_queries(self.session_id, self.HIERARCHY_SESSION_TABLE)
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

    def get_delete_specific_tree_queries(self, scenes, table):
        scenes_str = ', '.join([str(scene) for scene in scenes])
        return ("delete from %s where scene_kpi_results_fk in "
                "(select pk from report.scene_kpi_results where scene_fk in (%s));" % table, scenes_str)

    def get_delete_tree_scene_queries(self, scenes, table):
        scenes_str = ', '.join([str(scene) for scene in scenes])
        return ("delete from %s where scene_kpi_results_fk in "
                "(select pk from report.scene_kpi_results where scene_fk in (%s) and kpi_level_2_fk "
                "in (select pk from static.kpi_level_2 where kpi_calculation_stage_fk = '3' and scene_relevance = '1'))"
                ";" % table, scenes_str)

    def get_delete_scene_results_query_from_new_tables(self, scenes):
        scenes_str = ', '.join([str(scene) for scene in scenes])
        return ("delete from report.scene_kpi_results where scene_fk in (%s) and (kpi_level_2_fk "
                "in (select pk from static.kpi_level_2 where kpi_calculation_stage_fk = '3' and scene_relevance = '1'))"
                ";" % scenes_str)