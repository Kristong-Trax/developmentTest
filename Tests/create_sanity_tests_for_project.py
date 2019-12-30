from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
import sys
import os
import pandas as pd
import shutil
import autopep8

__author__ = 'yoava and ilays'

"""
this module creates dump file and sanity test classes for a specific project.
all you have to do is to insert the project name and run it 
"""


class SeedCreator:
    TOP_SESSIONS_AND_SCENES = {}
    """
    this class creates seed file
    """

    def __init__(self, project):
        self.project = project
        self.rds_conn = PSProjectConnector(project, DbUsers.CalculationEng)
        self.user = os.environ.get('USER')
        self.exporter_outputs_dir = os.path.join('/home', self.user, 'exporter')
        if not os.path.exists(self.exporter_outputs_dir):
            os.makedirs(self.exporter_outputs_dir)
        self.output_dir = os.path.join(self.exporter_outputs_dir, project)
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.rds_name = self.rds_conn.project_params['rds_name']
        self.seed_name = '{}_seed.sql.gz'.format(project.replace('-', '_'))
        self.export_dir = os.path.join('/home', self.user, 'dev', 'traxdatabase', 'traxExport')

    def _set_top_sessions_from_db(self, number_of_sessions):
        """
        this method gets top sessions from the project db to test
        :return: list of sessions
        """
        Log.info('Fetching {} sessions'.format(number_of_sessions))
        query = """
        SELECT s.session_uid, s.pk FROM probedata.session s
        join reporting.scene_item_facts r on r.session_id = s.pk
        WHERE
            status = 'Completed' 
            and r.session_id is not null  
            ORDER BY s.visit_date , s.number_of_scenes DESC
            LIMIT {};
        """.format(number_of_sessions)
        sessions_df = pd.read_sql(query, self.rds_conn.db)
        for session in sessions_df.session_uid.values:
            self.TOP_SESSIONS_AND_SCENES[session] = []

    def build_export_command(self):
        """
        this method return an adjusted export query created from the given sessions
        :return: The custom query mentioned
        """
        export_command = """./traxExportIntuition.sh {0} {1} session_uid """.format(self.rds_name, self.output_dir)
        for session_uid in self.TOP_SESSIONS_AND_SCENES.keys():
            export_command = export_command.__add__(session_uid)
            if session_uid != self.TOP_SESSIONS_AND_SCENES.keys()[len(self.TOP_SESSIONS_AND_SCENES.keys()) - 1]:
                export_command += ','
        return export_command

    def activate_exporter(self, number_of_sessions=1, specific_sessions_and_scenes=None):
        """
        this method build a dump file with traxExporter from the given sessions
        :param number_of_sessions: number of sessions in cae we want to fetch them from db
        :param specific_sessions_and_scenes: list of sessions and scenes if we already know the sessions to test
        :return: None
        """
        os.chdir(self.export_dir)
        if specific_sessions_and_scenes is not None:
            self.TOP_SESSIONS_AND_SCENES = specific_sessions_and_scenes
        else:
            self._set_top_sessions_from_db(number_of_sessions)
        Log.info('Activating exporter')
        export_command = self.build_export_command()
        Log.info(export_command)
        os.system(export_command)
        os.chdir(self.output_dir)
        os.rename('dump.sql.gz', self.seed_name)
        shutil.copy2(os.path.join(self.output_dir, self.seed_name),
                     os.path.join('/home', self.user, 'dev', 'kpi_factory', 'Tests', 'Data', 'Seeds', self.seed_name))
        Log.info('Done')


class SanityTestsCreator:
    """
    this class creates the sanity tests class
    """
    TEST_CLASS = """
import os
import MySQLdb
import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import Seeder
from Trax.Algo.Calculations.Core.DataProvider import KEngineDataProvider, Output
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Tests.Data.TestData.test_data_%(project)s_sanity import ProjectsSanityData
from Projects.%(project_capital)s.Calculations import %(main_class_name)s
%(scene_import)s
from Trax.Apps.Core.Testing.BaseCase import TestFunctionalCase
from Tests.TestUtils import remove_cache_and_storage


__author__ = '%(author)s'


class TestKEngineOutOfTheBox(TestFunctionalCase):

    def set_up(self):
        super(TestKEngineOutOfTheBox, self).set_up()
        remove_cache_and_storage()

    @property
    def import_path(self):
        return 'Trax.Apps.Services.KEngine.Handlers.SessionHandler'
    
    @property
    def config_file_path(self):
        return os.path.join(os.path.dirname(os.path.realpath(__file__)), 'k-engine-test.config')
    
    seeder = Seeder()
    
    def _assert_old_tables_kpi_results_filled(self, distinct_kpis_num=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT * FROM report.kpi_results
           ''')
        kpi_results = cursor.fetchall()
        if distinct_kpis_num:
            df = pd.DataFrame(kpi_results)
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()

    def _assert_new_tables_kpi_results_filled(self, distinct_kpis_num=None, list_of_kpi_names=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT kl2.pk, kl2.client_name, kl2r.kpi_level_2_fk, kl2r.result 
           FROM report.kpi_level_2_results kl2r left join static.kpi_level_2 kl2 
           on kpi_level_2_fk = kl2.pk
           ''')
        kpi_results = cursor.fetchall()
        df = pd.DataFrame(kpi_results)
        if distinct_kpis_num:
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        if list_of_kpi_names:
            exisitng_results = df['client_name'].unique()
            result = all(elem in exisitng_results for elem in list_of_kpi_names)
            self.assertTrue(result)
        connector.disconnect_rds()
    
    def _assert_test_results_matches_reality(self):
        real_res_dict = pd.DataFrame(%(kpi_results)s)

        real_results = pd.DataFrame(real_res_dict)

        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
                        SELECT 
                        distinct kpi.client_name,res.kpi_level_2_fk, numerator_id, denominator_id, context_id, result
                        FROM
                            report.kpi_level_2_results res
                                LEFT JOIN
                            static.kpi_level_2 kpi ON kpi.pk = res.kpi_level_2_fk
                                LEFT JOIN
                            probedata.session ses ON ses.pk = res.session_fk
           ''')
        kpi_results = cursor.fetchall()
        kpi_results = pd.DataFrame(kpi_results)
        merged_results = pd.merge(real_results, kpi_results, on=['kpi_level_2_fk', 'numerator_id', 'denominator_id',
                                                                 'context_id'], how="left")
        wrong_results = merged_results[merged_results['result_x'] != merged_results['result_y']]
        if not wrong_results.empty:
            print "The following KPIs had wrong results:"
            for i, res in wrong_results.iterrows():
                print "kpi_level_2_fk: {0}, client_name: {1}, numerator_id: {2}, denominator_id: {3}, " \
                      "context_id: {4}".format(str(res['kpi_level_2_fk']), str(res['client_name_x']),
                                               str(res['numerator_id']), str(res['denominator_id']),
                                               str(res['context_id']))
        self.assertTrue(wrong_results.empty)

    def _assert_scene_tables_kpi_results_filled(self, distinct_kpis_num=None):
        connector = PSProjectConnector(TestProjectsNames().TEST_PROJECT_1, DbUsers.Docker)
        cursor = connector.db.cursor(MySQLdb.cursors.DictCursor)
        cursor.execute('''
           SELECT * FROM report.scene_kpi_results
           ''')
        kpi_results = cursor.fetchall()
        if distinct_kpis_num:
            df = pd.DataFrame(kpi_results)
            self.assertEquals(df['kpi_level_2_fk'].unique().__len__(), distinct_kpis_num)
        else:
            self.assertNotEquals(len(kpi_results), 0)
        connector.disconnect_rds()
        
    @seeder.seed(["%(seed)s"%(need_pnb)s], ProjectsSanityData())
    def test_%(project)s_sanity(self):
        project_name = ProjectsSanityData.project_name
        data_provider = KEngineDataProvider(project_name)
        sessions = %(sessions)s
        for session in sessions.keys():
            data_provider.load_session_data(str(session))
            output = Output()
            %(main_class_name)s(data_provider, output).run_project_calculations()
            # self._assert_old_tables_kpi_results_filled(distinct_kpis_num=None)
            # self._assert_new_tables_kpi_results_filled(distinct_kpis_num=None, list_of_kpi_names=None)
            # self._assert_test_results_matches_reality()
            # for scene in sessions[session]:
            #     data_provider.load_scene_data(str(session), scene_id=scene)
            #     SceneCalculations(data_provider).calculate_kpis()
            #     self._assert_scene_tables_kpi_results_filled(distinct_kpis_num=None)
"""

    def __init__(self, project, sessions_scenes_list, need_pnb=True, kpi_results=None):
        self.project = project.lower().replace('-', '_')
        self.project_capital = self.project.upper().replace('-', '_')
        self.user = os.environ.get('USER')
        self.project_short = self.project_capital.split('_')[0]
        self.main_class_name = '{}Calculations'.format(self.project_capital)
        self.sessions_scenes_list = sessions_scenes_list
        self.need_pnb = ', "mongodb_products_and_brands_seed"' if need_pnb else ""
        self.kpi_results = str(kpi_results) if kpi_results else ""

    def create_test_class(self):
        """
        this method create sanity test class
        :return: None
        """
        formatting_dict = {'author': self.user,
                           'main_class_name': self.main_class_name,
                           'project_capital': self.project_capital,
                           'seed': '{}_seed'.format(self.project.replace('-', '_')),
                           'project': self.project,
                           'sessions': str(self.sessions_scenes_list),
                           'scene_import': self._import_scene_calculation(),
                           'need_pnb': self.need_pnb,
                           'kpi_results': self.kpi_results
                           }
        project_name = self.project.upper().replace("-", "_")
        data_class_directory_path = '/home/{0}/dev/kpi_factory/Projects/{1}/Tests'.format(self.user, project_name)
        file_name = 'test_functional_{0}_sanity.py'.format(self.project)
        if not os.path.exists(data_class_directory_path):
            os.makedirs(data_class_directory_path)
            with open(os.path.join(data_class_directory_path, '__init__.py'), 'wb') as f:
                f.write("")
        with open(os.path.join(data_class_directory_path, file_name), 'wb') as f:
            f.write(autopep8.fix_code(source=(SanityTestsCreator.TEST_CLASS % formatting_dict),
                                      options={"max_line_length": 100}))

    def _import_scene_calculation(self):
        total_values = [j for i in self.sessions_scenes_list.values() for j in i]
        if len(total_values) > 0:
            return "from Projects.{}.SceneKpis.SceneCalculations import SceneCalculations".format(self.project_capital)
        else:
            return ""


class CreateTestDataProjectSanity:
    """
    this classs creates project sanity data class
    """

    def __init__(self, project):
        self.project = project
        self.user = os.environ.get('USER')

    def create_data_class(self):
        """
        this method creates the data class
        :return:  None
        """
        seed_data = """DATA_TYPE: BaseSeedData.MYSQL,
                       FILES_RELATIVE_PATH: ['Data/Seeds/{}_seed.sql.gz'],
                       PROJECT_NAME: project_name
                """.format(self.project.replace('-', '_'))
        seed_data = '{' + seed_data
        seed_data = seed_data + '       }'

        mongo_data = """DATA: [{"_id": ObjectId("5824dd6bc9b9128805964eab"),
                                                "project_name": TestProjectsNames().TEST_PROJECT_1, 
                                                Keys.PRODUCTS_AND_BRANDS: True}]}"""

        data_class_content = """
from Trax.Algo.Calculations.Core.Constants import Keys
from Trax.DB.Mongo.Connector import MongoConnector
from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH, DATABASE_NAME, COLLECTION_NAME, \\
   DATA
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME
from bson import ObjectId

__author__ = '{0}'


class ProjectsSanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    {1}_seed = {2} 
    mongodb_products_and_brands_seed = {3}DATA_TYPE: BaseSeedData.MONGO,
                                        DATABASE_NAME: MongoConnector.SMART,
                                        COLLECTION_NAME: 'projects_project',
                                        {4}
""".format(self.user, self.project.replace('-', '_'), seed_data, "{", mongo_data)
        project_name = self.project.upper().replace("-", "_")
        data_class_directory_path = '/home/{0}/dev/kpi_factory/Projects/{1}/Tests/Data'.format(self.user, project_name)
        file_name = 'test_data_{0}_sanity.py'.format(self.project)
        if not os.path.exists(data_class_directory_path):
            os.makedirs(data_class_directory_path)
            with open(os.path.join(data_class_directory_path.replace('Data', '__init__.py')), 'wb') as f:
                f.write("")
            with open(os.path.join(data_class_directory_path, '__init__.py'), 'wb') as f:
                f.write("")
        with open(os.path.join(data_class_directory_path, file_name), 'wb') as f:
            f.write(data_class_content)


class GetKpisDataForTesting:
    def __init__(self, project):
        self.project = project
        self.rds_conn = PSProjectConnector(project, DbUsers.CalculationEng)
        self.session = ""

    def get_session_with_max_kpis(self, days_back=7):
        Log.info('Fetching recent session with max number of kpis')
        number_of_sessions = 1
        query = """
                SELECT 
                    res.session_fk,
                    ses.session_uid,
                    COUNT(DISTINCT client_name) AS kpi_count
                FROM
                    report.kpi_level_2_results res
                        LEFT JOIN
                    static.kpi_level_2 kpi ON kpi.pk = res.kpi_level_2_fk
                        LEFT JOIN
                    probedata.session ses ON ses.pk = res.session_fk
                WHERE
                    kpi.initiated_by <> 'OutOfTheBox'
                    and ses.visit_date between date_add(now(), interval -{0} day) and now()
                    and ses.status = 'Completed'
                GROUP BY res.session_fk , session_uid
                
                ORDER BY kpi_count desc limit {1};
               """.format(days_back, number_of_sessions)
        sessions_df = pd.read_sql(query, self.rds_conn.db)
        if sessions_df.empty:
            Log.warning("No sessions were found (with Non-OOTB KPIs) in the last {0} days.".format(days_back))
            return
        sessions_chosen = list(sessions_df['session_uid'])
        sessions_chosen_dict = dict.fromkeys(sessions_chosen, [])
        Log.info("The chosen sessions are: {}".format(list(sessions_chosen)))
        return sessions_chosen_dict

    def get_one_result_per_kpi(self, sessions):
        Log.info('Fetching kpis results to check')
        if len(sessions.keys()) > 1:
            sessions_for_query = "in " + str(tuple(sessions.keys()))
        else:
            sessions_for_query = "= '" + str(sessions.keys()[0]) + "'"
        query = """
                SELECT 
                    distinct kpi.client_name,res.kpi_level_2_fk, numerator_id, denominator_id, context_id, result
                FROM
                    report.kpi_level_2_results res
                        LEFT JOIN
                    static.kpi_level_2 kpi ON kpi.pk = res.kpi_level_2_fk
                        LEFT JOIN
                    probedata.session ses ON ses.pk = res.session_fk
                WHERE ses.session_uid {}
                GROUP BY 1;
                       """.format(sessions_for_query)
        kpi_results_df = pd.read_sql(query, self.rds_conn.db)
        if kpi_results_df.empty:
            Log.error("No results were found for sessions: {}, "
                      "try to recalculate the sessions".format(str(sessions.keys())))
            return
        return kpi_results_df


def get_sessions_in_correct_format(sessions_param):
    if type(sessions_param) == dict:
        return sessions_param
    elif type(sessions_param) == str:
        return {sessions_param: []}
    elif type(sessions_param) == list:
        return dict.fromkeys(sessions_param, [])
    else:
        return None


def create_seed(project, sessions_from_user=None):
    kpisData = GetKpisDataForTesting(project=project)
    if not sessions_from_user or len(sessions_from_user) == 0:
        sessions_to_use = kpisData.get_session_with_max_kpis()
    else:
        sessions_to_use = get_sessions_in_correct_format(sessions_from_user)
    kpi_results = kpisData.get_one_result_per_kpi(sessions=sessions_to_use)
    if not kpi_results:
        return None, None
    creator = SeedCreator(project=project)
    creator.activate_exporter(specific_sessions_and_scenes=sessions_to_use)
    creator.rds_conn.disconnect_rds()
    return sessions_to_use, kpi_results


def create_sanity_test(project, sessions_to_use, kpi_results):
    if len(kpi_results) == 0:
        kpi_results = GetKpisDataForTesting(project=project).get_one_result_per_kpi(sessions=sessions_to_use)
    if kpi_results.empty:
        return
    kpi_results_as_str = str(kpi_results.to_dict()).replace('nan', 'None')

    # products_and_brands is needed for some projects, if you don't need it, put False in the script,
    # the tests will run much faster without it
    data_class = CreateTestDataProjectSanity(project=project)
    data_class.create_data_class()

    # Create functional-sanity test
    sanity = SanityTestsCreator(project=project, sessions_scenes_list=sessions_to_use, need_pnb=True,
                                kpi_results=kpi_results_as_str if kpi_results_as_str else None)
    sanity.create_test_class()


if __name__ == '__main__':
    """
    This script was made to create a sanity test per project.
    """
    LoggerInitializer.init('running sanity creator script')
    project = 'diageoug'
    kpi_results = pd.DataFrame()
    # Insert a session_uid / list of session_uids / dict of session_uid and scenes in the following format {'a': [1, 3]}
    sessions = ['f9d6b8a5-7964-4ef5-afe4-8580df97f57c']
    # In case you don't need to generate a new seed, just comment out the below row
    # sessions, kpi_results = create_seed(project=project, sessions_from_user=sessions)
    if kpi_results is None:
        sys.exit(1)
    sessions = get_sessions_in_correct_format(sessions)
    create_sanity_test(project=project, sessions_to_use=sessions, kpi_results=kpi_results)
