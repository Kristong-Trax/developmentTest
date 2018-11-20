import os

from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Testing.SeedNew import BaseSeedData, DATA_TYPE,  FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Conventions.Events import AwsRegions
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME

__author__ = 'yonatana'

INPUT_QUEUE_NAME = 'test_input_queue'
SESSION_CALCULATED_QUEUE_NAME = "TEST_SMARTCOOLER_SESSION_CALCULATED"
SCENE_ANALYZED_QUEUE_NAME = "test_scene_analyzed_queue"
QUEUE_REGION = AwsRegions.NORTH_VIRGINIA
SCENE_ID = 206042


class SodTestSeedData(BaseSeedData):

    schema = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['../../../../Algo/Calculations/Core/Tests/Data/schema.sql'],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }

    sql_sod = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: [os.path.join(os.path.dirname(os.path.realpath(__file__)), 'dumpNew.sql.gz')],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1}


class InsertDataIntoMySqlProjectSOD(object):
    def __init__(self, project):
        self._conn = PSProjectConnector(project, DbUsers.Garage)
        self._cur = self._conn.db.cursor()

    def update_all_scenes_to_same_session(self):
        query = """update probedata.scene
                    set session_uid = '2b14c6c6-1458-4c3c-96a2-1ae20824f054';"""
        self._cur.execute(query)
        self._conn.db.commit()

