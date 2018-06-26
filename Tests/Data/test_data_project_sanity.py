
from Trax.Apps.Core.QueueServer.Handlers import InternalHandler
from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME
from Trax.Utils.Conventions.Events import AwsRegions

__author__ = 'pavel'

TERMINATE_MESSAGE = {
    'type': InternalHandler.MessageType.TERMINATE
}

INPUT_QUEUE_REGION = AwsRegions.NORTH_VIRGINIA
TEST_UNIFIED_INPUT_QUEUE_NAME = 'K-ENGINE_UNIFIED_LOW_PRIORITY'


class ProjectsSanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    project_name2 = TestProjectsNames().TEST_PROJECT_2

    ccru_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/ccru_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    ccza_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/ccza_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    ccus_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/ccus_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    marsru_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/marsru_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    diageoza_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/diageoza_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    diageous_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/diageous_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    diageogtr_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/diageogtr_seed.sql'],
        PROJECT_NAME: project_name
    }

    diageouk_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/diageouk_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    diageotw_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/diageotw_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    pngamerica_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/pngamerica_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    ccbottlersus_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/ccbottlersus_seed.sql.gz'],
        PROJECT_NAME: project_name
    }

    inbevtradmx_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/inbevtradmx_seed.sql.gz'],
        PROJECT_NAME: project_name
    }


