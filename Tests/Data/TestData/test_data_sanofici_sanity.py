
from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME

__author__ = 'avrahama'


class ProjectsSanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    sanofici_seed = {DATA_TYPE: BaseSeedData.MYSQL,
                        FILES_RELATIVE_PATH: ['Data/Seeds/sanofici_seed.sql.gz'],
                        PROJECT_NAME: project_name
                        } 
