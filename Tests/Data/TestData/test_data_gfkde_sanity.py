
from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME

__author__ = 'elis'


class ProjectsSanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    gfkde_seed = {DATA_TYPE: BaseSeedData.MYSQL,
                        FILES_RELATIVE_PATH: ['Data/Seeds/gfkde_seed.sql.gz'],
                        PROJECT_NAME: project_name
                        } 
