from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME


class DataTestMarsuk(BaseSeedData):

    sql_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/marsuk_seed.sql.gz'],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }

    shelves_not_correct = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/shelves_not_correct_shelf.sql.gz'],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }

    mixed_shelves_bottom = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/mixed_shelves_bottom.sql.gz'],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }

    right_shelves_bottom = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/right_shelves_bottom.sql.gz'],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }

    product_not_distributed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: ['Data/product_not_distributed.sql.gz'],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }
