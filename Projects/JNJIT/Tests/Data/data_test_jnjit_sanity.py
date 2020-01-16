
from Trax.Algo.Calculations.Core.Constants import Keys
from Trax.DB.Mongo.Connector import MongoConnector
from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH, DATABASE_NAME, COLLECTION_NAME, \
    DATA
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME
from bson import ObjectId

__author__ = 'ilays'


class ProjectsSanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    jnjit_seed = {DATA_TYPE: BaseSeedData.MYSQL,
                  FILES_RELATIVE_PATH: ['Data/Seeds/jnjit_seed.sql.gz'],
                  PROJECT_NAME: project_name
                  }
    jnjit_seed_product_prices_2 = {DATA_TYPE: BaseSeedData.MYSQL,
                                   FILES_RELATIVE_PATH: ['Data/Seeds/missing_sql_data.sql'],
                                   PROJECT_NAME: project_name
                  }
    mongodb_products_and_brands_seed = {DATA_TYPE: BaseSeedData.MONGO,
                                        DATABASE_NAME: MongoConnector.SMART,
                                        COLLECTION_NAME: 'projects_project',
                                        DATA: [{"_id": ObjectId("5824dd6bc9b9128805964eab"),
                                                "project_name": TestProjectsNames().TEST_PROJECT_1,
                                                Keys.PRODUCTS_AND_BRANDS: True}]}