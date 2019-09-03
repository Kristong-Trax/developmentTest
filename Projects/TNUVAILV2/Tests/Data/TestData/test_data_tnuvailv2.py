from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH, DATABASE_NAME, COLLECTION_NAME, \
    DATA
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME
from Trax.Algo.Calculations.Core.Constants import Keys
from Trax.DB.Mongo.Connector import MongoConnector
from bson import ObjectId

__author__ = 'Idanr'


class TnuvailV2SanityData(BaseSeedData):
    project_name = TestProjectsNames().TEST_PROJECT_1
    tnuvailv2_sand_seed = {DATA_TYPE: BaseSeedData.MYSQL,
                           FILES_RELATIVE_PATH: ['Data/Seeds/tnuvailv2_seed.sql.gz'],
                           PROJECT_NAME: project_name
                           }
    mongodb_products_and_brands_seed = \
        {DATA_TYPE: BaseSeedData.MONGO,
         DATABASE_NAME: MongoConnector.SMART,
         COLLECTION_NAME: 'projects_project',
         DATA: [{
             "_id": ObjectId("5824dd6bc9b9128805964eab"),
             "project_name": TestProjectsNames().TEST_PROJECT_1,
             Keys.PRODUCTS_AND_BRANDS: True
         }]}
