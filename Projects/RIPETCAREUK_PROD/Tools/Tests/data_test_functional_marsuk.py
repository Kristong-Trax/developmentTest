from Trax.Data.Testing.Resources import BaseSeedData, DATA_TYPE, FILES_RELATIVE_PATH
from Trax.Data.Testing.TestProjects import TestProjectsNames
from Trax.Utils.Environments.DockerGlobal import PROJECT_NAME
import pandas as pd

class DataTestStaticUpdate(BaseSeedData):

    sql_seed = {
        DATA_TYPE: BaseSeedData.MYSQL,
        FILES_RELATIVE_PATH: [],
        PROJECT_NAME: TestProjectsNames().TEST_PROJECT_1
    }

    hierarchy_one = {
        'level_1': pd.DataFrame({
            'set_name': ['PERFECT STORE', 'ASSORTMENT SCORE']
        }),
        'level_2': pd.DataFrame(
            columns=['set_name', 'kpi_name'],
            data={
                'set_name': ['PERFECT STORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE'],
                'kpi_name': [u'ASSORTMENT SCORE', u'Sheba', u'Whi Pouch']
        }),
        'level_3': pd.DataFrame(
            columns=['set_name', 'kpi_name', 'atomic_name'],
            data={
                'atomic_name': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
                                u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
                                u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk',
                                u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk'],
                'kpi_name': [u'Sheba', u'Sheba', u'Whi Pouch', u'Whi Pouch'],
                'set_name': [u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE']})
    }

    hierarchy_two = {
        'level_1': pd.DataFrame({
            'set_name': ['PERFECT STORE', 'ASSORTMENT SCORE_2']
        }),
        'level_2': pd.DataFrame(
            columns=['set_name', 'kpi_name'],
            data={
                'set_name': ['PERFECT STORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE'],
                'kpi_name': [u'ASSORTMENT SCORE', u'Sheba', u'Whi Pouch_2']
            }),
        'level_3': pd.DataFrame(
            columns=['set_name', 'kpi_name', 'atomic_name'],
            data={
                'atomic_name': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6_2',
                                u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
                                u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk',
                                u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk'],
                'kpi_name': [u'Sheba', u'Sheba', u'Whi Pouch', u'Whi Pouch'],
                'set_name': [u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE']})
    }

    hierarchy_three = {
        'level_1': pd.DataFrame({
            'set_name': ['PERFECT STORE', 'ASSORTMENT SCORE_2']
        }),
        'level_2': pd.DataFrame(
            columns=['set_name', 'kpi_name'],
            data={
                'set_name': ['PERFECT STORE', u'ASSORTMENT SCORE_2', u'ASSORTMENT SCORE'],
                'kpi_name': [u'ASSORTMENT SCORE', u'Sheba', u'Whi Pouch_2']
        }),
        'level_3': pd.DataFrame(
            columns=['set_name', 'kpi_name', 'atomic_name'],
            data={
                'atomic_name': [u'SHEBA FRESH CHOICE FISH SELECTION IN GRAVY 50G X6,',
                                u'SHEBA POUCH FINE FLAKES POULTRY IN JELLY 85GX12,',
                                u'WHISKAS 1+ Cat Pouches Poultry Selection in Jelly 12x100g pk',
                                u'WHISKAS 1+ Cat Pouches Fish Selection in Jelly 12x100g pk'],
                'kpi_name': [u'Sheba', u'Sheba', u'Whi Pouch_2', u'Whi Pouch'],
                'set_name': [u'ASSORTMENT SCORE_2', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE', u'ASSORTMENT SCORE']})
    }
