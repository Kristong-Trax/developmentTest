import pandas as pd

from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Israel'


class AddKPIs():
    def __init__(self, project):
        self.project = project
        self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)

    def add_kpi_level_2(self):
        kpi_family_query = """
                        INSERT INTO `static`.`kpi_family` 
                            (`pk`, `name`, `kpi_calculation_stage_fk`) 
                        VALUES 
                            ('19', 'assortment', '3');
                            """

        calculation_stage_query = """
                                INSERT INTO static.kpi_calculation_stage
                                    (pk, name)
                                VALUES
                                    ('3', 'PS Customer Specific')
                                    """

        level2_query = """
                        INSERT INTO static.kpi_level_2
                            (pk, type, client_name, version, numerator_type_fk, denominator_type_fk,
                            valid_from, valid_until, initiated_by, kpi_calculation_stage_fk)
                        VALUES
                          ('300000', 'SANOFI_PERFECT_STORE_COMPLIANCE', 'Perfect Store Compliance', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300001',  'SANOFI_PRIMARY_SHELF_COMPLIANCE', 'Primary Shelf Compliance', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300002',  'SANOFI_PRIMARY_SHELF_LOCATION', 'Shelf Location Compliance', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300003',  'SANOFI_PRIMARY_SHELF_LOCATION_BY_SKU', 'Shelf Location Compliance By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300004',  'SANOFI_PRIMARY_POSM_AVAILABILITY', 'POSM Availability Primary', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300005',  'SANOFI_PRIMARY_POSM_AVAILABILITY_BY_SKU', 'POSM Availability Primary By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300006',  'SANOFI_PRIMARY_MINIMUM_FACINGS', 'Product Minimum Facings Primary', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300007',  'SANOFI_PRIMARY_MINIMUM_FACINGS_BY_SKU', 'Product Minimum Facings Primary By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300008',  'SANOFI_PRIMARY_BLOCKED_TOGETHER', 'Blocked Together', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300009',  'SANOFI_PRIMARY_BLOCKED_TOGETHER_BY_SKU', 'Blocked Together By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300010',  'SANOFI_SECONDARY_SHELF', 'Secondary Shelf Compliance', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300011',  'SANOFI_SECONDARY_POSM_AVAILABILITY', 'POSM Availability Secondary', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300012',  'SANOFI_SECONDARY_POSM_AVAILABILITY_BY_SKU', 'POSM Availability Secondary By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300013',  'SANOFI_SECONDARY_MINIMUM_FACINGS', 'Product Minimum Facings Secondary', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300014',  'SANOFI_SECONDARY_MINIMUM_FACINGS_BY_SKU', 'Product Minimum Facings Secondary By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300015',  'SANOFI_MSL_COMPLIANCE', 'MSL Compliance', '1.0.0', '999', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3') ,
                          ('300016',  'SANOFI_MSL_COMPLIANCE_BY_SKU', 'MSL Compliance By SKU', '1.0.0', '1', '999', '1990-01-01', '2150-10-15', 'PS Customer Specific', '3')"""

        cur = self.aws_conn.db.cursor()
        # cur.execute(kpi_calculation_stage_query)
        # self.aws_conn.db.commit()
        # cur.execute(kpi_entity_type_query)
        # self.aws_conn.db.commit()
        cur.execute(level2_query)
        self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('test')
    Config.init()
    for project in ['sanofike']:
        print 'start project: ' + str(project)
        kpi = AddKPIs(project)
        kpi.add_kpi_level_2()
