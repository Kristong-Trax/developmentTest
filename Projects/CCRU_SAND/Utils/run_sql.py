# -*- coding: utf-8 -*-
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector

__author__ = 'Sergey'


class CCRU_SANDRunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = PSProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statements = \
"""
UPDATE `static`.`atomic_kpi` SET `name`='Juice Shelf: Moya Semya - Apple Mix - 0.95L', `description`='Juice Shelf: Moya Semya - Apple Mix - 0.95L', `display_text`='Juice Shelf: Moya Semya - Apple Mix - 0.95L' WHERE `pk`='3362';
UPDATE `static`.`atomic_kpi` SET `name`='Moya Semya - Apple Mix - 0.95L', `description`='Moya Semya - Apple Mix - 0.95L', `display_text`='Moya Semya - Apple Mix - 0.95L' WHERE `pk`='4235';
UPDATE `static`.`atomic_kpi` SET `name`='Moya Semya - Apple Mix - 0.95L', `description`='Moya Semya - Apple Mix - 0.95L', `display_text`='Moya Semya - Apple Mix - 0.95L' WHERE `pk`='4281';
UPDATE `static`.`atomic_kpi` SET `name`='Moya Semya - Pineapple-Mango - 0.95L', `description`='Moya Semya - Pineapple-Mango - 0.95L', `display_text`='Moya Semya - Pineapple-Mango - 0.95L' WHERE `pk`='4236';
UPDATE `static`.`atomic_kpi` SET `name`='Moya Semya - Pineapple-Mango - 0.95L', `description`='Moya Semya - Pineapple-Mango - 0.95L', `display_text`='Moya Semya - Pineapple-Mango - 0.95L' WHERE `pk`='4282';
UPDATE `static`.`atomic_kpi` SET `name`='Moya Semya - Apple-Strawberry - 0.95L', `description`='Moya Semya - Apple-Strawberry - 0.95L', `display_text`='Moya Semya - Apple-Strawberry - 0.95L' WHERE `pk`='4248';
"""
# """
# DELETE FROM `static`.`kpi_set` WHERE `pk`>='64';
# """
# """
# DELETE FROM `static`.`kpi` WHERE `pk`>='2843';
# """
# """
# UPDATE `static`.`kpi_set` SET `name`='Contract Execution 2018 X' WHERE `pk`='56';
# """

        statements = statements.split(';\n')
        for statement in statements:
            if len(statement) > 0:
                cur = self.aws_conn.db.cursor()
                print statement
                cur.execute(statement)
                self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('Run SQL')
    Config.init()
    for project in ['ccru_sand']:
        print 'Start Project: ' + str(project)
        sql_to_run = CCRU_SANDRunSql(project)
        sql_to_run.run_it()



