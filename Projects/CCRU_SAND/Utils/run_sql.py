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
    CREATE TABLE pservice.planned_visits 
    (
        store_fk INT(11) NOT NULL,
        visit_date DATE NOT NULL,
        sales_rep_fk INT(11) NOT NULL,
        planned_flag INT(1) NOT NULL,
        PRIMARY KEY (store_fk, visit_date, sales_rep_fk)
    );
"""

        statements = statements.split(';\n')
        for statement in statements:
            if len(statement) > 0:
                cur = self.aws_conn.db.cursor()
                print statement
                cur.execute(statement)
                self.aws_conn.db.commit()


if __name__ == '__main__':
    # Log.init('Run SQL')
    Config.init()
    for project in ['ccru-sand']:
        print 'Start Project: ' + str(project)
        sql_to_run = CCRU_SANDRunSql(project)
        sql_to_run.run_it()



