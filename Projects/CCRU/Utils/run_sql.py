from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Sergey'


class CCRURunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statement_1 = """
                        DELETE FROM `static`.`atomic_kpi` WHERE `pk`>='4695';

                                               """

        cur = self.aws_conn.db.cursor()
        cur.execute(statement_1)
        self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('test')
    Config.init()
    for project in ['ccru-sand']:
        print 'start project: ' + str(project)
        sql_to_run = CCRURunSql(project)
        sql_to_run.run_it()
