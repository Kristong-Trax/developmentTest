from Trax.Data.Projects.ProjectConnector import AwsProjectConnector
from Trax.Utils.Conf.Configuration import Config
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Keys import DbUsers

__author__ = 'Sergey'


class RunSql:
    def __init__(self, project):
        self.project = project
        self.aws_conn = AwsProjectConnector(self.project, DbUsers.CalculationEng)

    def run_it(self):
        statement_1 = """
                        UPDATE `static`.`atomic_kpi` SET `name`='Tea Display: Lead SKU Fuze Berry/Lemon - 1L', `description`='Tea Display: Lead SKU Fuze Berry/Lemon - 1L', `display_text`='Tea Display: Lead SKU Fuze Berry/Lemon - 1L' WHERE `pk`='3256';

                                               """

        cur = self.aws_conn.db.cursor()
        cur.execute(statement_1)
        self.aws_conn.db.commit()


if __name__ == '__main__':
    Log.init('test')
    Config.init()
    for project in ['ccru']:
        print 'start project: ' + str(project)
        sql_to_run = RunSql(project)
        sql_to_run.run_it()
