import pandas as pd
from Projects.GMIUS.Utils.Const import Const
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Data.Projects.Connector import ProjectConnector




class ResultUploader():
    def __init__(self, template_path):
        self.rds_conn = ProjectConnector(self.project, DbUsers.CalcAdmin)
        pd.read_sql_query(query, self.rds_conn.db)

    def load_data(self, template_path):



    def connect(self):
        self.rds_conn = ProjectConnector(self.project, DbUsers.CalcAdmin)
        self.cur = self.rds_conn.db.cursor()


ru = ResultUploader(os.path.join(os.path.dirname(os.path.realpath(__file__)), '../Data', 'GMI KPI Template v0.2.xlsx')
)
