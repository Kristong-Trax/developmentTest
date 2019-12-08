import pandas as pd
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers

QUERY = """
    SELECT scenes.scene_uid, scenes.session_uid, scenes.scene_date FROM
        probedata.scene scenes JOIN probedata.session sessions USING(session_uid)
    WHERE
        sessions.visit_date between "{}" and "{}" AND scenes.scene_date between "{}" and "{}"
        AND scenes.status = 6 AND sessions.status = "Completed" AND scenes.delete_time is null 
    ORDER BY scenes.scene_date, scenes.session_uid, scenes.scene_uid;"""


class GetRecalcBatches:

    def __init__(self, projects, email_addresses=None, recalc_scenes=False, local_path=None):
        if type(projects) is not list:
            projects = [projects]
        self.projects = projects
        if email_addresses is not None:
            if type(email_addresses) is not list:
                email_addresses = [email_addresses]
        self.email_addresses = email_addresses
        self.local_path = local_path
        self.recalc_scenes = recalc_scenes
        self.get_list_files = False
        self.query = None
        self.projects_batches = {}
        self.max_scenes_in_batch, self.max_sessions_in_batch = None, None
        # self.rds_conn = None

    def get_batches(self, query='', max_scenes_in_batch=10000, max_sessions_in_batch=None, start_date="2019-01-01",
                    end_date="2019-12-31"):
        self.max_scenes_in_batch = max_scenes_in_batch
        self.max_sessions_in_batch = max_sessions_in_batch
        if query:
            self.get_list_files = True
            self.query = query.format(start_date, end_date, start_date, end_date)
            if self.local_path is None and self.email_addresses is None:
                print "You need to insert your local path or email address!"
                return False
        else:
            self.query = QUERY.format(start_date, end_date, start_date, end_date)
        for project_name in self.projects:
            self.get_batches_for_project(project_name=project_name)
        return True

    def get_batches_for_project(self, project_name):
        rds_conn = PSProjectConnector(project_name, DbUsers.CalculationEng)
        batches = self.get_batches_dates(rds_conn)
        self.projects_batches[project_name] = batches

    def get_batches_dates(self, rds_conn):
        rds_conn.connect_rds()
        df = pd.read_sql_query(self.query, rds_conn.db)
        batches = self.separate_df(df)
        return batches

    def separate_df(self, df):
        current_index = 0
        temp_df = df[current_index, self.max_scenes_in_batch]
        return []


if __name__ == '__main__':
    pass