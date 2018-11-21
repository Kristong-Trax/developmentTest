import re
import pandas as pd
from datetime import datetime
from Trax.Utils.Conf.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Algo.Calculations.Core.DataProvider import Data

__author__ = 'Nimrod'

REVIEWER = 'PS Success Criteria'
EXTERIOR = 'Exterior'
OK_FEEDBACK = 'OK'
SESSION_FEEDBACK = ['store_doesnt_exist_or_wrong_store_details',
                    'categories_dont_apply_to_store',
                    'store_temp_closed',
                    'store_perma_closed']


class LAVAZZAGSuccessCriteria:

    SUCCESSFUL = 'Successful'
    UNSUCCESSFUL_TM = 'Unsuccessful (TM)'
    UNSUCCESSFUL_OTHER = 'Unsuccessful (Other)'

    def __init__(self, data_provider, output=None):
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.session_id = self.session_fk = self.data_provider[Data.SESSION_INFO]['pk'].iloc[0]
        self.scene_info = self.get_scenes_data()
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.review_date = datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')

    def get_scenes_data(self):
        scenes_data = self.data_provider[Data.SCENES_INFO]
        template_data = self.data_provider[Data.ALL_TEMPLATES]
        scenes_data = scenes_data.merge(template_data[['template_fk', 'template_name']], how='left', on='template_fk')
        return scenes_data

    @property
    def session_feedback(self):
        if not hasattr(self, '_session_feedback'):
            query = "SELECT answer_key FROM probedata.session_feedback WHERE session_uid = '{}'".format(self.session_uid)
            data = pd.read_sql(query, self.rds_conn.db)
            self._session_feedback = '' if data.empty else data.iloc[0]['answer_key']
        return self._session_feedback

    def main_function(self):
        comment = None
        if self.session_feedback in SESSION_FEEDBACK:
            status = self.UNSUCCESSFUL_TM
        elif self.scene_info.empty:
            status = self.UNSUCCESSFUL_OTHER
            comment = 'No Scenes'
        elif all(self.scene_info['template_name'].str.contains(EXTERIOR, flags=re.IGNORECASE)):
            status = self.UNSUCCESSFUL_OTHER
            comment = 'All scenes are exterior'
        elif self.session_feedback.upper() != OK_FEEDBACK:
            status = self.UNSUCCESSFUL_OTHER
            comment = "Session feedback is not '{}'".format(OK_FEEDBACK)
        else:
            status = self.SUCCESSFUL

        if status == self.SUCCESSFUL:
            Log.info('Session is successful')
        elif status == self.UNSUCCESSFUL_TM:
            Log.info('Session is unsuccessful (TM)')
            self.save_review(exclude_status=1, resolution_code=2, action_code=6)
        elif status == self.UNSUCCESSFUL_OTHER:
            Log.info('Session is unsuccessful (Other) - {}'.format(comment))
            self.save_review(exclude_status=1, resolution_code=2, action_code=5)

    def save_review(self, exclude_status, resolution_code, action_code):
        update_query = """
                       UPDATE probedata.session
                       SET exclude_status_fk = {}, resolution_code_fk = {}, action_code_fk = {},
                       quality_review_by = '{}', quality_review_date = '{}'
                       WHERE pk = {}
                       """.format(exclude_status, resolution_code, action_code, REVIEWER, self.review_date, self.session_id)
        cur = self.rds_conn.db.cursor()
        cur.execute(update_query)
        self.rds_conn.db.commit()