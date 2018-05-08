
import pandas as pd
from datetime import datetime

from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

__author__ = 'nimrodp'

OBJECT = 'Visit'
USER = 'Trax Engine'
CHANNEL_FIELD = 'additional_attribute_15'
MODERN_TRADE = 'Modern Trade'
TRADITIONAL_TRADE = 'Traditional Trade'

DB_TABLE = 'probedata.quality_reviews'


class CCTH_SANDSuccessfulSessions:

    def __init__(self, aws_connector, session_uid):
        self.log_prefix = 'Successful Visits - '
        self.session_uid = session_uid
        self.aws_connector = aws_connector
        self.object = OBJECT
        self.object_fk, self.channel = self.get_session_data()
        self.status = None
        self.reason = None
        self.comment = None
        self.user = USER
        self.scenes_status = self.get_scenes_status()
        self.survey_status = self.get_survey_status()

    def get_session_data(self):
        query = """
                SELECT se.pk AS session_fk, st.{} AS channel
                FROM probedata.session se
                JOIN static.stores st ON st.pk = se.store_fk
                WHERE se.session_uid = '{}'""".format(CHANNEL_FIELD, self.session_uid)
        data = pd.read_sql_query(query, self.aws_connector.db)
        return data['session_fk'].values[0], data['channel'].values[0]

    def update_session(self):
        # Log.info(self.log_prefix + 'Starting to update success status for session {}'.format(self.session_uid))
        non_completed_scenes = self.scenes_status[self.scenes_status['status'] == 'Not Completed']
        completed_survey = self.survey_status[self.survey_status['non_completed'] == 0]
        non_completed_survey = self.survey_status[self.survey_status['non_completed'] == 1]

        if len(self.scenes_status) > 0 and len(non_completed_scenes) == 0 and len(completed_survey) > 0:
            self.status = 1
            self.reason = 'Surveyed'

        elif len(self.scenes_status) > 0 and self.survey_status.empty:
            self.status = 2
            self.reason = 'Partial Data - No Survey Answers'

        elif len(self.scenes_status) > 0 and not self.survey_status.empty \
                and len(non_completed_survey) == len(self.survey_status):
            self.status = 3
            self.reason = 'Incomplete - With Comment:'
            self.update_comment()

        elif len(non_completed_survey) > 0:
            self.status = 4
            self.reason = 'Incomplete - With Comment:'
            self.update_comment()

        elif len(non_completed_scenes) > 0:
            self.status = 5
            self.reason = 'Partial Data'

        elif len(completed_survey) > 0 and non_completed_survey.empty:
            self.status = 6
            self.reason = 'Partial Data - No Comment'

        elif self.scenes_status.empty and self.survey_status.empty:
            self.status = 7
            self.reason = 'Partial Data'

        else:
            Log.warning(self.log_prefix + 'session {} didn\'t match any of the scenario options'.format(self.session_uid))
            return

        self.write_session_data()
        # Log.info(self.log_prefix + 'Done updating success status for session {}'.format(self.session_uid))
        return self.status

    def get_scenes_status(self):
        query = """
                SELECT pk as scene_id,
                       CASE
                            WHEN status = 6 THEN 'Completed'
                            ELSE 'Not Completed'
                       END status
                FROM probedata.scene
                WHERE session_uid = '{}' AND delete_time IS NULL""".format(self.session_uid)
        scenes_status = pd.read_sql_query(query, self.aws_connector.db)
        return scenes_status

    def get_survey_status(self):
        query = """
                SELECT sq.pk as question_id,
                       sr.selected_option_text,
                       sr.text_value,
                       sr.number_value,
                       sq.group_name AS question_group,
                       CASE
                            WHEN sq.group_name LIKE '%Non Completion%' THEN 1
                            ELSE 0
                       END non_completed
                FROM probedata.survey_response sr
                JOIN static.survey_question sq ON sq.pk = sr.question_fk
                WHERE sr.session_uid = '{}' AND sr.delete_time IS NULL""".format(self.session_uid)
        survey_status = pd.read_sql_query(query, self.aws_connector.db)
        return survey_status

    def update_comment(self):
        if self.channel == TRADITIONAL_TRADE:
            question_ids = [268, 239]
        elif self.channel == MODERN_TRADE:
            question_ids = [165, 166]
        else:
            Log.warning(self.log_prefix + 'Channel {} is not valid'.format(self.channel))
            return
        comment = []
        for question in question_ids:
            question_data = self.survey_status[self.survey_status['question_id'] == question]
            if not question_data.empty:
                question_data = question_data.iloc[0]
                for field in ['selected_option_text', 'text_value', 'number_value']:
                    value = question_data[field]
                    if value:
                        comment.append(value)
                        break
        self.comment = ' '.join(comment).encode('utf-8')

    def write_session_data(self):
        created_time = datetime.utcnow().isoformat()
        attributes = pd.DataFrame([(self.object, self.object_fk, self.status, self.reason, self.comment, self.user,
                                    created_time, self.session_uid)],
                                  columns=['object', 'object_fk', 'status', 'reason', 'comment', 'user',
                                           'created_time', 'session_uid'])
        query = insert(attributes.to_dict(), DB_TABLE)
        cur = self.aws_connector.db.cursor()
        cur.execute("delete from {} where object_fk = '{}' and object = '{}'".format(DB_TABLE, self.object_fk, OBJECT))
        cur.execute(query)
        self.aws_connector.db.commit()


