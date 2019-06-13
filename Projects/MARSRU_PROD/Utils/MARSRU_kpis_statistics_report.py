import os
import time
import datetime
import tempfile
import pandas as pd

from Trax.Cloud.Services.Connector.Keys import DbUsers, EmailUsers
from Trax.Cloud.Services.Mailers.Factory import MailerFactory
from Trax.Data.Projects.Connector import ProjectConnector
from Trax.Data.Simon.Connectors import SimonConnectors
from Trax.Utils.Logging.Logger import Log
from Trax.Cloud.Services.Connector.Logger import LoggerInitializer
from Trax.Apps.Jobs.ProfessionalServicesReports.ReportUtils import ReportsUtils as Ru
from Trax.Apps.Services.SimonSDK.Public.SimonMethods import SimonSDK
from Trax.Apps.Services.SimonSDK.Public.SimonEnum import AnalysisResultEnum, MasterDataEnum
from Trax.Cloud.Services.Storage.Factory import StorageFactory


BUCKET = 'traxusapi'
PROJECT = 'marsru-prod'

BUCKET_FOLDER = 'marsru_kpis_statistics_reports'
REPORT_NAME = 'TRAX MARSRU KPIs Statistics Report'
FILE_NAME = 'trax_marsru_kpis_statistics'

RECEIVERS_BY_DEFAULT = ['sergey@traxretail.com']

__author__ = 'sergey'


class MARSRU_PRODMARSRU_KPIsStatistics:

    def __init__(self, arg_sd=None, arg_ed=None, arg_to=None, pivoted='pivoted'):
        self.log_prefix = BUCKET_FOLDER + '_' + PROJECT
        Log.init(self.log_prefix)
        Log.info(self.log_prefix + ' Opening SQL connector')
        self.project_name = PROJECT
        self.rds_conn = ProjectConnector(self.project_name, DbUsers.CalculationEng)

        self.pivoted = True if pivoted == 'pivoted' else False

        self.current_date = datetime.datetime.utcnow().date()

        self.start_date = self.current_date - datetime.timedelta(1)
        self.end_date = self.current_date - datetime.timedelta(1)

        self.start_date = arg_sd if arg_sd else self.start_date.strftime('%Y-%m-%d')
        self.end_date = arg_ed if arg_ed else self.end_date.strftime('%Y-%m-%d')

        self.report_name = FILE_NAME + '_' + str(self.start_date) + '_' + str(self.end_date) \
                           + ('_pivoted' if self.pivoted else '') \
                           + '_' + str(int(time.mktime(datetime.datetime.now().timetuple()))) \
                           + '.xlsx'

        self.receivers = arg_to.split(',') if arg_to else None

    @property
    def smn_conn(self):
        if not hasattr(self, '_smn_conn'):
            self._smn_conn = SimonConnectors()
        return self._smn_conn

    def create_report(self):
        Log.info("Start creating the report")

        Log.info("Parameters: {} {} {}".format(self.start_date, self.end_date, self.receivers if self.receivers else ''))

        raw_data = SimonSDK.get_analysis_results(self.project_name,
                                                 param_from_visit_date=self.start_date,
                                                 param_to_visit_date=self.end_date,
                                                 sections=[AnalysisResultEnum.KPIs, AnalysisResultEnum.Calculations])

        store_info = SimonSDK.get_master_data(self.project_name,
                                              required_data=[MasterDataEnum.Stores])[MasterDataEnum.Stores]

        # kpis
        analysis_results = \
            self.unfold_analysis_results_kpi_section(
                raw_data.get(AnalysisResultEnum.KPIs)[
                    raw_data.get(AnalysisResultEnum.KPIs)['name'] == 'MARS KPIs API'])
        analysis_results = analysis_results[[
            'session_uid',
            'name',
            'result']]\
            .merge(raw_data.get(AnalysisResultEnum.MetaData)[[
                        'session_uid',
                        'store_number']],
                   left_on='session_uid',
                   right_on='session_uid',
                   how='left')\
            .merge(store_info[[
                    'store_number',
                    'store_type_name']],
                   left_on='store_number',
                   right_on='store_number',
                   how='left')
        analysis_results.loc[analysis_results['name'] == 'MARS KPIs API', 'result'] = 1

        report_df = analysis_results[[
            'name',
            'result',
            'store_type_name']]\
            .copy()
        report_df.loc['result'] = report_df['result'].apply(lambda x: None if str(x) == '' else x)

        if self.pivoted:
            report_df = pd.pivot_table(report_df,
                                       index=['store_type_name'],
                                       columns=['name'],
                                       values=['result'],
                                       aggfunc={'result': 'count'},
                                       fill_value=0)

        self.report_name = self.report_name.replace('.xlsx', '_kpis.xlsx')

        Log.info("Creating excel file")
        self.create_excel_and_format_report(report_df)
        Log.info("Uploading report to S3 bucket")
        report_path = self.upload_report_to_s3()
        self.send_email(report_path, self.receivers)

        # calculations
        analysis_results = \
            self.unfold_analysis_results_kpi_section(raw_data.get(AnalysisResultEnum.Calculations))
        analysis_results = analysis_results[[
            'session_uid',
            'name',
            'result']]\
            .merge(raw_data.get(AnalysisResultEnum.MetaData)[[
                'session_uid',
                'store_number']],
                       left_on='session_uid',
                       right_on='session_uid',
                       how='left') \
            .merge(store_info[[
                'store_number',
                'store_type_name']],
                       left_on='store_number',
                       right_on='store_number',
                       how='left')

        report_df = analysis_results[[
            'name',
            'result',
            'store_type_name']]

        report_df['result'] = report_df['result'].apply(lambda x: None if str(x) == '' else x)
        if self.pivoted:
            report_df = pd.pivot_table(report_df,
                                       index=['store_type_name'],
                                       columns=['name'],
                                       values=['result'],
                                       aggfunc={'result': 'count'},
                                       fill_value=0)

        self.report_name = self.report_name.replace('_kpis.xlsx', '_calculations.xlsx')

        Log.info("Creating excel file")
        self.create_excel_and_format_report(report_df)
        Log.info("Uploading report to S3 bucket")
        report_path = self.upload_report_to_s3()
        self.send_email(report_path, self.receivers)

    def unfold_analysis_results_kpi_section(self, input_df, parent_session_uid=None, parent_kpi_id=0, kpi_id=0):
        """
        The function unfolds entities section for current kpi in the same row
        and adds rows unfolding subordinate kpi levels recursively
        :param input_df: raw dataframe with the analysis results kpi section after Simon SDK retrieval
        :param parent_session_uid: used internally by recursive call only
        :param parent_kpi_id: used internally by recursive call only
        :param kpi_id: used internally by recursive call only
        :return: dataframe with flat unfolded kpi section
        """
        output_df = pd.DataFrame()
        if parent_kpi_id == 0:
            is_level_0 = True
        else:
            is_level_0 = False
        for i, row in input_df.iterrows():
            kpi_id += 1
            row['kpi_id'] = kpi_id
            if not is_level_0:
                row['parent_kpi_id'] = parent_kpi_id
            if parent_session_uid:
                row['session_uid'] = parent_session_uid
            session_uid = row['session_uid']
            if 'entities' in row.keys():
                for entity in row['entities']:
                    entity_type = entity.asDict().get('type')
                    if not (entity_type is None or len(entity_type) == 0 or entity_type == 'none'):
                        row['entities_' + entity_type] = entity.asDict().get('uid')
                row = row.drop('entities')
            if 'results' in row.keys():
                if row['results']:
                    current_kpi_id = kpi_id
                    for result in row['results']:
                        result_df = self.unfold_analysis_results_kpi_section(pd.DataFrame([result.asDict()]),
                                                                             parent_session_uid=session_uid,
                                                                             parent_kpi_id=current_kpi_id,
                                                                             kpi_id=kpi_id)
                        output_df = output_df.append(result_df)
                        kpi_id += len(result_df)
                row = row.drop('results')
            output_df = output_df.append(row)
        return output_df

    @staticmethod
    def unfold_analysis_results_recognized_items_section(input_df):
        """
        The function unfolds recognized_items section on scene and item basis
        :param input_df: raw dataframe with the analysis results recognized_items section after Simon SDK retrieval
        :return: dataframe with flat recognized_items kpi section
        """
        output_df = pd.DataFrame()
        for i, row in input_df.iterrows():
            if 'items' in row.keys():
                output_row = row.drop('items')
                for item in row['items']:
                    item = item.asDict()
                    for item_key in item.keys():
                        if item_key == 'count':
                            count = item[item_key].asDict()
                            for count_key in count.keys():
                                output_row['items_count_' + count_key] = count[count_key]
                        else:
                            output_row['items_' + item_key] = item[item_key]
                    output_df = output_df.append(output_row)
        return output_df

    def create_excel_and_format_report(self, table):
        report_path = os.path.join(tempfile.gettempdir(), self.report_name)
        writer = pd.ExcelWriter(report_path, engine='xlsxwriter')
        table.to_excel(writer, sheet_name='Sheet1', index=True)

        writer.save()

    def upload_report_to_s3(self):
        folder_in_s3_bucket = BUCKET_FOLDER
        report_path = os.path.join(tempfile.gettempdir(), self.report_name)
        storage_connector = StorageFactory.get_connector(BUCKET)
        storage_connector.save_file_stream(folder_in_s3_bucket, self.report_name, open(report_path, 'r'))
        domain = storage_connector.base_domain[1:]
        file_link = "https://" + domain + "/" + BUCKET + "/" + folder_in_s3_bucket + "/" + self.report_name
        return file_link

    def send_email(self, report_path, receivers):
        """
        This func sends report email to clients
        """
        email_subject = REPORT_NAME + ' ({})'.format(self.current_date.strftime("%d.%m.%Y"))
        email_body = 'Hello,<br><br>' \
                     'You can find the report by the following link:<br><br>' \
                     '{} <br>' \
                     '<br><br>Best,<br><br>' \
                     'Simon - Trax Retail'.format(report_path)
        email_body = Ru.add_ps_comment_to_email_bodies(email_body, comment=Ru.ENGLISH)
        mailer = MailerFactory.get_mailer(EmailUsers.TraxMailer)
        # receivers = mailer.get_receivers_from_groups(self.project_name+'_Status') if not receivers else receivers
        receivers = RECEIVERS_BY_DEFAULT if not receivers else receivers
        mailer.send_email(project_name=PROJECT, receivers=receivers, email_body=email_body, subject=email_subject)


def run(sd=None, ed=None, to=None, pivoted=None):
    try:
        marsru_report = MARSRU_PRODMARSRU_KPIsStatistics(sd, ed, to, pivoted)
        marsru_report.create_report()
        return 0
    except Exception as e:
        Log.error(REPORT_NAME + ' has failed with {}'.format(str(e)))
        return 1


if __name__ == '__main__':
    LoggerInitializer.init('test run')
    # marsru_report = MARSRU_PRODMARSRU_KPIsStatistics()
    marsru_report = MARSRU_PRODMARSRU_KPIsStatistics('2019-02-25', '2019-03-10', 'sergey@traxretail.com')
    marsru_report.create_report()
