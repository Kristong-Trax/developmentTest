import os
import datetime
import pandas as pd
from Trax.Cloud.Services.Connector.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Data.Simon.Connectors import SimonConnectors
from Trax.Utils.Logging.Logger import Log

REPORT_PATH = '/tmp/mychemist_report_{}_{}.xlsx'
DEFAULT_CYCLE = []
NO_CYCLE = 'no_cycle'

# REPORT HEADERS
LOCATION = 'Location'
STORE_NAME = 'Store Name'
TASK = 'Task Location'
PRODUCT_HIERARCHY = 'Product Hierarchy'
EN_SKU_NAME = 'English SKU Name'
CATEGORY = 'Category'
MANUFACTURER_NAME = 'Manufacturer Name'
BRAND_NAME = 'Brand Name'
AVAILABILITY_CHECK = 'Availability Check'
LAST_CHECK = 'Last Check'
AVAILABILITY_1_WEEK = '1 Week'
AVAILABILITY_1_MONTH = '1 Month'


class MyChemistReport(object):
    """
    MyChemist Report - OOS of SKU per weekly and monthly cycle period
    """

    def __init__(self, project_name):
        self.project = project_name
        self.rds_conn = None
        self.cur = None
        self.connect()
        self.report_gen = ReportGenerator(self.project)

    def __del__(self):
        if self.rds_conn is not None:
            self.rds_conn.disconnect_rds()

    def _get_filter_query(self):
        return """
                    select sif.visit_date, st.name store, p.name product, cat.name category, m.name manufacturer, b.name brand, 1 - sif.oos_sc availability, 1 - (agg_7.oos / agg_7.expected) availability_1_week, 1 - (agg_30.oos / agg_30.expected) availability_1_month, sc.connected_shelf_fk, agg_30.session_start_time session_start_time from
                    (select  s_cur.pk, s_cur.session_uid, s_cur.start_time session_start_time, s_cur.store_fk, sif.item_id, sum(sif.oos_sc) oos, count(1) expected 
                    from probedata.session s_cur
                    join probedata.session s_agg on 
                        s_agg.visit_date <= s_cur.visit_date and
                        s_agg.visit_date > date_add(s_cur.visit_date, interval -7 day) 
                        and s_agg.store_fk = s_cur.store_fk
                    join reporting.scene_item_facts sif on sif.session_id = s_agg.pk
                    where s_cur.session_uid = %(session_uid)s
                    group by s_cur.pk, sif.item_id ) agg_7 
                    join
                    (select  s_cur.pk, s_cur.session_uid, s_cur.start_time session_start_time, s_cur.store_fk, sif.item_id, sum(sif.oos_sc) oos, count(1) expected 
                    from probedata.session s_cur
                    join probedata.session s_agg on 
                        s_agg.visit_date <= s_cur.visit_date and
                        s_agg.visit_date > date_add(s_cur.visit_date, interval -30 day) 
                        and s_agg.store_fk = s_cur.store_fk
                    join reporting.scene_item_facts sif on sif.session_id = s_agg.pk
                    where s_cur.session_uid = %(session_uid)s
                    group by s_cur.pk, sif.item_id) agg_30 
                    on agg_7.pk = agg_30.pk and agg_7.item_id = agg_30.item_id
                    join static_new.product p on agg_7.item_id = p.pk
                    join static.stores st on st.pk = agg_7.store_fk
                    join static_new.category cat on p.category_fk = cat.pk
                    join static_new.brand b on p.brand_fk = b.pk
                    join static_new.manufacturer m on b.manufacturer_fk = m.pk
                    join reporting.scene_item_facts sif on sif.session_id = agg_7.pk and sif.item_id = agg_7.item_id
                    join probedata.scene sc on sif.scene_id = sc.pk
                    where p.type = 'SKU'; 
               """


    def _query_db(self, query, **kwargs):
        return pd.read_sql_query(query, self.rds_conn.db, params=kwargs)


    def _get_report_path(self, session_uid, session_time):
        save_path = REPORT_PATH.format(session_uid, session_time)
        return save_path

    def _get_session_time(self, query_result):
        return query_result['session_start_time'][0]

    def connect(self):
        self.rds_conn = PSProjectConnector(self.project, DbUsers.ReadOnly)
        self.cur = self.rds_conn.db.cursor()
        queries = ['SET group_concat_max_len = 1000000']
        for query in queries:
            self.cur.execute(query)

    def generate_report(self, session_uid):
        result = self._query_db(self._get_filter_query(), session_uid=session_uid)
        if not self.is_query_result_valid(result, session_uid):
            return False

        session_time = self._get_session_time(result)
        save_path = self._get_report_path(session_uid, session_time)

        self.report_gen.save_report(result, save_path)
        self.report_gen.send_report_to_recipients(save_path, session_uid, session_time)
        self.report_gen.delete_report_file(save_path)

    def is_query_result_valid(self, result, session_uid):
        if len(result) == 0:
            Log.warning('Could not generate MyChemist report for session {}. Query returned empty result'.format(
                session_uid))
            return False

        if len(result[~result['connected_shelf_fk'].isnull()]) > 0:
            Log.debug('This session was generated by IOT device, omitting the report')
            return False

        return True


class ReportGenerator:
    """
    Class to handle the MyChemist xls generation only
    """

    def __init__(self, project):
        self.project = project
        self.excel_writer = None
        self.workbook = None


    def _get_visit_date(self, data, visit_date):
        return data.pop(visit_date)

    def _removed_unnecessary_columns(self, data):
        data.drop(['connected_shelf_fk', 'session_start_time'], axis=1, inplace=True)
        return data

    def _rename_columns(self, data):
        renamed = data.rename(columns={
                'store': 'Store Name',
                'product': 'English SKU Name',
                'category': 'Category',
                'manufacturer': 'Manufacturer Name',
                'brand': 'Brand Name',
                'availability': 'Last check',
                'availability_1_week': '1 Week',
                'availability_1_month': '1 Month',
            })
        return renamed


    def _set_report_format(self, worksheet, grouped_data):
        report_start_row = 4

        for col in self._get_column_widths():
            worksheet.set_column('{0}:{0}'.format(col[0]), col[1])

        num_of_rows = len(grouped_data)
        agg_cond_format_range = "G{}:H{}".format(report_start_row, report_start_row + num_of_rows - 1)
        check_cond_format_range = "F{}:F{}".format(report_start_row, report_start_row + num_of_rows - 1)
        report_format_range = "A{}:E{}".format(report_start_row, report_start_row + num_of_rows - 1)
        worksheet.autofilter("A{}:E{}".format(report_start_row - 1, report_start_row + num_of_rows - 1))

        for row_num in range(report_start_row - 1, report_start_row + num_of_rows):
            worksheet.set_row(row_num, 19.8)

        worksheet.conditional_format(report_format_range, self._get_conditional_format('report'))
        worksheet.conditional_format(check_cond_format_range, self._get_conditional_format('oos'))
        worksheet.conditional_format(check_cond_format_range, self._get_conditional_format('dist'))
        worksheet.conditional_format(agg_cond_format_range, self._get_conditional_format('low-dist'))
        worksheet.conditional_format(agg_cond_format_range, self._get_conditional_format('high-dist'))


    def _set_report_header(self, worksheet, visit_dates):
        worksheet.merge_range('A1:A2', LOCATION, self._get_format('header'))
        worksheet.merge_range('B1:E2', PRODUCT_HIERARCHY, self._get_format('header'))
        worksheet.merge_range('F1:H1', AVAILABILITY_CHECK, self._get_format('header'))
        worksheet.merge_range('F2:H2', '{}'.format(visit_dates[0][0]), self._get_format('date-header'))

    @staticmethod
    def _get_column_widths():
        return [('A', 35), ('B', 68), ('C', 15), ('D', 23), ('E', 15), ('F', 10), ('G', 10), ('H', 10), ('I', 10)]

    def _get_format(self, w_format):
        return self.workbook.add_format(self._get_workbook_format(w_format))

    def _get_workbook_format(self, w_format):
        return {
            'header': {'bold': True, 'font': 'Ariel', 'font_size': 14,
                       'align': 'center', 'valign': 'vcenter',
                       'bg_color': '#C0C0C0',
                        'border': 1},
            'date-header': {'font': 'Arial', 'font_size': 10,
                            'align': 'center', 'valign': 'vcenter',
                            'bg_color': '#C0C0C0',
                            'border': 1},
            'sub-header': {'font': 'Arial', 'font_size': 12, 'align': 'center',
                           'valign': 'vcenter', 'bg_color': '#C0C0C0', 'border': 1},
            'oos': {'font_color': '#C00000', 'bg_color': '#C00000', 'border': 1},
            'dist': {'font_color': '#FFFFFF', 'bg_color': '#FFFFFF', 'border': 1},
            'low-dist': {'font_color': '#000000', 'align': 'center', 'bg_color': '#FFC000', 'border': 1, 'num_format': '0.00'},
            'high-dist': {'font_color': '#000000', 'align': 'center', 'bg_color': '#22B14C', 'border': 1, 'num_format': '0.00'},
            'report': {'border': 1},
            'default': {'font_color': '#000000'}
        }.get(w_format, 'default')

    def _get_conditional_format(self, c_format):
        return {
            'oos': {'type': 'cell',
                    'criteria': '=',
                    'value': 0,
                    'format': self._get_format('oos')},
            'dist': {'type': 'cell',
                    'criteria': '=',
                    'value': 1,
                    'format': self._get_format('dist')},
            'low-dist': {'type': 'cell',
                     'criteria': '<',
                     'value': 0.5,
                     'format': self._get_format('low-dist')},
            'high-dist': {'type': 'cell',
                      'criteria': '>=',
                      'value': 0.5,
                      'format': self._get_format('high-dist')},
            'report': {'type': 'no_errors',
                       'format': self._get_format('report')},
            'default': {}
        }.get(c_format, 'default')

    def save_report(self, grouped_data, save_path):
        self.excel_writer = pd.ExcelWriter(save_path, engine='xlsxwriter')
        visit_dates = [self._get_visit_date(grouped_data, visit_date='visit_date')]
        grouped_data = self._removed_unnecessary_columns(grouped_data)
        grouped_data = self._rename_columns(grouped_data)
        grouped_data.to_excel(self.excel_writer, sheet_name='Daily Report', startcol=0, startrow=2, index=False)

        self.workbook = self.excel_writer.book
        worksheet = self.excel_writer.sheets['Daily Report']

        self._set_report_header(worksheet, visit_dates)
        self._set_report_format(worksheet, grouped_data)

        self.excel_writer.save()

    def send_report_to_recipients(self, save_path, session_uid, session_time):
        conn = SimonConnectors()
        email_body = """
        This is a report for {} project session {} started at
        """.format(self.project, session_uid, session_time)
        conn.send_email(group=self.project, email_subject='mychemistau report {}'.format(session_time),
                        email_body=email_body, files=[save_path])

    @staticmethod
    def delete_report_file(save_path):
        os.remove(save_path)
