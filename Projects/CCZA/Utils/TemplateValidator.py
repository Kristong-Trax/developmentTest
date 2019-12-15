from Trax.Tools.ProfessionalServices.Utils.MainTemplate import Main_Template
from Projects.CCZA.Utils.ParseTemplates import parse_template
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
import pandas as pd
import numpy as np
from Projects.CCZA_SAND.Utils.Const import Const


class CczaTemplateValidator(Main_Template):

    #
    # SHEETS_COL_MAP = {Const.KPIS:  set([Const.KPI_NAME, Const.KPI_GROUP, Const.KPI_TYPE, 'Tested KPI Group',
    #                                     Const.TARGET, Const.WEIGHT_SHEET, 'SCORE']),
    #                  Const.LIST_OF_ENTITIES: set([]), Const.SOS_WEIGHTS, Const.SOS_TARGETS, Const.PRICING_WEIGHTS,
    #                   Const.PRICING_TARGETS, Const.SURVEY_QUESTIONS, Const.FLOW_PARAMETERS}

    def __init__(self, project_name, file_url):
        Main_Template.__init__(self)
        self.project = project_name
        self.template_path = file_url
        self.rds_conn = self.rds_connect
        self.store_data = self.get_store_data
        self.all_products = self.get_product_data
        self.kpi_sheets = {}

    @property
    def rds_connect(self):
        self.rds_conn = PSProjectConnector(self.project, DbUsers.Ps)
        try:
            pd.read_sql_query('select pk from probedata.session limit 1', self.rds_conn.db)
        except Exception as e:
            self.rds_conn.disconnect_rds()
            self.rds_conn = PSProjectConnector(self.project, DbUsers.Ps)
        return self.rds_conn

    @property
    def get_store_data(self):
        query = "select * from static.stores"
        self.store_data = pd.read_sql_query(query, self.rds_conn.db)
        return self.store_data

    @property
    def get_product_data(self):
        query = "select * from static.product"
        self.all_products = pd.read_sql_query(query, self.rds_conn.db)
        return self.store_data

    def validate_template_data(self):
        self.check_all_tabs_exist()

        # validate sheets, columns format
        # validate sheets contents against sheet contents
        # validate kpi names in the db both against old tables and new tables
        # validate specific template parameters
        # validate all weights and targets are aligned
        #
        pass

    def check_all_tabs_exist(self):
        for name in Const.sheet_names_and_rows:
            try:
                self.kpi_sheets[name] = parse_template(self.template_path, sheet_name=name,
                                                       lower_headers_row_index=Const.sheet_names_and_rows[name])
            except Exception as e: # look up the type of exception in case sheet name is missing
                self.errorHandler.log_error('Sheet {} is missing in the file'.format(name))
