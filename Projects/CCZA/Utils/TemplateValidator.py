from Trax.Tools.ProfessionalServices.Utils.MainTemplate import Main_Template
from Projects.CCZA.Utils.ParseTemplates import parse_template
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Conf.Keys import DbUsers
import pandas as pd
import numpy as np
from Projects.CCZA.Utils.Const import Const


class CczaTemplateValidator(Main_Template):

    LIST_OF_SHEETS = [Const.KPIS, Const.LIST_OF_ENTITIES, Const.SOS_WEIGHTS, Const.SOS_TARGETS, Const.PRICING_WEIGHTS,
                      Const.PRICING_TARGETS, Const.SURVEY_QUESTIONS, Const.FLOW_PARAMETERS]

    def __init__(self, project_name, file_url):
        Main_Template.__init__(self)
        self.project = project_name
        self.file_path = file_url
        self.rds_conn = self.rds_connect
        self.store_data = self.get_store_data
        self.all_products = self.get_product_data

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
        pass

