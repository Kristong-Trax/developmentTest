from Trax.Algo.Calculations.Core.AceProjectConnectorDBGateway import SQLStaticGateway, SQLSessionGateway, \
    SQLReportsGateway
from Trax.Algo.Calculations.Core.Constants import Fields as Fd
from Trax.Algo.Calculations.Core.DataProvider import Data

__Author__ = 'Dudi_S'


class Fields(object):

    ALL_EXCLUDED_TEMPLATES = 'all_excluded_templates'
    ALL_EXCLUDED_TEMPLATE_PRODUCTS = 'all_excluded_template_products'
    ALL_EXCLUDED_PRODUCTS = 'all_excluded_products'

    SESSION_INFO = 'session_info'
    RELEVANT_INCLUDE_EXCLUDE_SETS = 'relevant_include_exclude_sets'
    SOS_INCLUDE_EXCLUDE_SET = 'sos_include_exclude_set'
    SOS_EXCLUDED_TEMPLATES = 'sos_excluded_templates'
    SOS_EXCLUDED_PRODUCTS = 'sos_excluded_products'
    SOS_EXCLUDED_TEMPLATE_PRODUCTS = 'sos_excluded_template_products'
    VISIT_DATE = 'visit_date'


class ShareOfDisplayDataProvider(object):

    def __init__(self, project_connector, session_uid):
        self._data = dict()
        self.project_connector = project_connector
        self.session_uid = session_uid
        self.get_exclude_data()

    def get_exclude_data(self, ):
        session_gateway = SQLSessionGateway(self.project_connector.db)
        static_gateway = SQLStaticGateway(self.project_connector.db)
        report_gateway = SQLReportsGateway(self.project_connector.db)
        self._data[Fields.SESSION_INFO] = session_gateway.get_session_info(self.session_uid)
        self._data[Data.VISIT_DATE] = self._data[Data.SESSION_INFO]['visit_date'].iloc[0]
        self._data[Fields.ALL_EXCLUDED_TEMPLATES], self._data[Fields.ALL_EXCLUDED_TEMPLATE_PRODUCTS], \
        self._data[Fields.ALL_EXCLUDED_PRODUCTS] = static_gateway.get_all_excluded_products()
        self._data[Fields.RELEVANT_INCLUDE_EXCLUDE_SETS] = report_gateway. \
            get_relevant_include_exclude_set(self._data[Fields.VISIT_DATE])
        self._data[Fields.SOS_INCLUDE_EXCLUDE_SET] = \
            self._get_include_exclude_set(self._data[Fields.RELEVANT_INCLUDE_EXCLUDE_SETS], 3)
        self._data[Fields.SOS_EXCLUDED_TEMPLATES] = self._data[Fields.ALL_EXCLUDED_TEMPLATES][
            self._data[Fields.ALL_EXCLUDED_TEMPLATES][Fd.EXCLUDE_INCLUDE_SET_FK] ==
            self._data[Fields.SOS_INCLUDE_EXCLUDE_SET]].copy()
        self._data[Fields.SOS_EXCLUDED_PRODUCTS] = self._data[Fields.ALL_EXCLUDED_PRODUCTS][
            self._data[Fields.ALL_EXCLUDED_PRODUCTS][Fd.EXCLUDE_INCLUDE_SET_FK] == self._data[
                Fields.SOS_INCLUDE_EXCLUDE_SET]].copy()
        self._data[Fields.SOS_EXCLUDED_TEMPLATE_PRODUCTS] = self._data[Fields.ALL_EXCLUDED_TEMPLATE_PRODUCTS][
            self._data[Fields.ALL_EXCLUDED_TEMPLATE_PRODUCTS][Fd.EXCLUDE_INCLUDE_SET_FK] == self._data[
                Fields.SOS_INCLUDE_EXCLUDE_SET]].copy()
        # For backward compatibility
        self._data[Fields.SOS_EXCLUDED_TEMPLATES].drop(Fd.EXCLUDE_INCLUDE_SET_FK, axis=1, inplace=True)
        self._data[Fields.SOS_EXCLUDED_PRODUCTS].drop(Fd.EXCLUDE_INCLUDE_SET_FK, axis=1, inplace=True)
        self._data[Fields.SOS_EXCLUDED_TEMPLATE_PRODUCTS].drop(Fd.EXCLUDE_INCLUDE_SET_FK, axis=1, inplace=True)

    @staticmethod
    def _get_include_exclude_set(relevant_include_exclude_sets, report_group_fk):
        relevant_sets = relevant_include_exclude_sets[
            relevant_include_exclude_sets['report_group_fk'] == report_group_fk]['pk']
        if relevant_sets.count() == 0:
            return None
        return relevant_sets.iloc(0)[0]
