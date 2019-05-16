import os
import pandas as pd
import KPIUtils as KPIUtils

from datetime import datetime

from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Conf.Keys import DbUsers
from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Utils.Logging.Logger import Log
from Trax.Data.Utils.MySQLservices import get_table_insertion_query as insert

from Projects.INBEVEC.Utils.Fetcher import INBEVECQueries
from Projects.INBEVEC.Utils.GeneralToolBox import INBEVECGENERALToolBox
from KPIUtils.ParseTemplates import parse_template

__author__ = 'Israel'

KPI_RESULT = 'report.kpi_results'
KPK_RESULT = 'report.kpk_results'
KPS_RESULT = 'report.kps_results'

TEMPLATE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Template.xlsx')


def log_runtime(description, log_start=False):
    def decorator(func):
        def wrapper(*args, **kwargs):
            calc_start_time = datetime.utcnow()
            if log_start:
                Log.info('{} started at {}'.format(description, calc_start_time))
            result = func(*args, **kwargs)
            calc_end_time = datetime.utcnow()
            Log.info('{} took {}'.format(description, calc_end_time - calc_start_time))
            return result

        return wrapper

    return decorator


class INBEVECToolBox:
    LEVEL1 = 1
    LEVEL2 = 2
    LEVEL3 = 3
    EXCLUDE_EMPTY = 0
    IRRELEVANT = 'Irrelevant'

    # Relative Position
    CHANNEL = 'Channel'
    LOCATION = 'Primary "In store location"'
    TESTED = 'Tested SKU2'
    ANCHOR = 'Anchor SKU2'
    ATOMIC_NAME = 'Atomic Name'
    TOP_DISTANCE = 'Up to (above) distance (by shelves)'
    BOTTOM_DISTANCE = 'Up to (below) distance (by shelves)'
    LEFT_DISTANCE = 'Up to (Left) Distance (by SKU facings)'
    RIGHT_DISTANCE = 'Up to (right) distance (by SKU facings)'

    # Block Together
    BRAND_NAME = 'Brand Name'
    SUB_BRAND_NAME = 'Brand Variant'

    # Purity
    ENTITY_TYPE = 'Entity Type To Check Purity'
    VALUE = 'Value (as Appears in DB)'

    UNLIMITED_DISTANCE = 'General'
    KPI_NAME = 'Atomic'

    def __init__(self, data_provider, output):
        self.output = output
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_channel = self.store_info['store_type'].values[0]
        if self.store_channel:
            self.store_channel = self.store_channel.upper()
        self.store_type = self.store_info['additional_attribute_1'].values[0]
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.rds_conn = PSProjectConnector(self.project_name, DbUsers.CalculationEng)
        self.tools = INBEVECGENERALToolBox(self.data_provider, self.output, rds_conn=self.rds_conn)
        self.kpi_static_data = self.get_kpi_static_data()
        self.kpi_results_queries = []
        self.relative_positioning = parse_template(TEMPLATE_PATH, 'Relative Positioning', lower_headers_row_index=2)
        self.brand_blocking = parse_template(TEMPLATE_PATH, 'Brand Blocking', lower_headers_row_index=6)
        self.purity = parse_template(TEMPLATE_PATH, 'Purity', lower_headers_row_index=3)

    def get_kpi_static_data(self):
        """
        This function extracts the static KPI data and saves it into one global data frame.
        The data is taken from static.kpi / static.atomic_kpi / static.kpi_set.
        """
        query = INBEVECQueries.get_all_kpi_data()
        kpi_static_data = pd.read_sql_query(query, self.rds_conn.db)
        return kpi_static_data

    def main_calculation(self, kpi_set_name):
        """
        This function calculates the KPI results.
        """

        if kpi_set_name in ('Relative Positioning',):
            self.calculate_relative_position_sets()
        elif kpi_set_name in ('Brand Blocking',):
            self.calculate_block_together_sets()
        elif kpi_set_name == 'Purity':
            self.calculate_purity()
        else:
            return

        set_fk = self.kpi_static_data[self.kpi_static_data['kpi_set_name'] == kpi_set_name]['kpi_set_fk'].values[0]
        self.write_to_db_result(set_fk, None, self.LEVEL1)
        self.write_to_db_result(set_fk, None, self.LEVEL2)
        return

    def calculate_relative_position_sets(self):
        """
        This function calculates every relative-position-typed KPI from the relevant sets, and returns the set final score.
        """
        for i in xrange(len(self.relative_positioning)):
            params = self.relative_positioning.iloc[i]
            if params.get('Tested SKU EAN'):
                tested_filters = {'product_ean_code': params.get('Tested SKU EAN')}
            elif params.get('Tested Brand Name'):
                tested_filters = {'brand_name': params.get('Tested Brand Name')}
            else:
                Log.warning("No tested input for atomic `{}`".format(params.get('Atomic Name')))
                continue
            if params.get('Anchor SKU EAN'):
                anchor_filters = {'product_ean_code': params.get('Anchor SKU EAN')}
            elif params.get('Anchor Brand Name'):
                anchor_filters = {'brand_name': params.get('Anchor Brand Name')}
            else:
                Log.warning("No anchor input for atomic `{}`".format(params.get('Atomic Name')))
                continue
            direction_data = {'top': self._get_direction_for_relative_position(params.get(self.TOP_DISTANCE)),
                              'bottom': self._get_direction_for_relative_position(
                                  params.get(self.BOTTOM_DISTANCE)),
                              'left': self._get_direction_for_relative_position(
                                  params.get(self.LEFT_DISTANCE)),
                              'right': self._get_direction_for_relative_position(
                                  params.get(self.RIGHT_DISTANCE))}
            general_filters = {'template_display_name': params.get(self.LOCATION)}
            result = self.tools.calculate_relative_position(tested_filters, anchor_filters, direction_data,
                                                            **general_filters)
            score = 1 if result else 0
            kpi_fk = self.get_kpi_fk_by_kpi_name(params.get(self.ATOMIC_NAME))
            self.write_to_db_result(score=score, level=self.LEVEL3, fk=kpi_fk)

    def calculate_purity(self):
        for i in xrange(len(self.purity)):
            params = self.purity.iloc[i]
            sos_filters = {params.get(self.ENTITY_TYPE): params.get(self.VALUE)}
            general_filters = {'template_display_name': params.get(self.LOCATION),
                               'product_type': ([self.IRRELEVANT, self.tools.EMPTY], self.tools.EXCLUDE_FILTER)}

            result = self.tools.calculate_share_of_shelf(sos_filters=sos_filters, **general_filters)
            score = 1 if int(result) == 1 else 0
            kpi_fk = self.get_kpi_fk_by_kpi_name(params.get(self.LOCATION))
            self.write_to_db_result(score=score, level=self.LEVEL3, fk=kpi_fk)

    def _get_direction_for_relative_position(self, value):
        """
        This function converts direction data from the template (as string) to a number.
        """
        if value == self.UNLIMITED_DISTANCE:
            value = 1000
        elif not value or not str(value).isdigit():
            value = 0
        else:
            value = int(value)
        return value

    def calculate_block_together_sets(self):
        """
        This function calculates every block-together-typed KPI from the relevant sets, and returns the set final score.
        """
        for i in xrange(len(self.brand_blocking)):
            if i == 6:
                pass
            params = self.brand_blocking.iloc[i]
            filters = {'template_display_name': params.get(self.LOCATION), 'brand_name': params.get(self.BRAND_NAME)}
            result = self.calculate_block_together(**filters)
            score = 1 if result else 0
            kpi_fk = self.get_kpi_fk_by_kpi_name(params.get(self.ATOMIC_NAME))
            self.write_to_db_result(score=score, level=self.LEVEL3, fk=kpi_fk)

    def calculate_block_together(self, allowed_products_filters=None, include_empty=None, **filters):
        """
        :param allowed_products_filters: These are the parameters which are allowed to corrupt the block without failing it.
        :param include_empty: This parameter dictates whether or not to discard Empty-typed products.
        :param filters: These are the parameters which the blocks are checked for.
        :return: True - if in (at least) one of the scenes all the relevant SKUs are grouped together in one block;
                 otherwise - returns False.
        """
        if include_empty is None:
            include_empty = self.EXCLUDE_EMPTY
        return self.tools.calculate_block_together(allowed_products_filters, include_empty, **filters)

    def get_kpi_fk_by_kpi_name(self, kpi_name):
        assert isinstance(kpi_name, unicode), "name is not a string: %r" % kpi_name
        try:
            return self.kpi_static_data[self.kpi_static_data['atomic_kpi_name'].str.encode('utf-8') ==
                                        kpi_name.encode('utf-8')]['atomic_kpi_fk'].values[0]
        except IndexError:
            Log.info(u'Kpi name: {}, isnt equal to any kpi name in static table'.format(kpi_name))
            return None

    def write_to_db_result(self, fk, score, level):
        """
        This function creates the result data frame of every KPI (atomic KPI/KPI/KPI set),
        and appends the insert SQL query into the queries' list, later to be written to the DB.
        """
        attributes = self.create_attributes_dict(fk, score, level)
        if level == self.LEVEL1:
            table = KPS_RESULT
        elif level == self.LEVEL2:
            table = KPK_RESULT
        elif level == self.LEVEL3:
            table = KPI_RESULT
        else:
            return
        query = insert(attributes, table)
        self.kpi_results_queries.append(query)

    def create_attributes_dict(self, fk, score, level):
        """
        This function creates a data frame with all attributes needed for saving in KPI results tables.

        """
        if level == self.LEVEL1:
            score_1 = format(score, '.2f') if score else None
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['kpi_set_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(kpi_set_name, self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        score_1, fk)],
                                      columns=['kps_name', 'session_uid', 'store_fk', 'visit_date', 'score_1',
                                               'kpi_set_fk'])
        elif level == self.LEVEL2:
            kpi_name = self.kpi_static_data[self.kpi_static_data['kpi_fk'] == fk]['kpi_name'].values[0]
            attributes = pd.DataFrame([(self.session_uid, self.store_id, self.visit_date.isoformat(),
                                        fk, kpi_name, score)],
                                      columns=['session_uid', 'store_fk', 'visit_date', 'kpi_fk', 'kpk_name', 'score'])
        elif level == self.LEVEL3:
            data = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]
            atomic_kpi_name = data['atomic_kpi_name'].values[0]
            kpi_fk = data['kpi_fk'].values[0]
            kpi_set_name = self.kpi_static_data[self.kpi_static_data['atomic_kpi_fk'] == fk]['kpi_set_name'].values[0]
            attributes = pd.DataFrame([(atomic_kpi_name, self.session_uid, kpi_set_name, self.store_id,
                                        self.visit_date.isoformat(), datetime.utcnow().isoformat(),
                                        score, score, kpi_fk, fk)],
                                      columns=['display_text', 'session_uid', 'kps_name', 'store_fk', 'visit_date',
                                               'calculation_time', 'score', 'result', 'kpi_fk', 'atomic_kpi_fk'])
        else:
            attributes = pd.DataFrame()
        return attributes.to_dict()

    @log_runtime('Saving to DB')
    def commit_results_data(self):
        """
        This function writes all KPI results to the DB, and commits the changes.
        """
        insert_queries = self.merge_insert_queries(self.kpi_results_queries)
        cur = self.rds_conn.db.cursor()
        delete_queries = INBEVECQueries.get_delete_session_results_query(self.session_uid)
        for query in delete_queries:
            cur.execute(query)
        for query in insert_queries:
            cur.execute(query)
        self.rds_conn.db.commit()

    @staticmethod
    def merge_insert_queries(insert_queries):
        query_groups = {}
        for query in insert_queries:
            static_data, inserted_data = query.split('VALUES ')
            if static_data not in query_groups:
                query_groups[static_data] = []
            query_groups[static_data].append(inserted_data)
        merged_queries = []
        for group in query_groups:
            merged_queries.append('{0} VALUES {1}'.format(group, ',\n'.join(query_groups[group])))
        return merged_queries
