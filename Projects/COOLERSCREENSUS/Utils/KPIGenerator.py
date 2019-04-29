from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers


class COOLERSCREENSUSKGenerator:
    def __init__(self, data_provider, output, common):
        """

        :param data_provider:
        :type: Trax.Algo.Calculations.Core.DataProvider
        :param output:
        :param common:
        :type common: KPIUtils.DB.Common.Common
        """
        self._data_provider = data_provider
        self._output = output
        self._common = common

    def main_function(self):
        matches_with_products = self._data_provider.matches.merge(right=self._data_provider.products, on='product_fk')
        empty_matches = matches_with_products[matches_with_products['product_type'] == 'Empty']
        empty_matches = empty_matches[
            ['product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number', 'creation_time']]
        project_connector = PSProjectConnector(self._data_provider.project_name, DbUsers.CalculationEng)
        for _, match in empty_matches.iterrows():
            prev_product_id = self._find_prev_product(project_connector, match)
            if prev_product_id is not None:
                kpi_result = 0 if len(self._data_provider.matches[self._data_provider.matches['product_fk'] == prev_product_id]) == 0 else 1
                self._common.write_to_db_result_new_tables(fk=10000,
                                                           numerator_id=prev_product_id,
                                                           numerator_result=kpi_result,
                                                           result=kpi_result)

        self._common.commit_results_data_to_new_tables()

    def _find_prev_product(self, project_connector, match):
        cur = project_connector.execute("""
        SELECT product_fk
        FROM probedata.match_product_in_scene
        WHERE creation_time < %(creation_time)s AND bay_number = %(bay_number)s AND shelf_number = %(shelf_number)s
            AND facing_sequence_number = %(facing_sequence_number)s AND product_fk <> %(product_fk)s 
        ORDER BY creation_time DESC
        LIMIT 1
        """, {'product_fk': match['product_fk'], 'bay_number': match['bay_number'],
              'shelf_number': match['shelf_number'],
              'facing_sequence_number': match['facing_sequence_number'], 'creation_time': match['creation_time']})

        for row in cur:
            return row[0]

        return None
