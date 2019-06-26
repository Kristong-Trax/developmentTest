from KPIUtils_v2.DB.PsProjectConnector import PSProjectConnector
from Trax.Cloud.Services.Connector.Keys import DbUsers
from Trax.Utils.Logging.Logger import Log


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
            ['product_fk', 'bay_number', 'shelf_number', 'facing_sequence_number', 'creation_time', 'shelf_number_from_bottom']]
        self.find_prev_products_and_write_to_db(empty_matches)

        Log.info('Commiting the results of COOLERSCREENSUS kpi')
        self._common.commit_results_data_to_new_tables()
        Log.info('Commited the results of COOLERSCREENSUS kpi NEW VERSION')

    def find_prev_products_and_write_to_db(self, empty_matches):
        project_connector = PSProjectConnector(self._data_provider.project_name, DbUsers.CalculationEng)
        for _, match in empty_matches.iterrows():
            prev_product_id = self._find_prev_product(project_connector, match)
            if prev_product_id is not None:
                kpi_result = 100 if len(self._data_provider.matches[self._data_provider.matches['product_fk'] == prev_product_id]) == 0 else 101
                shelf_letter = self._get_shelf_letter_by_shelf_number(project_connector, match['shelf_number_from_bottom'])
                Log.info('Calculated COOLERSCREENSUS kpi for product {} the result is {}'.format(prev_product_id, kpi_result))
                self._common.write_to_db_result_new_tables(fk=10000,
                                                           numerator_id=prev_product_id,
                                                           numerator_result=kpi_result,
                                                           score=shelf_letter,
                                                           denominator_result=match['facing_sequence_number'],
                                                           result=kpi_result)

    def _find_prev_product(self, project_connector, match):
        cur = project_connector.execute("""
        SELECT product_fk
        FROM probedata.match_product_in_scene mpis JOIN probedata.scene sc on sc.pk = mpis.scene_fk
        JOIN static_new.product pr on pr.pk = mpis.product_fk
        WHERE sc.creation_time < %(creation_time)s AND bay_number = %(bay_number)s AND shelf_number = %(shelf_number)s
            AND facing_sequence_number = %(facing_sequence_number)s AND product_fk <> %(product_fk)s AND 
            sc.store_fk = %(store_fk)s AND sc.template_fk =%(template_fk)s
            AND pr.type = 'SKU'
        ORDER BY sc.creation_time DESC
        LIMIT 1
        """, {'product_fk': match['product_fk'], 'bay_number': match['bay_number'],
              'shelf_number': match['shelf_number'],
              'facing_sequence_number': match['facing_sequence_number'], 'creation_time': match['creation_time'],
              'store_fk': self._data_provider.store_fk, 'template_fk': int(self._data_provider.templates['template_fk'])})

        for row in cur:
            return row[0]

        return None

    def _get_shelf_letter_by_shelf_number(self, project_connector, shelf_number):
        letter = chr(shelf_number + 64)
        return self._get_pk_letter_from_kpi_score_value(project_connector, letter)

    @staticmethod
    def _get_pk_letter_from_kpi_score_value(project_connector, letter):
        cur = project_connector.execute("""
                SELECT KSV.pk
                FROM static.kpi_score_value KSV
                JOIN static.kpi_score_type KST ON KSV.kpi_score_type_fk = KST.pk
                WHERE KST.name = 'SHELF_LETTER'
                    AND KSV.value = '{letter}'
                LIMIT 1;
                """.format(letter=letter))

        for row in cur:
            return row[0]

        return None
