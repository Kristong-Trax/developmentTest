
__author__ = 'sergey'


class Queries(object):

    @staticmethod
    def get_sos_store_policies(visit_date):
        return \
            """
            SELECT ktp.*, p.policy AS store_policy 
            FROM ( SELECT kt.kpi AS kpi_fk, kt.sku_name, kt.target,
                kt.target_validity_start_date, kt.target_validity_end_date, 
                kt.store_policy_fk, p.policy AS sos_policy 
                FROM pservice.kpi_targets kt
                JOIN pservice.policy p ON p.pk = kt.store_policy_fk
                AND (kt.target_validity_start_date IS NULL OR '{visit_date}' >= kt.target_validity_start_date)
                AND (kt.target_validity_end_date IS NULL OR '{visit_date}' <= kt.target_validity_end_date)
            ) AS ktp
            JOIN pservice.policy p ON p.policy_name = ktp.sku_name;
            """.format(visit_date=visit_date)
