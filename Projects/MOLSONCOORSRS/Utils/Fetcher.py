
__author__ = 'sergey'


class MOLSONCOORSRSQueries(object):

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

    @staticmethod
    def get_result_values():
        return \
            """
            SELECT 
            rt.pk AS result_type_fk,
            rt.name AS result_type, 
            rv.pk AS result_value_fk, 
            rv.value AS result_value
            FROM static.kpi_result_value rv
            JOIN static.kpi_result_type rt ON rt.pk=rv.kpi_result_type_fk;
            """
