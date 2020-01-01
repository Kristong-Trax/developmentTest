from Trax.Algo.Calculations.Core.KPI.UnifiedKPICalculation import UnifiedCalculationsScript
from Projects.DIAGEORU.KPIs.util import DiageoUtil
from KPIUtils_v2.Utils.Consts.DataProvider import ProductsConsts, ScifConsts
from KPIUtils.GlobalProjects.DIAGEO.Utils.Consts import DiageoKpiNames
from Projects.DIAGEORU.KPIs.util import DiageoConsts
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block


class BrandBlockSKU(UnifiedCalculationsScript):

    def __init__(self, data_provider, config_params=None, **kwargs):
        super(BrandBlockSKU, self).__init__(data_provider, config_params=config_params, **kwargs)
        self.util = DiageoUtil(data_provider)

    def kpi_type(self):

        return DiageoKpiNames.BRAND_BLOCKING

    def _get_calculation_param_and_value(self, kpi_name, params):
        """
        this method extracts param and value to from kpi_name and template data
        :param kpi_name: kpi name
        :param params: template data
        :return: param and value to filter by
        """
        entity_type = self.util.common.get_entity_type_by_kpi_name(kpi_name)
        if entity_type == 'manufacturer':
            param = ProductsConsts.MANUFACTURER_NAME
            value = DiageoConsts.DIAGEO
        elif entity_type == 'brand':
            param = ProductsConsts.BRAND_NAME
            value = params.get(DiageoConsts.BRAND_NAME_BT)
        else:
            param = DiageoConsts.SUB_BRAND_NAME
            value = params.get(DiageoConsts.BRAND_VARIANT)
        return param, value

    def get_numerator_id_block_together(self, extra_filter, params):
        """
        this method finds id for numerator in block together KPI
        :param extra_filter: type to find id for
        :param params: params the KPI is calculated by
        :return: numerator id if exists, -1 otherwise
        """
        numerator_id = -1
        try:
            if extra_filter == 'brand_name':
                numerator_df = self.util.scif[self.util.scif.brand_name == params.get(DiageoConsts.BRAND_NAME_BT)]
                if not numerator_df.empty:
                    numerator_id = numerator_df.brand_fk.values[0]
            elif extra_filter == ProductsConsts.MANUFACTURER_NAME:
                numerator_df = self.util.scif[self.util.scif.manufacturer_name == DiageoConsts.DIAGEO]
                if not numerator_df.empty:
                    numerator_id = numerator_df.manufacturer_fk.values[0]
            else:
                numerator_df = self.util.sub_brands[self.util.sub_brands.name == params.get(DiageoConsts.BRAND_VARIANT)]
                if not numerator_df.empty:
                    numerator_id = numerator_df.pk.values[0]
            return numerator_id
        except Exception as e:
            self.util.Log.error(str(e))
            return -1

    def _get_template_fk_by_scene_type(self, scene_type):
        """
        :param scene_type: this needs to be an instance of template_name from scif (or a derivative)
        :return: template_fk
        """
        filtered_scif = self.util.templates[self.util.templates['template_name'].str.encode('utf8') ==
                                            scene_type.encode('utf8')]
        if filtered_scif.empty:
            self.util.Log.debug(DiageoConsts.WRONG_TEMPLATE_NAME_IN_TEMPLATE_LOG.format(scene_type))
            return None
        return filtered_scif.template_fk.values[0]

    def _get_relevant_template_fk(self, params):
        """
        This method gets the KPI params and returns the relevant template_fk if exists, else, returns None.
        """
        template_fk = None
        if DiageoConsts.LOCATION in params.keys():
            template_fk = self._get_template_fk_by_scene_type(params[DiageoConsts.LOCATION])
        return template_fk

    def calculate(self):
        """
        this class calculates all the menu KPI's depending on the parameter given
        """
        kpi_name = DiageoKpiNames.BRAND_BLOCKING + ' - SKU'
        template_data = self.util.template_handler.download_template(DiageoKpiNames.BRAND_BLOCKING)
        specific_params = self._config_params
        ratio = 1
        ignore_empty = False
        if specific_params:
            if 'minimum_block_ratio' in specific_params.keys():
                ratio = specific_params['minimum_block_ratio']
            if 'ignore_empty' in specific_params.keys():
                ignore_empty = specific_params['ignore_empty']
        res_list = []
        network_x = Block(data_provider=self.util.data_provider, output=self.util.output,
                          ps_data_provider=self.util.ps_data_provider, rds_conn=self.util.rds_conn)

        for params in template_data:
            calculation_param, calculation_value = self._get_calculation_param_and_value(kpi_name, params)
            # build filters
            location = {'template_name': params.get(DiageoConsts.LOCATION)}
            population = {calculation_param: [calculation_value]}
            additional = {'calculate_all_scenes': False, 'minimum_block_ratio': ratio, 'ignore_empty': ignore_empty,
                          'minimum_facing_for_block': 2}
            # calculate result
            result_df = network_x.network_x_block_together(population=population, location=location,
                                                           additional=additional)
            result = result_df['is_block'].iloc[0] if not result_df.empty else False
            sku_result = 1 if result else 0
            # Save it to the old tables
            # if 'Atomic' in params.keys():
            #     old_tables_kpi_name = params['Atomic']
            #     self.util._write_results_to_old_tables(old_tables_kpi_name, sku_result, 2)

            # get numerator id
            numerator_id = self.get_numerator_id_block_together(calculation_param, params)

            # build dict and insert to list, if it's not already there
            # if numerator_id not in already_inserted_list and numerator_id > 0:
            if numerator_id > 0:
                template_fk = self._get_relevant_template_fk(params)
                res_list.append(self.util.build_dictionary_for_db_insert_v2(
                    kpi_name=kpi_name, result=sku_result, score=sku_result,
                    numerator_id=numerator_id, denominator_id=template_fk, identifier_parent=kpi_name))

        for res in res_list:
            self.write_to_db_result(**res)
