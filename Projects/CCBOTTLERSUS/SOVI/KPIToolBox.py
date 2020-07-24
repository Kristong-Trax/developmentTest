from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from Projects.CCBOTTLERSUS.SOVI.Const import Const
import pandas as pd

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations


__author__ = 'Hunter & Nicolas'


class SOVIToolBox:

    def __init__(self, data_provider, output, common_v2):
        self.output = output
        self.data_provider = data_provider
        self.common = common_v2
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.products = self.data_provider[Data.PRODUCTS]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.visit_date = self.data_provider[Data.VISIT_DATE]
        self.session_info = self.data_provider[Data.SESSION_INFO]
        self.scene_info = self.data_provider[Data.SCENES_INFO]
        self.store_info = self.data_provider[Data.STORE_INFO]
        self.store_id = self.data_provider[Data.STORE_FK]
        self.region = self.store_info['region_name'].iloc[0]
        if self.region == Const.UNITED:
            self.manufacturer_attribute = 'United Deliver'
            self.manufacturer_value = 'Y'
        else:
            self.manufacturer_attribute = 'manufacturer_fk'
            self.manufacturer_value = Const.OWN_MANUFACTURER_FK
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.pseudo_pk = 0

    def main_calculation(self, *args, **kwargs):
        """
        This function calculates the KPI results.
        """
        self.sanitize_scif()
        self.calculate_total_kpis()

        self._calculate_sos(Const.UNITED_SSD, 'att4', denominator_id='store_id', united_numerator=True)
        self._calculate_sos(Const.UNITED_CATEGORY, 'category_fk', denominator_id='store_id',
                            context_id='att4', united_numerator=True)

        self._calculate_sos(Const.SHARE_TEMPLATE_GROUP, 'template_group', denominator_id='store_id',
                            united_numerator=True)
        self._calculate_sos(Const.SHARE_SSD_TEMPLATE_GROUP, 'att4', denominator_id='store_id',
                            context_id='template_group', united_numerator=True)
        self._calculate_sos(Const.SHARE_CATEGORY_TEMPLATE_GROUP, 'category_fk', denominator_id='store_id',
                            context_id=['att4', 'template_group'], united_numerator=True)

        self._calculate_sos(Const.MANUFACTURER_SHARE_STORE, 'manufacturer_fk', denominator_id='store_id')
        self._calculate_sos(Const.BRAND_SHARE_MANUFACTURER, 'brand_fk', denominator_id='store_id',
                            context_id='manufacturer_fk')
        self._calculate_sos(Const.PRODUCT_SHARE_MANUFACTURER, 'product_fk', denominator_id='store_id',
                            context_id=['brand_fk', 'manufacturer_fk'])

        self._calculate_sos(Const.TEMPLATE_GROUP_STORE, 'template_group', denominator_id='store_id')
        self._calculate_sos(Const.MANUFACTURER_TEMPLATE_GROUP, 'manufacturer_fk', denominator_id='store_id',
                            context_id='template_group')
        self._calculate_sos(Const.BRAND_TEMPLATE_GROUP, 'brand_fk', denominator_id='store_id',
                            context_id=['manufacturer_fk', 'template_group'])
        self._calculate_sos(Const.PRODUCT_TEMPLATE_GROUP, 'product_fk', denominator_id='store_id',
                            context_id=['brand_fk', 'manufacturer_fk', 'template_group'])

    def _calculate_sos(self, kpi_name, numerator_id, denominator_id=None, context_id=None, united_numerator=False):
        kpi_fk = self.common.get_kpi_fk_by_kpi_type(kpi_name)
        parent_fk = self._get_parent_kpi_fk_by_kpi_type(kpi_name)

        num_columns = [col for col in [numerator_id, denominator_id] if col]
        if context_id:
            if isinstance(context_id, list):
                num_columns.extend(context_id)
            else:
                num_columns.append(context_id)

        if united_numerator:
            num_df = self.scif[self.scif[self.manufacturer_attribute] == self.manufacturer_value]
        else:
            num_df = self.scif
        num_df = num_df.groupby(num_columns, as_index=False)['facings'].sum()
        num_df.rename(columns={'facings': 'numerator'}, inplace=True)

        den_columns = [col for col in [denominator_id] if col]
        den_df = self.scif.groupby(den_columns, as_index=False)['facings'].sum()
        den_df.rename(columns={'facings': 'denominator'}, inplace=True)

        results_df = pd.merge(den_df, num_df, how='left', on=den_columns).fillna(0)

        results_df['sos'] = results_df['numerator'] / results_df['denominator']

        if 'template_group' in results_df.columns.tolist():
            results_df = pd.merge(results_df,
                                  self.scif[['template_fk',
                                             'template_group']].drop_duplicates(subset=['template_group']),
                                  how='inner', on='template_group')
            results_df['template_group'] = results_df['template_fk']

        if 'att4' in results_df.columns.tolist():
            results_df['att4'] = results_df['att4'].apply(lambda x: Const.STILL_FK if x == 'Still' else Const.SSD_FK)
        # print('{}: {}'.format(kpi_name, len(results_df)))
        for result in results_df.itertuples():
            identifier_result = {'kpi_fk': kpi_fk, numerator_id: getattr(result, numerator_id),
                                 denominator_id: getattr(result, denominator_id)}
            identifier_result.pop('store_id', None)
            identifier_parent = {'kpi_fk': parent_fk, denominator_id: getattr(result, denominator_id)}
            context_id_for_db = None
            if context_id:
                if isinstance(context_id, list):
                    for attr_name in context_id:
                        identifier_parent.update({attr_name: getattr(result, attr_name)})
                        identifier_result.update({attr_name: getattr(result, attr_name)})
                    context_id_for_db = getattr(result, str(context_id[0]), None)
                else:
                    identifier_parent.update({context_id: getattr(result, context_id)})
                    identifier_result.update({context_id: getattr(result, context_id)})
                    context_id_for_db = getattr(result, str(context_id), None)
            identifier_parent.pop('store_id', None)
            self.common.write_to_db_result(kpi_fk, numerator_id=getattr(result, numerator_id),
                                           denominator_id=getattr(result, denominator_id),
                                           context_id=context_id_for_db,
                                           numerator_result=getattr(result, 'numerator', 0),
                                           denominator_result=getattr(result, 'denominator', 0),
                                           result=result.sos * 100, identifier_parent=identifier_parent,
                                           identifier_result=identifier_result, should_enter=True)

    def _get_parent_kpi_fk_by_kpi_type(self, kpi_type):
        try:
            return self.common.get_kpi_fk_by_kpi_type(Const.HIERARCHY[kpi_type])
        except:
            Log.error('No parent found for {}'.format(kpi_type))
            return 0

    def calculate_total_kpis(self):

        united_df = self.scif[self.scif[self.manufacturer_attribute] == self.manufacturer_value]

        numerator_result = united_df.facings.sum()
        denominator_result = self.scif.facings.sum()

        sos_value = self.calculate_percentage_from_numerator_denominator(numerator_result, denominator_result)

        total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TOTAL)
        united_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.UNITED_TOTAL)
        united_by_template_group_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.UNITED_TEMPLATE_GROUP)
        manufacturer_total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.MANUFACTURER_TOTAL_STORE)
        manufacturer_template_total_kpi_fk = self.common.get_kpi_fk_by_kpi_type(Const.TEMPLATE_GROUP_MANUFACTURER)

        total_identifier = {'kpi_fk': total_kpi_fk}
        self.common.write_to_db_result(total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=numerator_result, result=sos_value,
                                       identifier_result=total_identifier,
                                       should_enter=True, denominator_id=self.store_id,
                                       denominator_result=denominator_result)
        self.common.write_to_db_result(united_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=numerator_result,
                                       result=sos_value, identifier_result={'kpi_fk': united_kpi_fk},
                                       identifier_parent=total_identifier, should_enter=True,
                                       denominator_id=self.store_id, denominator_result=denominator_result)
        self.common.write_to_db_result(united_by_template_group_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=numerator_result,
                                       result=sos_value, identifier_result={'kpi_fk': united_by_template_group_kpi_fk},
                                       identifier_parent=total_identifier, should_enter=True,
                                       denominator_id=self.store_id, denominator_result=denominator_result)
        self.common.write_to_db_result(manufacturer_total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=1,
                                       result=100, identifier_result={'kpi_fk': manufacturer_total_kpi_fk},
                                       identifier_parent=total_identifier, should_enter=True,
                                       denominator_id=self.store_id, denominator_result=1)
        self.common.write_to_db_result(manufacturer_template_total_kpi_fk, numerator_id=Const.OWN_MANUFACTURER_FK,
                                       numerator_result=1,
                                       result=100, identifier_result={'kpi_fk': manufacturer_template_total_kpi_fk},
                                       identifier_parent=total_identifier, should_enter=True,
                                       denominator_id=self.store_id, denominator_result=1)
        return

    def sanitize_scif(self):
        excluded_types = ['Empty', 'Irrelevant']
        self.scif = self.scif[~(self.scif['product_type'].isin(excluded_types)) &
                              ~(self.scif['brand_name'].isin(Const.EXCLUDED_BRANDS)) &
                              (self.scif['facings'] != 0)]

    @staticmethod
    def calculate_percentage_from_numerator_denominator(numerator_result, denominator_result):
        try:
            ratio = numerator_result / denominator_result
        except Exception as e:
            Log.error(e.message)
            ratio = 0
        if not isinstance(ratio, (float, int)):
            ratio = 0
        return round(ratio * 100, 2)

    def commit_results(self):
        pass
        # self.common_v2.commit_results_data()

    def commit_results_without_delete(self):
        self.common.commit_results_data_without_delete_version2()
