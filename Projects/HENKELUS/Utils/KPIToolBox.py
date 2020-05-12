from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Utils.Logging.Logger import Log
from KPIUtils_v2.Utils.GlobalScripts.Scripts import GlobalSessionToolBox
import pandas as pd
from Projects.HENKELUS.Utils.HenkelDataProvider import HenkelDataProvider
from Projects.HENKELUS.Data.LocalConsts import Consts
from KPIUtils_v2.Calculations.BlockCalculations_v2 import Block
from KPIUtils_v2.Calculations.CalculationsUtils.GENERALToolBoxCalculations import GENERALToolBox
from collections import Counter

# from KPIUtils_v2.Utils.Consts.DataProvider import 
# from KPIUtils_v2.Utils.Consts.DB import 
# from KPIUtils_v2.Utils.Consts.PS import 
# from KPIUtils_v2.Utils.Consts.GlobalConsts import 
# from KPIUtils_v2.Utils.Consts.Messages import 
# from KPIUtils_v2.Utils.Consts.Custom import 
# from KPIUtils_v2.Utils.Consts.OldDB import 

# from KPIUtils_v2.Calculations.AssortmentCalculations import Assortment
# from KPIUtils_v2.Calculations.AvailabilityCalculations import Availability
# from KPIUtils_v2.Calculations.NumberOfScenesCalculations import NumberOfScenes
# from KPIUtils_v2.Calculations.PositionGraphsCalculations import PositionGraphs
# from KPIUtils_v2.Calculations.SOSCalculations import SOS
# from KPIUtils_v2.Calculations.SequenceCalculations import Sequence
# from KPIUtils_v2.Calculations.SurveyCalculations import Survey

# from KPIUtils_v2.Calculations.CalculationsUtils import GENERALToolBoxCalculations

__author__ = 'nicolaske'


class ToolBox(GlobalSessionToolBox):
    EXCLUDE_FILTER = 0
    INCLUDE_FILTER = 1
    CONTAIN_FILTER = 2
    EXCLUDE_EMPTY = False
    INCLUDE_EMPTY = True

    STRICT_MODE = ALL = 1000

    EMPTY = 'Empty'
    DEFAULT = 'Default'
    TOP = 'Top'
    BOTTOM = 'Bottom'

    def __init__(self, data_provider, output):
        GlobalSessionToolBox.__init__(self, data_provider, output)
        self.kpi_template_path = Consts.KPI_TEMPLATE_PATH
        self.kpi_template = self.parse_template(self.kpi_template_path, Consts.relevant_kpi_sheets)
        self.manufacturer_fk = Consts.OWN_MANUFACTURER_FK
        self.hdp = HenkelDataProvider(self.data_provider)
        self.block = Block(self.data_provider)
        self.toolbox = GENERALToolBox(self.data_provider)
        self.mpis = pd.merge(self.all_products, self.matches, how='right', on='product_fk')
        self.ignore_stacking = False
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'

    def main_calculation(self):
        self.calculate_sku_count()
        self.calculate_facing_count()
        self.calculate_smart_tags()

        self.calculate_base_measurement()
        self.calculate_liner_measure()

        self.calculate_horizontal_shelf_position()
        self.calculate_vertical_shelf_position()
        self.calculate_blocking()
        # self.calculate_blocking_orientation()

        score = 0
        return score

    def calculate_vertical_shelf_position(self):
        template = self.kpi_template[Consts.VERTICAL_SHELF_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']
            sub_category_value = row['sub_category']
            shelf_map_df = self.kpi_template[Consts.SHELF_MAP_SHEET]

            mpis = pd.merge(self.all_products, self.matches, how='right', on='product_fk')

            if not pd.isna(sub_category_value):
                mpis = mpis[mpis['sub_category'] == sub_category_value]

            for value1 in mpis[Params1].unique().tolist():
                filtered_mpis = mpis[mpis[Params1] == value1]

                if not pd.isna(Params2):
                    for value2 in mpis[Params2].unique().tolist():
                        filtered_mpis = filtered_mpis[filtered_mpis[Params2] == value2]
                        self.calculate_position(kpi_fk=kpi_fk, mpis=mpis, filtered_mpis=filtered_mpis,
                                                shelf_map_df=shelf_map_df,
                                                param1=Params1)

                else:
                    self.calculate_position(kpi_fk=kpi_fk, mpis=mpis, filtered_mpis=filtered_mpis,
                                            shelf_map_df=shelf_map_df, param1=Params1)

    def calculate_position(self, kpi_fk=None, mpis=None, filtered_mpis=None, shelf_map_df=None, param1=None):
        shelf_positions = []
        try:
            product_fk = filtered_mpis.product_fk.iloc[0]
        except:
            product_fk = -1

        if param1 == 'product_fk':
            denom_id = self.store_id
        else:
            denom_id = self.manufacturer_fk

        for scene in list(filtered_mpis.scene_fk.unique()):
            filtered_mpis = filtered_mpis[filtered_mpis['scene_fk'] == scene]
            shelf_count = len(mpis['shelf_number'][mpis['scene_fk'] == scene].unique())
            for i, row in filtered_mpis.iterrows():
                shelf_number = row['shelf_number']
                pos = shelf_map_df[shelf_number][shelf_map_df['Num Shelves'] == shelf_count]
                shelf_positions.append(pos.iloc[0])

        if len(shelf_positions) == 0:
            pass
        else:

            mode = max(shelf_positions, key=shelf_positions.count)

            result = Consts.VERTICAL_SHELF_POS_DICT[mode]

            self.write_to_db(fk=kpi_fk,
                             numerator_id=product_fk,
                             numerator_result=1,
                             context_id=product_fk,
                             denominator_id=denom_id, result=result)

    def calculate_horizontal_shelf_position(self):
        template = self.kpi_template[Consts.HORIZONTAL_SHELF_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            mpis = self.mpis

            for value1 in mpis[Params1].unique().tolist():
                shelf_positions = []
                filtered_mpis = mpis[mpis[Params1] == value1]
                product_fk = filtered_mpis.product_fk.iloc[0]

                for scene in list(filtered_mpis.scene_fk.unique()):
                    filtered_mpis = mpis[mpis['scene_fk'] == scene]
                    bay_count = len(filtered_mpis.bay_number.unique())

                    if bay_count == 1:
                        pos = 'Center'
                    else:
                        factor_bay = round(bay_count / float(3))
                        for i, row in filtered_mpis.iterrows():
                            sku_bay_number = row.bay_number

                            if sku_bay_number <= factor_bay:
                                pos = 'Left'
                            elif sku_bay_number > (bay_count - factor_bay):
                                pos = 'Right'

                    shelf_positions.append(pos)

                mode = max(shelf_positions, key=shelf_positions.count)
                if not pd.isna(mode):
                    result = Consts.HORIZONTAL_SHELF_POS_DICT[mode]
                    self.write_to_db(fk=kpi_fk,
                                     numerator_id=product_fk,
                                     numerator_result=1,
                                     denominator_id=self.store_id, result=result)




    def calculate_blocking_orientation(self):
        template = self.kpi_template[Consts.BASE_MEASURE_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']

        pass

    def calculate_blocking(self):
        template = self.kpi_template[Consts.BLOCKING_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Value1 = row['VALUE 1']
            Params2 = row['PARAM 2']
            VALUE2 = row['VALUE 2']
            minimum_block_ratio = row['minimum_ratio']

            Exlude_Params1 = row['EXCLUDE PARAM 1']
            Exclude_Value1 = self.sanitize_row(row['EXCLUDE VALUE 1'])

            Exlude_Params2 = row['EXCLUDE PARAM 2']
            Exclude_Value2 = self.sanitize_row(row['EXCLUDE VALUE 2'])

            Block_AllowConnected_Params1 = row['BLOCK ALLOW-CONNECTED PARAM 1']
            Block_AllowConnected_Value1 = self.sanitize_row(row['BLOCK ALLOW-CONNECTED VALUE 1'])

            Block_AllowConnected_Params2 = row['BLOCK ALLOW-CONNECTED PARAM 2']
            Block_AllowConnected_Value2 = self.sanitize_row(row['BLOCK ALLOW-CONNECTED VALUE 2'])

            Block_AllowConnected_Params3 = row['BLOCK ALLOW-CONNECTED PARAM 3']
            Block_AllowConnected_Value3 = self.sanitize_row(row['BLOCK ALLOW-CONNECTED VALUE 3'])

            Block_Exlude_Params1 = row['BLOCK EXCLUDE PARAM 1']
            Block_Exclude_Value1 = self.sanitize_row(row['BLOCK EXCLUDE VALUE 1'])



            # is_aggregate = row['aggregate']
            #
            # if is_aggregate == 'yes':
            #     pass

            connect_dict = {}
            excluded_dict = {}
            smart_attribute_data_df = \
                self.hdp.get_match_product_in_probe_state_values(self.matches['probe_match_fk'].unique().tolist())

            if Block_AllowConnected_Params1:
                connect_dict.update({Block_AllowConnected_Params1: Block_AllowConnected_Value1})
            if Block_AllowConnected_Params2:
                connect_dict.update({Block_AllowConnected_Params2: Block_AllowConnected_Value2})
            if Block_AllowConnected_Params3:
                connect_dict.update({Block_AllowConnected_Params3: Block_AllowConnected_Value3})


            connected_product_pks = []
            for key in connect_dict.keys():
                if key == 'Smart Tag':
                    product_fks = smart_attribute_data_df.product_fk.tolist()
                    connected_product_pks.extend(product_fks)
                elif not pd.isna(key):
                    filtered_dict = self.scif[self.scif[key].isin(connect_dict[key])]

                    if not filtered_dict.empty:
                        connected_product_pks.extend(filtered_dict.product_fk.tolist())




            result = 0

            general_filters = {}

            param_dict = {Params1: [Value1], Params2: [VALUE2]}
            excluded_dict = {Exlude_Params1: Exclude_Value1, Exlude_Params2: Exclude_Value2,
                             Block_Exlude_Params1: Block_Exlude_Params1 }


            # for param_key in param_dict.keys():
            #     if not pd.isna(param_key):
            #         general_filters[param_key] = param_dict[param_key]

            general_filters = self.remove_nans_dict(param_dict)
            excluded_filters = self.remove_nans_dict(excluded_dict)



            block_result = self.block.network_x_block_together(
                population=general_filters,
                additional={'minimum_block_ratio': minimum_block_ratio,
                            'minimum_facing_for_block': 1,
                            'allowed_products_filters': {'product_type': connected_product_pks},
                            'include_stacking': True,
                            'exclude_filter': excluded_filters,
                            'check_vertical_horizontal': False})

            if not block_result.empty:
                result = 1

            self.write_to_db(fk=kpi_fk, numerator_id=self.manufacturer_fk, numerator_result=result,
                             denominator_id=self.store_id, result=result)

    def calculate_liner_measure(self):
        template = self.kpi_template[Consts.LINEAR_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']

            parent_brand_list = self.remove_type_none_from_list(self.scif[Params1].unique().tolist())
            relevant_scif = self.scif[self.scif[Params1].isin(parent_brand_list)]
            matches_no_stacking = self.matches[self.matches['stacking_layer'] == 1]
            mpis = pd.merge(relevant_scif, matches_no_stacking, how='left', on='product_fk')

            for parent_brand in parent_brand_list:
                for format in mpis[Params2][mpis[Params1] == parent_brand].unique().tolist():
                    if not pd.isna(format):
                        relevant_mpis = mpis[(mpis[Params2] == format) & (mpis[Params1] == parent_brand)]

                        linear_per_format_sum_mm = relevant_mpis['width_mm_advance'].sum()
                        product_fk = relevant_mpis.product_fk.iloc[0]

                        linear_per_format_sum_ft = linear_per_format_sum_mm * float(0.00328)
                        self.write_to_db(fk=kpi_fk,
                                         numerator_id=product_fk, numerator_result=linear_per_format_sum_mm,
                                         context_id=product_fk,
                                         denominator_id=self.manufacturer_fk, result=linear_per_format_sum_ft)

    def calculate_base_measurement(self):
        template = self.kpi_template[Consts.BASE_MEASURE_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']

            sub_category_list = self.remove_type_none_from_list(self.scif.sub_category_fk.unique().tolist())
            mpis = pd.merge(self.all_products, self.matches, how='right', on='product_fk')
            for sub_category_fk in sub_category_list:
                relevant_mpis = mpis[(mpis[Params1] == sub_category_fk) & (mpis['stacking_layer'] == 1)]
                bay_measure = []
                bay_mm_total = 0
                bays = relevant_mpis.bay_number.unique().tolist()
                for bay in bays:
                    shelves_in_bay = len(relevant_mpis['shelf_number'][relevant_mpis['bay_number'] == bay].unique())
                    mpis_by_bay_df = relevant_mpis[relevant_mpis['bay_number'] == bay]
                    bay_sum_width_mm = mpis_by_bay_df['width_mm_advance'].sum()
                    bay_avg = bay_sum_width_mm / float(shelves_in_bay)
                    bay_measure.append(bay_avg)
                    bay_mm_total += bay_avg
                bay_feet = bay_mm_total * 0.00328
                self.write_to_db(fk=kpi_fk,
                                 numerator_id=sub_category_fk, numerator_result=bay_mm_total,
                                 denominator_id=self.store_id, result=bay_feet)

    def calculate_smart_tags(self):
        template = self.kpi_template[Consts.SMART_TAG_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Value1 = row['VALUE 1']

            relevant_mpis = self.matches

            smart_attribute_data_df = \
                self.hdp.get_match_product_in_probe_state_values(relevant_mpis['probe_match_fk'].unique().tolist())

            result = 0

            smart_tags_df = pd.DataFrame()
            try:
                smart_tags_df = smart_attribute_data_df[smart_attribute_data_df[Params1] == Value1]
            except:
                pass

            if not smart_tags_df.empty:
                result = 1

            self.write_to_db(fk=kpi_fk,
                             numerator_id=self.manufacturer_fk, numerator_result=result,
                             denominator_id=self.store_id, result=result)

    def calculate_sku_count(self):
        template = self.kpi_template[Consts.SKU_COUNT_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']

            param1_list = self.remove_type_none_from_list(list(self.scif[Params1].unique()))
            param2_list = self.remove_type_none_from_list(list(self.scif[Params2].unique()))
            for value1 in param1_list:
                filtered_scif_df = self.scif[self.scif[Params1] == value1]
                for value2 in param2_list:
                    reduce_scif_df = filtered_scif_df[filtered_scif_df[Params2] == value2]
                    if not reduce_scif_df.empty:
                        product_fk = reduce_scif_df['product_fk'].iloc[0]
                        result = len(reduce_scif_df['product_fk'].unique())
                        manufacturer_fk = reduce_scif_df['manufacturer_fk'].iloc[0]
                        self.write_to_db(fk=kpi_fk,
                                         numerator_id=product_fk, numerator_result=result,
                                         denominator_id=manufacturer_fk,
                                         context_id=product_fk, result=result)

    def calculate_facing_count(self):
        template = self.kpi_template[Consts.FACING_COUNT_SHEET]
        for i, row in template.iterrows():
            kpi_name = row['KPI Name'].strip()
            kpi_fk = self.get_kpi_fk_by_kpi_type(kpi_name)
            Params1 = row['PARAM 1']
            Params2 = row['PARAM 2']

            param1_list = self.remove_type_none_from_list(list(self.scif[Params1].unique()))
            param2_list = self.remove_type_none_from_list(list(self.scif[Params2].unique()))
            for value1 in param1_list:
                filtered_scif_df = self.scif[self.scif[Params1] == value1]
                for value2 in param2_list:
                    reduce_scif_df = filtered_scif_df[filtered_scif_df[Params2] == value2]
                    if not reduce_scif_df.empty:
                        product_fk = reduce_scif_df['product_fk'].iloc[0]
                        result = reduce_scif_df['facings_ign_stack'].sum()
                        manufacturer_fk = reduce_scif_df['manufacturer_fk'].iloc[0]
                        self.write_to_db(fk=kpi_fk,
                                         numerator_id=product_fk, numerator_result=result,
                                         denominator_id=manufacturer_fk,
                                         context_id=product_fk, result=result)

    def remove_type_none_from_list(self, orig_list):
        for item in orig_list:
            if pd.isna(item):
                try:
                    orig_list.remove(item)
                except:
                    pass
        fixed_list = orig_list
        return fixed_list

    def parse_template(self, template_path, relevant_sheets_list):
        template = {}
        if len(relevant_sheets_list) > 0:
            for sheet in relevant_sheets_list:
                template[sheet] = pd.read_excel(template_path, sheetname=sheet, encoding='utf8')

        return template

    def get_filter_condition(self, df, **filters):
        """
        :param df: The data frame to be filters.
        :param filters: These are the parameters which the data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts data frame.
        """
        if not filters:
            return df
        if self.facings_field in df.keys():
            filter_condition = (df[self.facings_field] > 0)
        else:
            filter_condition = None
        for field in filters.keys():
            if field in df.keys():
                if isinstance(filters[field], tuple):
                    value, exclude_or_include = filters[field]
                else:
                    value, exclude_or_include = filters[field], self.INCLUDE_FILTER
                if not value:
                    continue
                if not isinstance(value, list):
                    value = [value]
                if exclude_or_include == self.INCLUDE_FILTER:
                    condition = (df[field].isin(value))
                elif exclude_or_include == self.EXCLUDE_FILTER:
                    condition = (~df[field].isin(value))
                elif exclude_or_include == self.CONTAIN_FILTER:
                    condition = (df[field].str.contains(value[0], regex=False))
                    for v in value[1:]:
                        condition |= df[field].str.contains(v, regex=False)
                else:
                    continue
                if filter_condition is None:
                    filter_condition = condition
                else:
                    filter_condition &= condition
            else:
                Log.warning('field {} is not in the Data Frame'.format(field))

        return filter_condition

    def sanitize_row(self, row):

        if type(row) == unicode:
            row = row.encode()
            items = row.split(",")
            cleansed_items = [s.strip() if type(s) == str else s for s in items]
            return cleansed_items
        else:
            return [row]


    def remove_nans_dict(self, input_dict):
        filtered_dict = {}
        for param_key in input_dict.keys():
            if not pd.isna(param_key):
                filtered_dict[param_key] = input_dict[param_key]
        return filtered_dict



