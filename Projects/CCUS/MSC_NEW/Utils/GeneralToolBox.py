import pandas as pd
from Trax.Algo.Calculations.Core.DataProvider import Data
from Trax.Algo.Calculations.Core.Shortcuts import BaseCalculationsGroup
from Trax.Utils.Logging.Logger import Log
from Projects.CCUS.MSC_NEW.Utils.Fetcher import MSC_NEWQueries

__author__ = 'Ortal'


class MSC_NEWGENERALToolBox:

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

    def __init__(self, data_provider, output, rds_conn=None, ignore_stacking=False, front_facing=False, **kwargs):
        self.k_engine = BaseCalculationsGroup(data_provider, output)
        self.rds_conn = rds_conn
        self.data_provider = data_provider
        self.project_name = self.data_provider.project_name
        self.session_uid = self.data_provider.session_uid
        self.scif = self.data_provider[Data.SCENE_ITEM_FACTS]
        self.match_product_in_scene = self.data_provider[Data.MATCHES]
        self.all_products = self.data_provider[Data.ALL_PRODUCTS]
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.scenes_info = self.data_provider[Data.SCENES_INFO].merge(self.data_provider[Data.ALL_TEMPLATES],
                                                                      how='left', on='template_fk', suffixes=['', '_y'])
        self.survey_response = self.data_provider[Data.SURVEY_RESPONSES]
        self.get_atts()
        self.ignore_stacking = ignore_stacking
        self.facings_field = 'facings' if not self.ignore_stacking else 'facings_ign_stack'
        self.front_facing = front_facing
        for data in kwargs.keys():
            setattr(self, data, kwargs[data])
        if self.front_facing:
            self.scif = self.scif[self.scif['front_face_count'] == 1]

    def get_atts(self):

        query = MSC_NEWQueries.get_product_atts()
        product_att3 = pd.read_sql_query(query, self.rds_conn.db)
        self.scif = self.scif.merge(product_att3, how='left', left_on='product_fk',
                                    right_on='product_fk')


    def calculate_share_of_shelf(self, scene_data, sos_filters=None, include_empty=EXCLUDE_EMPTY, **general_filters):
        """
        :param sos_filters: These are the parameters on which ths SOS is calculated (out of the general DF).
        :param include_empty: This dictates whether Empty-typed SKUs are included in the calculation.
        :param general_filters: These are the parameters which the general Data frame is filtered by.
        :return: The ratio of the Facings SOS.
        """
        if include_empty == self.EXCLUDE_EMPTY and 'product_type' not in sos_filters.keys() + general_filters.keys():
            general_filters['product_type'] = (self.EMPTY, self.EXCLUDE_FILTER)
        general_filters['stacking_layer'] = 1
        sos_filters['stacking_layer'] = 1
        merged_scene_data = scene_data.merge(self.all_products, on='product_fk', how='left')
        pop_filter = self.get_filter_condition(merged_scene_data, **general_filters)
        subset_filter = self.get_filter_condition(merged_scene_data, **sos_filters)

        try:
            ratio = float(sum(subset_filter))/sum(pop_filter)
        except:
            ratio = 0

        if not isinstance(ratio, (float, int)):
            ratio = 0
        return ratio

    def get_filter_condition(self, df, **filters):
        """
        :param df: The Data frame to be filters.
        :param filters: These are the parameters which the Data frame is filtered by.
                       Every parameter would be a tuple of the value and an include/exclude flag.
                       INPUT EXAMPLE (1):   manufacturer_name = ('Diageo', DIAGEOAUMSCGENERALToolBox.INCLUDE_FILTER)
                       INPUT EXAMPLE (2):   manufacturer_name = 'Diageo'
        :return: a filtered Scene Item Facts Data frame.
        """
        if not filters:
            return df['pk'].apply(bool)
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
