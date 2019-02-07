import numpy as np
import pandas as pd
from Projects.RINIELSENUS.Utils.Const import FILTER_NAMING_DICT, TARGET, IRRELEVANT_FIELDS, KPI_GROUP, KPI_NAME, \
    KPI_TYPE, WEIGHT, RELEVANT_SCENE_TYPES, SEPARATOR, FILTER_NAME_SEPARATOR, EXCLUDE, ALLOWED, DEPEND_ON, \
    DEPEND_SCORE, NOT_SEPARATE_FIELDS
from Projects.RINIELSENUS.Utils.GeneralToolBox import MarsUsGENERALToolBox
from Projects.RINIELSENUS.Utils.ParseTemplates import KPIConsts


class Definition(object):
    def __init__(self, template_data, channel, retailer, store_type):
        self._template_data = template_data
        self._channel = channel
        self._retailer = retailer
        self._store_type = store_type

    def get_atomic_hierarchy_and_filters(self, set_name):
        kpi_hierarchy = self._template_data[KPIConsts.SHEET_NAME]
        atomics = []
        for index, kpi in kpi_hierarchy.iterrows():
            if not self._is_non_negotiable(kpi):
                atomic = self._get_atomic_definition(kpi, set_name)
                atomic['filters'] = MarsUsGENERALToolBox.rename_filters_to_match_scif(atomic['filters'],
                                                                                      FILTER_NAMING_DICT)
                atomic['allowed'] = MarsUsGENERALToolBox.rename_filters_to_match_scif(atomic['allowed'],
                                                                                      FILTER_NAMING_DICT)
            else:
                atomic = self._get_nbil_atomic_definition(kpi, set_name)

            atomics.append(atomic)

        return atomics

    @staticmethod
    def _is_non_negotiable(atomic_kpi_data):
        return atomic_kpi_data['NBIL'] == 'Y'

    def _get_nbil_atomic_definition(self, kpi, set_name):
        nbil_data = self._get_nbil_relevant_data(kpi, set_name)
        general_info = self._get_general_info_from_template(kpi, set_name)

        general_info.update({'nbil_products': nbil_data})
        return general_info

    def _get_nbil_relevant_data(self, kpi, set_name):
        nbil_data = self._template_data[kpi['Target']]
        if 'CHANNEL' in nbil_data.keys():
            nbil_data_for_set = self._get_nbil_channel_data(nbil_data, self._channel, set_name)
        else:
            nbil_data_for_set = self._get_nbil_retailer_data(nbil_data, self._retailer, set_name, self._store_type)

        nbil_products = pd.DataFrame(columns=['upc'])
        nbil_products.loc[:, 'upc'] = nbil_data_for_set['UPC'].astype(str).str.zfill(12)
        nbil_products.loc[:, 'either'] = nbil_data_for_set['Either/Or\n(by Retailer & Sub-Category)']
        return nbil_products

    def _get_atomic_kpi_row(self, kpi, set_name):
        template = self._template_data[kpi['Target']][self._template_data[kpi['Target']]['KPI name'] == kpi['KPI name']]
        return template[(template['KPI name'] == kpi['KPI name']) & (template['Score Card Name'] == set_name)]

    @staticmethod
    def _get_nbil_channel_data(nbil_data, channel, set_name):
        return nbil_data[(nbil_data['CHANNEL'] == channel) & (nbil_data['MARS_SUB-CATEGORY'] == set_name.upper())]
        # return nbil_data[(nbil_data['CHANNEL'] == channel)]

    @staticmethod
    def _get_nbil_retailer_data(nbil_data, retailer, set_name, store_type=None):
        nbil_data_filter = nbil_data[(nbil_data['RETAILER'] == retailer) & (nbil_data['MARS_SUB-CATEGORY'] == set_name.upper())]
        # nbil_data_filter = nbil_data[(nbil_data['RETAILER'] == retailer)]
        try:
            if nbil_data_filter['Store Type'].unique()[0]:
                return nbil_data_filter[nbil_data_filter['Store Type'].str.contains(store_type)]
            else:
                return nbil_data_filter
        except:
            return nbil_data_filter

    def _get_atomic_definition(self, kpi, set_name):
        template = self._get_atomic_kpi_row(kpi, set_name)

        general_info = self._get_general_info_from_template(kpi, set_name)
        general_info.update({
            'target': template[TARGET].iloc[0],
            'filters': template.drop(IRRELEVANT_FIELDS, axis=1, errors='ignore').replace({'': np.nan}).dropna(
                axis=1).to_dict(orient='records')[0]
        })
        general_info['filters'] = self.split_multiple_filters(general_info['filters'])
        general_info['filters'], general_info['allowed'] = self._split_allowed_filters(general_info['filters'])
        general_info['filters'] = self._exclude_filters(general_info['filters'])
        general_info['allowed'] = self._exclude_filters(general_info['allowed'])
        return general_info

    @staticmethod
    def _get_general_info_from_template(kpi, set_name):
        return {
            'set': set_name,
            'kpi': kpi[KPI_GROUP],
            'atomic': kpi[KPI_NAME],
            'kpi_type': kpi[KPI_TYPE],
            'weight': kpi[WEIGHT] / 100,
            'scene_types': str(kpi[RELEVANT_SCENE_TYPES]).split(SEPARATOR),
            'depend_on': str(kpi[DEPEND_ON]).split(SEPARATOR),
            'depend_score': str(kpi[DEPEND_SCORE]).split(SEPARATOR),
            # 'kpi_category': kpi['Target'],
        }

    @staticmethod
    def _exclude_filters(filters):
        for filter_ in filters:
            if filter_.split(FILTER_NAME_SEPARATOR)[0] == EXCLUDE:
                filters[filter_.split(FILTER_NAME_SEPARATOR)[1]] = (
                    filters.pop(filter_), MarsUsGENERALToolBox.EXCLUDE_FILTER)
        return filters

    @staticmethod
    def split_multiple_filters(filters):
        for key in filters:
            if key not in NOT_SEPARATE_FIELDS:
                filters[key] = str(filters[key]).split(SEPARATOR)
        return filters

    @staticmethod
    def _split_allowed_filters(filters):
        old_filters = filters.copy()
        allowed = {}
        new_filters = {}
        for filter_ in old_filters:
            if filter_.split(FILTER_NAME_SEPARATOR)[0] == ALLOWED:
                allowed[filter_.split(FILTER_NAME_SEPARATOR)[1]] = filters.pop(filter_)
            else:
                new_filters[filter_] = filters.pop(filter_)
        return new_filters, allowed


class NbilLoader(object):
    def __init__(self, channel, retailer):
        """

        :param Pandas.DataFrame nbil_data:
        :param str channel :
        :param str retailer:
        """
        self._channel = channel
        self._retailer = retailer
        self._set_name = 'NBIL'
        self._kpi_type = 'NBIL AVAILABILITY'

    @staticmethod
    def get_nbil_for_channel(nbil_data, channel):
        """

        :param nbil_data:
        :param str channel:
        :return:
        """
        return nbil_data[nbil_data['CHANNEL'] == channel.upper()]

    def get_hierarchy(self, nbil_data):
        atomics = []
        nbil_for_channel = self.get_nbil_for_channel(nbil_data, self._channel)
        if nbil_for_channel.empty:
            return atomics

        nbil_with_target = self._get_nbil_with_target(nbil_for_channel)
        for i, row in nbil_with_target.iterrows():
            atomics.append(self._get_general_info_from_template(row))

        return atomics

    def _get_general_info_from_template(self, row):
        return {
            'set': self._set_name,
            # 'kpi': row['MARS_SUB-CATEGORY'],
            'kpi': row['SubSection'],
            'atomic': row['UPC'],
            'kpi_type': self._kpi_type,
            'weight': None,
            'scene_types': str(row[RELEVANT_SCENE_TYPES]).split(SEPARATOR),
            'filters': {'product_ean_code': str(row['UPC'])},
            'target': row['result_target']
        }

    def _get_nbil_with_target(self, nbil_data):
        target_column = self.find_target_column(nbil_data)
        self._set_target_column(nbil_data, target_column)
        return self.filter_row_with_not_valid_target(nbil_data)

    @staticmethod
    def _get_nbil_channel_data(nbil_data, channel):
        return nbil_data[(nbil_data['CHANNEL'] == channel)]

    @staticmethod
    def _get_nbil_channel_data(nbil_data, retailer):
        return nbil_data[(nbil_data['RETAILER'] == retailer)]

    def _is_explicit_target_for_retailer(self, nbil_data, retailer_channel_combination):
        return retailer_channel_combination in nbil_data.columns.tolist()

    def _get_target_name_for_retailer_and_mass(self):
        return '{} {}'.format(self._retailer.upper(), self._channel.upper())

    @staticmethod
    def _set_target_column(nbil_data, from_columns):
        nbil_data['result_target'] = nbil_data[from_columns]

    def _get_general_channel_target(self):
        return 'ALL OTHER {}'.format(self._channel.upper())

    def find_target_column(self, nbil_data):
        if self._is_explicit_target_for_retailer(nbil_data, self._get_target_name_for_retailer_and_mass()):
            return self._get_target_name_for_retailer_and_mass()
        else:
            return self._get_general_channel_target()

    @staticmethod
    def _is_target_valid(target):
        """

        :param str target:
        :return:
        """
        return ((type(target) == str) and target.isdigit()) or (type(target) in [float, int])

    def filter_row_with_not_valid_target(self, nbil_data):
        return nbil_data[nbil_data.apply(lambda x: self._is_target_valid(x['result_target']), axis=1)]
