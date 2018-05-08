from Projects.CCBOTTLERSUS_SAND.REDSCORE.Const import Const


class Converters(object):

    @staticmethod
    def convert_column_to_scif_filter(column):

        column_map = {
            Const.SCENE_TYPE: 'template_name',
            Const.SCENE_TYPE2: 'template_name',
            Const.SCENE_TYPE_GROUP: 'template_group',
            Const.MANUFACTURE: 'manufacturer_name',
            Const.BRAND_NAME: 'brand_name',
            Const.TRADEMARK:'att2',
            Const.PRODUCT_CATEGORY: 'att4',
            Const.SUB_CATEGORY: 'sub_category',
            Const.NUM_OF_SUB_PACKAGES: 'number_of_sub_packages',
            Const.PRODUCT_CODES: 'product_ean_code'
            #TODO add att3

        }

        return column_map.get(column)

    @staticmethod
    def convert_column_to_store_filter(column):
        column_map = {
            Const.STORE_ATT15: 'additional_attribute_15'
        }
        return column_map.get(column)

    @staticmethod
    def convert_sos_filter(column):

        column_map = {
            Const.MANUFACTURE: 'manufacturer_name',
            Const.BRAND: 'brand_name',
            Const.CATEGORY: 'category',
            Const.TRADEMARK: 'att2',
            Const.PRODUCT_CATEGORY: 'att4'

        }

        return column_map.get(column)

    @staticmethod
    def get_scif_filter(filtered_template):

        scif_filter = {}
        filtered_template_dict = filtered_template.to_dict()
        for key in filtered_template_dict.keys():
            if filtered_template.get(key) and Converters.convert_column_to_scif_filter(key):
                # convert to list
                if ',' in filtered_template.get(key):
                    scif_filter[str(Converters.convert_column_to_scif_filter(key))] = \
                        [str(item.strip()) for item in filtered_template.get(key).split(',')]
                else:
                    scif_filter[str(Converters.convert_column_to_scif_filter(key))] = str(filtered_template_dict.get(key))

        return scif_filter

    @staticmethod
    def get_sos_filter(filtered_template):
        sos_filter = {}
        filtered_template_dict = filtered_template.to_dict()
        for key in filtered_template_dict.keys():
            if filtered_template.get(key) and key == Const.ENTITY_TYPE_NUMERATOR :
                sos_key = Converters.convert_sos_filter(filtered_template.get(key))

                # convert NUMERATOR list
                if filtered_template.get(Const.NUMERATOR) and ',' in filtered_template.get(Const.NUMERATOR):
                    sos_filter[sos_key] = \
                        [str(item.strip()) for item in filtered_template.get(Const.NUMERATOR).split(',')]
                elif filtered_template.get(Const.NUMERATOR):
                    sos_filter[sos_key] = filtered_template.get(Const.NUMERATOR)

        return sos_filter