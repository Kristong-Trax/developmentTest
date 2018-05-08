
__author__ = 'Ilan'


class Queries(object):

    @staticmethod
    def get_store_attribute_15(store_id):
        return """
            select additional_attribute_15 from static.stores where pk = '{}'
               """.format(store_id)

    @staticmethod
    def get_product_attribute_3(pk):
        return """
            select att3 from static.product where pk = '{}'
               """.format(pk)

    @staticmethod
    def get_list_of_products_with_att3(att3_name, scene_list):
        query =  """
            select p.pk
            from probedata.match_product_in_scene_recognition as m,
            static.product  as p
            where p.pk = m.product_fk
            """
        if len(att3_name) > 1 and isinstance(att3_name, (list, tuple)):
            att3_condition = 'and p.att3 in {} '.format(att3_name)
        else:
            att3_condition = 'and p.att3 = {} '.format("'" + att3_name + "'")

        if len(scene_list) > 1:
            scene_condition = 'and m.scene_fk in {}'.format(scene_list)
        else:
            scene_condition = 'and m.scene_fk = {}'.format(scene_list)

        return query + att3_condition + scene_condition
