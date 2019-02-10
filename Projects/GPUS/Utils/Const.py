

__author__ = 'Nic, Sam'


class Const(object):
    FACINGS = 'facings'
    LINEAR_FACINGS = 'net_len_ign_stack'
    ALLOWED_EDGES = {'left', 'right'}
    EMPTY_FILTER = {'product_type': 'Empty'}
    EMPTY_FKS = {'Empty': 0}
    HIERARCHY = {'brand': {'parent': 'category', 'ident': None}, 'category': {'parent': None, 'ident': 'category'}}
