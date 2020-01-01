
class AtomicKpiNotInStaticException(Exception):

    def __init__(self, message):
        super(AtomicKpiNotInStaticException, self).__init__(message)


class KpiNotInStaticException(Exception):

    def __init__(self, message):
        super(KpiNotInStaticException, self).__init__(message)


class KpiSetNotInStaticException(Exception):

    def __init__(self, message):
        super(KpiSetNotInStaticException, self).__init__(message)

