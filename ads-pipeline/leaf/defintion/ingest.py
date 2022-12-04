from abc import *


class Ingest(metaclass=ABCMeta):
    @abstractmethod
    def export(self, s, u):
        pass

    @abstractmethod
    def get_data(self):
        pass
