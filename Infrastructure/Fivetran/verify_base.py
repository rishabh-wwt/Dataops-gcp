from abc import ABC, abstractmethod


class verfiyFivetranObj(ABC):
    def payload(self):
        pass

    def build(self):
        pass

    def excute(self):
        pass

    def validate(self):
        pass


class verifyObjExistence(verfiyFivetranObj):
    def verify_group():
        pass

    def verify_connector():
        pass

    def verify_destination():
        pass
