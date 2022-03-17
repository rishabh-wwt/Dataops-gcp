import json
from abc import ABC, abstractmethod, abstractstaticmethod

import requests
from requests.auth import HTTPBasicAuth


def api_call_headers(api_key):
    return {"Authorization": "Basic " + api_key, "Content-Type": "application/json"}


class createBaseFivetranObj(ABC):
    @abstractmethod
    def build(self, http_protocol: str, fivetran: str, api_version: str):
        pass

    @abstractmethod
    def verify_payload(self, payload: dict):
        pass

    @abstractstaticmethod
    def autheincate(self, api_key: str, api_secret: str):
        pass

    @abstractmethod
    def execute(self, header, url, auth, json) -> dict:
        pass

    @abstractmethod
    def validate(self, response: dict):
        pass

    @abstractmethod
    def store_success_resp(self, location: str):
        pass


class create_warehouse(createBaseFivetranObj):

    """A group represents a warehouse within Fivetran Top of the hiearchy"""

    def build(
        self, http_protocol: str, fivetran: str, api_version: str, group_route: str
    ) -> str:
        return http_protocol + "://" + fivetran + "/" + api_version + "/" + group_route

    def execute(self, header, group_creaetion_api, auth, payload) -> dict:

        response = requests.post(
            headers=header, url=group_creaetion_api, auth=auth, json=payload
        ).json()

        return response

    def verify_payload(self, payload: dict):
        if isinstance(payload, dict):
            key_name = list(payload.keys())[0]
            group_name = list(payload.values())[0]

            if not group_name or key_name != "name":
                raise ValueError(
                    f"Either group_name is null or payload key is not name"
                )
        else:
            raise Exception(f"payload object is not a dict please pass is as a dict")

    def autheincate(self, api_key: str, api_secret: str):
        auth = HTTPBasicAuth(api_key, api_secret)
        return auth

    def validate(self, response: dict):
        """

        Validating reponse from create call

        """

        if response["code"] == "Success":
            return True
        else:
            return False

    def store_success_resp(self, response: dict, location: str):
        try:
            with open(location, "a") as save_file:
                save_file.write(json.dumps(response))
            return "Write Successfull"
        except Exception as e:
            return f"Error in writing - {e}"


class create_connectors(createBaseFivetranObj):
    def build(self, http_protocol: str, fivetran: str, api_version: str):
        print("I am building connector api call")

    def execute(self, header, url, auth, payload):
        print("I execute connector api calls")

    def verify_payload(self, payload: dict):

        if isinstance(payload, dict):
            group_name = list(payload.values())[0]
            if not group_name:
                print(f"group_name cannot be null")
        else:
            print(f"payload object is not a dict please pass is as a dict")

    def verify_group(self):
        print("I verfiy group before creating connection")

    def autheincate(self, api_key: str, api_secret: str):
        pass

    def validate(self, response: dict):
        pass


class create_destination(createBaseFivetranObj):
    def build(self, http_protocol: str, fivetran: str, api_version: str):
        print("I am building destination api call")

    def execute(self, header, url, auth, payload):
        print("I execute destination api calls")

    def verify_payload(self, payload: dict):
        print("I am verify destination payload")

    def autheincate(self, api_key: str, api_secret: str):
        pass

    def validate(self, response: dict):
        pass


class fivetran:
    def __init__(
        self,
        c: createBaseFivetranObj,
        protocol,
        core_fivetran_url,
        api_version,
        api_key,
        api_secret,
        payload,
        request_type,
        file_location,
    ) -> None:
        self.client = c
        self.http_protocol = protocol
        self.fivetran_base_api = core_fivetran_url
        self.api_version = api_version
        self.api_key = api_key
        self.api_secret = api_secret
        self.request_payload = payload
        self.type_of_request = request_type
        self.location = file_location

    def create_fivetran_requested_object(self):

        """
        # Step -1 : Verify Payload -> verify_payload()
        # Step -2 : Buil API -> build()
        # Step -3 : Authenicate and build headers -> authenicate ()
        # Step -4 : Execute API request -> execute()
        # Step -5 : Verify API response -> validate()

        """
        self.client.verify_payload(self.request_payload)
        basic_api_call = self.client.build(
            self.http_protocol,
            self.fivetran_base_api,
            self.api_version,
            self.type_of_request,
        )
        auth = self.client.autheincate(self.api_key, self.api_secret)
        response = self.client.execute(
            header=api_call_headers(api_key=self.api_key),
            group_creaetion_api=basic_api_call,
            auth=auth,
            payload=self.request_payload,
        )
        if self.client.validate(response=response):

            print(self.client.store_success_resp(response, self.location))

            return True
        else:
            return False
