from create_base import (
    create_connectors,
    create_destination,
    create_warehouse,
    fivetran,
)


def main(type_of_request):

    protocol = "https"
    core_fivetran_url = "api.fivetran.com"
    api_version = "v1"
    api_key = "kIVnsK2i9paDLij2"
    api_secret = "11S8JSYAfkIHnqBAphbaixBhw6mxpsUY"
    file_location = (
        "C:\\Learning\\Dataops-gcp\\Infrastructure\\Fivetran\\data\\connector_info.txt"
    )

    payload = {"name": "warehouse_code"}

    request_type = {
        "groups": create_warehouse,
        "connectors": create_connectors,
        "destination": create_destination,
    }

    # Initialsing class as per the request

    req_obj = request_type[type_of_request]()
    c = fivetran(
        req_obj,
        protocol,
        core_fivetran_url,
        api_version,
        api_key,
        api_secret,
        payload,
        type_of_request,
        file_location,
    )
    if c.create_fivetran_requested_object():
        print("Group creation successfull")
    else:
        print("Error in group creation check logs")


main("groups")
