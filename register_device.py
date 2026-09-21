import getpass

from db import register_device


device_id = input("Device ID: ").strip()
token = getpass.getpass("Device token: ")

name = input("Device name: ").strip()
spot = input("Parking spot: ").strip()

device_pk = register_device(
    device_id=device_id,
    token=token,
    name=name,
    spot=spot
)

print(f"Device registered. Database ID: {device_pk}")
