import requests
import os
import time
from concurrent.futures import ThreadPoolExecutor
import argparse

jwt = ""
base_url = "https://connect-api.duckdns.org"


# Get all devices.
def get_devices():
    url = f'{base_url}/v1/me/devices?sig={jwt}'
    response = requests.get(url)
    
    if response.status_code == 200:
        devices = response.json()
        return devices
    else:
        return {"error": response.status_code, "message": response.text}

def find_routes(dongle_id):
    now = int(time.time())*1000
    start_time = (int(time.time())-86400)*1000 # 1 day ago in ms
    url = f'{base_url}/v1/devices/{dongle_id}/routes_segments?start={start_time}&end={now}&sig={jwt}'
    response = requests.get(url)
    if response.status_code == 200:
        routes = response.json()
        return routes
    else:
        return {"error": response.status_code, "message": response.text}

def get_route_files(route_name):
    url = f'{base_url}/v1/route/{route_name}/files?sig={jwt}'
    response = requests.get(url)
    if response.status_code == 200:
        files = response.json()
        return files
    else:
        return {"error": response.status_code, "message": response.text}

def download_file(url: str):
    path: str = url.lstrip("https://connect-api.duckdns.org/connectdata/qlog/")
    path = path.split("?")[0]  # remove query params from URL
    # https://connect-api.duckdns.org/connectdata/qlog/3b58edf884ab4eaf/2024-06-13--15-59-30/0/qlog.bz2
    path = os.path.join('downloads', path)
    if os.path.exists(path):
        print(f"File already exists: {path}")
        return
    response = requests.get(url)
    if response.status_code != 200:
        return
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as file:
        file.write(response.content)
    print(f"Downloaded and saved file: {path}")

def main():
    parser = argparse.ArgumentParser(description="Download files for devices.")
    parser.add_argument(
        '--categories', 
        nargs='+', 
        default=['cameras', 'qcameras', 'logs', 'qlogs', 'ecameras', 'dcameras'],
        help=f"List of file categories to include. Defaults to all categories. ['cameras', 'qcameras', 'logs', 'qlogs', 'ecameras', 'dcameras']"
    )
    parser.add_argument(
        '--dongle_id',
        default=None,
    )
    args = parser.parse_args()
    
    devices = get_devices()
    files = []
    for device in devices:
        if args.dongle_id and args.dongle_id != device['dongle_id']:
            continue
        device_routes = find_routes(device["dongle_id"])
        for route in device_routes:
            file_urls = get_route_files(route["fullname"])
            file_mappings = {
                'cameras': 'fcam.hevc',
                'qcameras': 'qcam.ts',
                'logs': 'rlog.bz2',
                'qlogs': 'qlog.bz2',
                'ecameras': 'ecam.hevc',
                'dcameras': 'dcam.hevc'
            }
            for category, filename in file_mappings.items():
                for file_url in file_urls.get(category, []):
                    if 'unlog' in file_url: # unlog files are in qlogs category
                        continue
                    if category not in args.categories:
                        continue
                    files.append(file_url)


    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, files)

if __name__ == "__main__":
    main()

