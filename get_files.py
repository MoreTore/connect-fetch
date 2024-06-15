import requests
import os
import time
import json
from concurrent.futures import ThreadPoolExecutor
import argparse

authorization = None # get jwt from https://connect-portal.duckdns.org browser debug tools. stored in app storage
assert authorization
base_url = "https://connect-api.duckdns.org"
FILE_MAPPINGS = {
    'cameras': 'fcam.hevc',
    'qcameras': 'qcam.ts',
    'logs': 'rlog.bz2',
    'qlogs': 'qlog.bz2',
    'ecameras': 'ecam.hevc',
    'dcameras': 'dcam.hevc'
}

# Get all devices.
def get_devices():
    url = f'{base_url}/v1/me/devices?sig={authorization}'
    response = requests.get(url)
    
    if response.status_code == 200:
        devices = response.json()
        return devices
    else:
        return {"error": response.status_code, "message": response.text}

def find_routes(dongle_id):
    now = int(time.time())*1000
    start_time = (int(time.time())-86400)*1000 # 1 day ago in ms
    url = f'{base_url}/v1/devices/{dongle_id}/routes_segments?start={start_time}&end={now}&sig={authorization}'
    response = requests.get(url)
    if response.status_code == 200:
        routes = response.json()
        return routes
    else:
        return {"error": response.status_code, "message": response.text}

def get_route_files(route_name):
    url = f'{base_url}/v1/route/{route_name}/files?sig={authorization}'
    response = requests.get(url)
    if response.status_code == 200:
        files = response.json()
        return files
    else:
        return {"error": response.status_code, "message": response.text}

def ls_log_dir(dongle_id):
    jsonrpc_request = {
        "jsonrpc": "2.0",
        "method": "listDataDirectory",
        "id": 0
    }
    headers = {
        "Authorization": f"JWT {authorization}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(f'{base_url}/ws/{dongle_id}', headers=headers, data=json.dumps(jsonrpc_request), timeout=5)
    except requests.exceptions.Timeout:
        print(f"Timeout occurred while getting upload URLs for {dongle_id}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None
    if response.status_code == 200:
        json_response = response.json()
        if json_response.get('result'):
            return json_response.get('result')
        else:
            print("Error")
    else:
        print(f"Failed to send request: {response.status_code}")
        print("Response:", response.text)

def get_upload_url(dongle_id, paths):
    url = f'{base_url}/v1/{dongle_id}/upload_urls'
    for i, path in enumerate(paths):
        if 'rlog' in path:
            paths[i] = path + ".bz2"
        if 'qlog' in path:
            paths[i] = path + ".bz2"
        if 'qcam' in path:
            paths[i] = path + ".ts"
        if 'fcam' in path:
            paths[i] = path + ".hevc"
        if 'dcam' in path:
            paths[i] = path + ".hevc"
        if 'ecam' in path:
            paths[i] = path + ".hevc"

    payload = {
        "paths": paths,
    }
    headers = {
        "Authorization": f"JWT {authorization}",
        "Content-Type": "application/json"
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code == 200:
        upload_urls = response.json()

        return upload_urls
    else:
        return {"error": response.status_code, "message": response.text}

def request_upload(dongle_id, paths):
    urls = get_upload_url(dongle_id, paths)
    headers = {
        "Authorization": f"JWT {authorization}",
        "Content-Type": "application/json"
    }
    for i, url in enumerate(urls): # Do this in one request with uploadFilesToUrls
        jsonrpc_request = {
            "jsonrpc": "2.0",
            "method": "uploadFileToUrl",
            "params": {            
                "fn": paths[i],
                "url": url['url'], 
                "headers": {
                    "x-ms-blob-type": "BlockBlob"}
                },
            "id": 0
        }
        response = requests.post(f'{base_url}/ws/{dongle_id}', headers=headers, data=json.dumps(jsonrpc_request))
        if response.status_code == 200:
            print(response.json())
        else:
            print(f"Failed to send request: {response.status_code}")
            print("Response:", response.text)
            break # device could go offline

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

def run(args):
    devices = get_devices()
    files = []
    for device in devices:
        if args.device_upload:
            if device["online"]:
                files = ls_log_dir(device["dongle_id"])
                if files:
                    filtered_files = list(filter(lambda file: 'rlog' in file, files))
                    request_upload(device["dongle_id"], filtered_files)

        if args.dongle_id and args.dongle_id != device['dongle_id']:
            continue
        device_routes = find_routes(device["dongle_id"])
        for route in device_routes:
            file_urls = get_route_files(route["fullname"])
            for category, filename in FILE_MAPPINGS.items():
                for file_url in file_urls.get(category, []):
                    if 'unlog' in file_url: # unlog files are in qlogs category
                        continue
                    if category not in args.categories:
                        continue
                    files.append(file_url)


    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(download_file, files)

def main():
    parser = argparse.ArgumentParser(description="Download files for devices.")
    parser.add_argument(
        '--endless',
        action='store_true',
        help="Runs in a loop forever"
    )
    parser.add_argument(
        '--device_upload',
        action='store_true', 
        help="SLOW! Upload rlogs from the device to the server, then download the rlogs from the server.",
    )
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
    if args.endless:
        while 1:
            run(args)
    else:
        run(args)


if __name__ == "__main__":
    main()

