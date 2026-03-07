import http.client
import json
from urllib.parse import urlparse
from Library.credentials import credentials
from time import sleep

def get_robot_face(url: str):
    try:
        parsed_url = urlparse(url)
        conn = http.client.HTTPConnection(parsed_url.netloc)

        path = parsed_url.path.rstrip("/") + "/api/face"
        print(f"{url}/api/face")

        conn.request("GET", path)
        response = conn.getresponse()
        status_code = response.status
        data = response.read()

        if status_code != 200:
            print(f"Failed to retrieve face status: HTTP {status_code}")
            return None

        json_data = json.loads(data)
        print('Face Status:', json_data)
        return json_data

    except Exception as e:
        print(f"Failed to retrieve face status: {e}")
        return None

if __name__ == "__main__":
    while True:
        get_robot_face(credentials["url"])
        sleep(1)
        


    
