import requests


url = "https://api.hcdp.ikewai.org/raw/list?date=2020-11-03"

headers = {"Authorization": "Bearer e84cad0640218ce7ac1526413401c921"}


response = requests.get(url=url, headers=headers, verify=False)

print(response)

print(f"code: {response.status_code}")
print(response.json())
