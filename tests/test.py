import csv
import requests

if __name__ == "__main__":
  with open('pixel_data.csv', newline='') as csvfile:
    ip_reader = csv.reader(csvfile, delimiter=',')
    for row in ip_reader:
      requests.post(url='http://localhost:5000/pixel', headers={'Content-Type': 'application/json'}, json={'date': row[0], 'ip': row[1], 'useragent': row[2], 'thirdpartyid': row[3]})

  response = requests.get(url='http://localhost:5000/pixel')

  try:
    assert len(response.json()['data']) > 10
  except AssertionError as msg:
    print('FAILED TEST: got less than 10 records')
    raise msg

  print('PASSED TEST: go ahead an submit this if you want!')


