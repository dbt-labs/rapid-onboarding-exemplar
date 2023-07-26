# Note: the easiest way to run this is through this Hex app because it has the token stored as a secret
# https://app.hex.tech/fishtown/hex/be772c9d-fce1-4994-9bb9-423429318943/draft/logic

import requests

account_id = 26712
job_id = 38569
token = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'

headers = {
   'Authorization': f'Token {token}',
   'Content-Type': 'application/json'
}

json_data = {
   'cause': 'Triggered from a Python Script'
}

response = requests.post(
   f'https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run',
   headers=headers,
   json=json_data
)

print(response)