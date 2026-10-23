from massive import RESTClient

# docs
# https://massive.com/docs/rest/economy/funding-conditions

# client = RESTClient("XXXXXX") # hardcoded api_key is used
client = RESTClient()  # MASSIVE_API_KEY environment variable is used

conditions = []
for day in client.list_funding_conditions(date_gte="2025-06-01", sort="date.asc"):
    conditions.append(day)

print(conditions)
