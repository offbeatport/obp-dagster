# Set your token

# Define the query as a single-line string for the JSON body
QUERY='query($searchQuery: String!) {
  search(query: $searchQuery, type: ISSUE, first: 10) {
    issueCount
    nodes {
      __typename
      ... on Issue {
        title
        url
        repository { nameWithOwner }
      }
      ... on Discussion {
        title
        url
        repository { nameWithOwner }
      }
    }
  }
  rateLimit {
    cost
    remaining
    resetAt
  }
}'

# Execute the curl command
curl -X POST \
     -H "Authorization: bearer $TOKEN" \
     -H "Content-Type: application/json" \
     -d "{
       \"query\": \"$(echo $QUERY | tr -d '\n' | sed 's/"/\\"/g')\",
       \"variables\": { \"searchQuery\": \"created:2026-02-07\" }
     }" \
     https://api.github.com/graphql
