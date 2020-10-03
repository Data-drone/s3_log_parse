# List of things we should be looking at

Individual Prefix performance

HTTP Status codes
- there seem to be a fair variety probably need to plot out

Object Size to get a feel for small file problem

Total Time
- How long it took server to respond - was this uniformish?
- Request Received -> Last part of response sent

Turn-Around Time
- S3 Processing time

- Check Referers and User-Agent
  -- User-Agent can identify the service 

Host Header?
  - endpoint connecting to S3

Request IDs
  - identifies a request - should in theory be show how many files were needed to fulfil a request?

## Interesting Tidbits 

- S3 bucket max is 100k files (from Wargaming client - found on slack)