# loggy

Your friendly log generator

## Functions

### 1. TimerTrigger
- Runs automatically every 5 minutes
- Generates a log with a unique UUID

### 2. HttpTrigger
- **Route**: `/HttpTrigger`
- **Methods**: GET/POST
- Generates a single log with a unique UUID

### 3. CustomLog
- **Route**: `/CustomLog`
- **Methods**: GET/POST
- Allows custom log generation with parameters:
  - `message`: **Required** - Custom message to log
  - `level`: Log level - debug/info/warning/error/critical (default: "info")
  - `count`: Number of logs to generate, max 100 (default: 1)

#### Examples:
```bash
# GET with query params
curl "https://<function-app>.azurewebsites.net/api/CustomLog?message=Test%20error&level=error&count=5"

# POST with JSON body
curl -X POST https://<function-app>.azurewebsites.net/api/CustomLog \
  -H "Content-Type: application/json" \
  -d '{"message": "Testing log pipeline", "level": "warning", "count": 10}'
```

## Deployment

```bash
func azure functionapp publish <APP NAME>
```
