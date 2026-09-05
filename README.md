# Webhook broker

The server accepts webhooks and events, saves them to the DB, and the broker reliably delivers saved events to hooks. All events are delivered to hooks in FIFO format strictly following the queue. 

## Tech Stack

- **Language:** GO 1.26.0

### Prerequisites

- Git installed
- GO 1.26.0+
- Docker installed

## API Endpoints

The API runs on `http://localhost:8080` by default.

### 1. Add webhook

- URL: `http://localhost:8080/webhooks`
- Method: POST
  Add a new webhook.

### 1. Get events

- URL: `http://localhost::8080/events`
- Method: POST
  Add a new event and push it to the delivery queue.

## Quck use

The project includes make commands to easily use the app.

1. Start the DB:

```bash
make init
```

2. Start server:

```bash
make server
```

3. Start the broker:

```bash
make broker
```
4. Start the filter

```bash
make filer
```
5. Remove DB and clean mounts:

```bash
make rm-db
```

6. Other useful commands:

```bash
make help
```

## Filtering

The project includes a separate service to filter events according to the webhooks `filters` parameter. All currently available filters with a POST body example: 

```JSON
{
  "hook_url": "https://api.example.com/test",
  "filters": {
    "divisibleByN": 2,
    "dataStartsWith": "A"
    "expectedIssuer": "testo"
  }
}
```
Filters are not necessary for mandatory functionality. You can drop any of the filters, щк the whole `filters` block itself, so that the webhook will receive all incoming events. 

## Environment

The project already has default environment variables, but if necessary, you can create your own `.emv` file with variables from the `/internal/config/config.go` file.  


