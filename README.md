# Arrow
Data import translation service

## Requirements
- `python3`
- `pip`

## API
`POST` `/avroToRawls`
- Request Body: `{ "url": "..." }`
- Success Response: Rawls upsert JSON

## Development Setup
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Running locally
```
python arrow/server.py
```

Running tests
```
pytest
```

## Deployment
```
ENV=<env> ./scripts/deploy.sh
```
