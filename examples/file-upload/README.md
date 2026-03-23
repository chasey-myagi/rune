# File Upload Example

Demonstrates multipart file upload and download via the Rune Gate HTTP API.

## How It Works

Rune Gate supports `multipart/form-data` requests:

- A part named `input` (with `Content-Type: application/json`) is passed as the Rune handler's input
- All other parts are treated as file attachments, stored in the file broker
- Files < 4 MB are transferred inline; larger files go through the broker and can be fetched via `/api/v1/files/:id`

## Prerequisites

1. Start the Rune server:
   ```bash
   cargo run -p rune-server
   ```

2. Start the Python caster (provides the `file_info` rune):
   ```bash
   cd sdks/python && pip install -e .
   python examples/python-caster/main.py
   ```

## Usage

### Run the script

```bash
./examples/file-upload/upload.sh
```

The script demonstrates:
1. Single file upload via gate path (`/file-info`)
2. Multiple file upload in one request
3. Upload via the debug API (`/api/v1/runes/file_info/run`)
4. File download via `/api/v1/files/:id`

### Manual curl examples

**Upload a file:**
```bash
curl -X POST http://localhost:50060/file-info \
  -F 'input={"tag":"test"};type=application/json' \
  -F 'file=@examples/file-upload/sample.txt;type=text/plain'
```

Response:
```json
{
  "files": [
    {
      "file_id": "f-abc123",
      "filename": "sample.txt",
      "mime_type": "text/plain",
      "size": 142,
      "transfer_mode": "inline"
    }
  ],
  "count": 1
}
```

**Download a file:**
```bash
curl -O http://localhost:50060/api/v1/files/{file_id}
```

## File Size Limits

The default max upload size is configured in the server settings (typically 50 MB).
Files exceeding the limit will return `413 Payload Too Large`.
