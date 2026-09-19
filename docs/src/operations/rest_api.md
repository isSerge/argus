# REST API

Argus includes a built-in REST API server for system introspection. This API provides a way to observe the state of the running application, such as its health and the configuration of its active monitors.

## Configuring the API

The API server provides both read-only and secured write endpoints. To configure it, you must set up the `server` section in your [`app.yaml`](../user_guide/app_yaml.md).

```yaml
# in configs/app.yaml
server:
  # The address and port for the server to listen on.
  listen_address: "0.0.0.0:8080"
  # API key for securing write endpoints (or set ARGUS_API_KEY env var)
  api_key: "your-secret-api-key-here"
```

**Important:** An API key is **required** for write operations. If no `api_key` is configured and the `ARGUS_API_KEY` environment variable is not set, the server will refuse to start.

Once configured, the API endpoints will be available at the specified `listen_address`.

## Authentication

The API uses **Bearer token authentication** for write operations. Read-only endpoints (like `GET /health`, `GET /status`, `GET /monitors`) do not require authentication.

### Write Endpoints (Require Authentication)
- `POST /monitors` - Create a new monitor
- `PUT /monitors/{id}` - Update an existing monitor
- `DELETE /monitors/{id}` - Delete a monitor
- `POST /actions` - Create a new action
- `PUT /actions/{id}` - Update an existing action
- `DELETE /actions/{id}` - Delete an action
- `POST /abis` - Upload a new ABI
- `DELETE /abis/{name}` - Delete an ABI

### Authentication Header

For write operations, include the API key in the `Authorization` header:

```bash
# Example authenticated request
curl -X POST http://localhost:8080/monitors \
  -H "Authorization: Bearer your-secret-api-key-here" \
  -H "Content-Type: application/json" \
  -d '{...}'
```

### Error Responses

**Unauthorized (`401 Unauthorized`)**
```json
{
  "error": "Unauthorized"
}
```

This error occurs when:
- No `Authorization` header is provided for a write endpoint
- The bearer token doesn't match the configured API key
- An invalid authorization format is used

## API Endpoints

### Health Check

-   **`GET /health`**

    Provides a simple health check of the API server.

    **Success Response (`200 OK`)**
    ```json
    {
      "status": "ok"
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/health
    ```

### Application Status

-   **`GET /status`**

    Retrieves the current status and metrics of the application.

    **Success Response (`200 OK`)**
    ```json
    {
      "version": "0.1.0",
      "network_id": "ethereum",
      "uptime_secs": 3600,
      "latest_processed_block": 18345678,
      "latest_processed_block_timestamp_secs": 1698382800,
      "reorgs_detected": 0
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/status
    ```

### List All Monitors

-   **`GET /monitors`**

    Retrieves a list of all monitors currently loaded and active in the application for the configured network.

    **Success Response (`200 OK`)**
    ```json
    {
      "monitors": [
        {
          "id": 1,
          "name": "Large ETH Transfers",
          "network": "ethereum",
          "address": null,
          "abi_name": null,
          "filter_script": "tx.value > ether(10)",
          "actions": [
            "my-webhook"
          ],
          "created_at": "2023-10-27T10:00:00Z",
          "updated_at": "2023-10-27T10:00:00Z"
        },
        {
          "id": 2,
          "name": "Large USDC Transfers",
          "network": "ethereum",
          "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
          "abi_name": "usdc",
          "filter_script": "log.name == \"Transfer\" && log.params.value > usdc(1000000)",
          "actions": [
            "slack-notifications"
          ],
          "created_at": "2023-10-27T10:00:00Z",
          "updated_at": "2023-10-27T10:00:00Z"
        }
      ]
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/monitors
    ```

### Get a Specific Monitor

-   **`GET /monitors/{id}`**

    Retrieves the full configuration of a single monitor by its unique ID.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the monitor.

    **Success Response (`200 OK`)**
    ```json
    {
      "monitor": {
        "id": 1,
        "name": "Large ETH Transfers",
        "network": "ethereum",
        "address": null,
        "abi_name": null,
        "filter_script": "tx.value > ether(10)",
        "actions": [
          "my-webhook"
        ],
        "created_at": "2023-10-27T10:00:00Z",
        "updated_at": "2023-10-27T10:00:00Z"
      }
    }
    ```

    **Error Response (`404 Not Found`)**
    If no monitor with the specified ID exists.
    ```json
    {
      "error": "Monitor not found"
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/monitors/1
    ```

### Create a Monitor

-   **`POST /monitors`**

    Creates a new monitor. The request body must be a JSON object representing the monitor configuration. Requires authentication.

    **Request Body:**
    ```json
    {
      "name": "my-new-monitor",
      "network": "ethereum",
      "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
      "abi_name": "erc20",
      "filter_script": "log.name == \"Transfer\"",
      "actions": [
        "my-webhook"
      ]
    }
    ```

    **Note:** For transaction-level monitors (not contract-specific), set both `address` and `abi_name` to `null`.

    **Success Response (`201 Created`)**
    ```json
    {
      "status": "Monitor creation triggered"
    }
    ```

    **Error Responses:**
    - `400 Bad Request`: If the request body is malformed.
    - `409 Conflict`: If a monitor with the same name already exists on this network.
    - `422 Unprocessable Entity`: If the monitor configuration is invalid (e.g., invalid filter script syntax, missing required fields, nonexistent ABI or actions).

    **Example Usage:**
    ```bash
    curl -X POST http://localhost:8080/monitors \
      -H "Authorization: Bearer your-secret-api-key-here" \
      -H "Content-Type: application/json" \
      -d '{
        "name": "my-new-monitor",
        "network": "ethereum",
        "address": null,
        "abi_name": null,
        "filter_script": "tx.value > ether(10)",
        "actions": ["my-webhook"]
      }'
    ```

### Update a Monitor

-   **`PUT /monitors/{id}`**

    Updates an existing monitor by its ID. The request body must be a complete JSON object for the monitor. Requires authentication.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the monitor to update.

    **Request Body:**
    ```json
    {
      "name": "my-updated-monitor",
      "network": "ethereum",
      "address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
      "abi_name": "erc20",
      "filter_script": "log.name == \"Transfer\" && log.params.value > usdc(100000)",
      "actions": [
        "updated-webhook"
      ]
    }
    ```

    **Success Response (`200 OK`)**
    ```json
    {
      "status": "Monitors update triggered"
    }
    ```

    **Error Responses:**
    - `404 Not Found`: If no monitor with the specified ID exists.
    - `409 Conflict`: If the new name conflicts with another existing monitor on this network.
    - `422 Unprocessable Entity`: If the updated monitor configuration is invalid (e.g., invalid filter script, nonexistent ABI or actions).

    **Example Usage:**
    ```bash
    curl -X PUT http://localhost:8080/monitors/1 \
      -H "Authorization: Bearer your-secret-api-key-here" \
      -H "Content-Type: application/json" \
      -d '{
        "name": "my-updated-monitor",
        "network": "ethereum",
        "address": null,
        "abi_name": null,
        "filter_script": "tx.value > ether(100)",
        "actions": ["my-webhook"]
      }'
    ```

### Delete a Monitor

-   **`DELETE /monitors/{id}`**

    Deletes a monitor by its unique ID. Requires authentication.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the monitor to delete.

    **Success Response (`204 No Content`)**
    ```json
    {
      "status": "Monitor deletion triggered"
    }
    ```

    **Error Response:**
    - `404 Not Found`: If no monitor with the specified ID exists.

    **Example Usage:**
    ```bash
    curl -X DELETE http://localhost:8080/monitors/1 \
      -H "Authorization: Bearer your-secret-api-key-here"
    ```

### Update Monitor Status

-   **`PATCH /monitors/{id}/status`**

    Updates the status of an existing monitor by its ID. Requires authentication.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the monitor to update.

    **Request Body:**
    ```json
    {
      "status": "paused" 
    }
    ```
    or
    ```json
    {
      "status": "active" 
    }
    ```

    **Success Response (`200 OK`)**
    ```json
    {
      "status": "Monitor status update triggered"
    }
    ```

    **Error Responses:**
    - `404 Not Found`: If no monitor with the specified ID exists.
    - `400 Bad Request`: If the request body is malformed or the status is invalid.

    **Example Usage:**
    ```bash
    curl -X PATCH http://localhost:8080/monitors/1/status \
      -H "Authorization: Bearer your-secret-api-key-here" \
      -H "Content-Type: application/json" \
      -d '{"status": "paused"}'
    ```

### List All Actions

-   **`GET /actions`**

    Retrieves a list of all actions currently loaded and active in the application.

    **Success Response (`200 OK`)**
    ```json
    {
      "actions": [
        {
          "id": 1,
          "name": "my-webhook",
          "webhook": {
            "url": "https://webhook.site/your-unique-url",
            "method": "POST",
            "headers": {
              "Content-Type": "application/json"
            },
            "message": {
              "title": "Large ETH Transfer Detected",
              "body": "- **Amount**: {{ tx.value | ether }} ETH\n- **From**: `{{ tx.from }}`\n- **To**: `{{ tx.to }}`\n- **Tx Hash**: `{{ transaction_hash }}`"
            }
          },
        },
        {
          "id": 2,
          "name": "slack-notifications",
          "slack": {
            "slack_url": "https://hooks.slack.com/services/T0000/B0000/XXXXXXXX",
            "message": {
              "title": "Large USDC Transfer Detected",
              "body": "A transfer of over 1,000,000 USDC was detected.\n<https://etherscan.io/tx/{{ transaction_hash }}|View on Etherscan>"
            }
          },
          "policy": {
            "throttle": {
              "max_count": 5,
              "time_window_secs": 60
            }
          }
        }
      ]
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/actions
    ```

### Get a Specific Action

-   **`GET /actions/{id}`**

    Retrieves the full configuration of a single action by its unique ID.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the action.

    **Success Response (`200 OK`)**
    ```json
    {
      "action": {
        "id": 1,
        "name": "my-webhook",
        "webhook": {
          "url": "https://webhook.site/your-unique-url",
          "method": "POST",
          "headers": {
            "Content-Type": "application/json"
          },
          "message": {
            "title": "Large ETH Transfer Detected",
            "body": "- **Amount**: {{ tx.value | ether }} ETH\n- **From**: `{{ tx.from }}`\n- **To**: `{{ tx.to }}`\n- **Tx Hash**: `{{ transaction_hash }}`"
          }
        },
      }
    }
    ```

    **Error Response (`404 Not Found`)**
    If no action with the specified ID exists.
    ```json
    {
      "error": "Action not found"
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/actions/1
    ```

### Create an Action

-   **`POST /actions`**

    Creates a new action. The request body must be a JSON object representing the action configuration. Requires authentication.

    **Request Body:**
    ```json
    {
      "name": "my-new-webhook",
      "webhook": {
        "url": "https://example.com/webhook",
        "message": {
          "title": "New Event",
          "body": "Details: {{ transaction_hash }}"
        }
      }
    }
    ```

    **Success Response (`201 Created`)**
    The newly created action object, including its assigned ID.
    ```json
    {
      "action": {
        "id": 3,
        "name": "my-new-webhook",
        "webhook": { ... }
      }
    }
    ```

    **Error Responses:**
    - `400 Bad Request`: If the request body is malformed.
    - `409 Conflict`: If an action with the same name already exists.
    - `422 Unprocessable Entity`: If the action configuration is invalid (e.g., missing required fields for a webhook).

    **Example Usage:**
    ```bash
    curl -X POST http://localhost:8080/actions \
      -H "Authorization: Bearer your-secret-api-key-here" \
      -H "Content-Type: application/json" \
      -d '{"name": "my-new-webhook", "webhook": {"url": "https://example.com/webhook", "message": {"title": "New Event", "body": "Details: {{ transaction_hash }}"}}}'
    ```

### Update an Action

-   **`PUT /actions/{id}`**

    Updates an existing action by its ID. The request body must be a complete JSON object for the action. Requires authentication.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the action to update.

    **Request Body:**
    ```json
    {
      "name": "my-updated-webhook",
      "webhook": {
        "url": "https://example.com/new-webhook",
        "message": {
          "title": "Updated Event",
          "body": "Updated Details: {{ transaction_hash }}"
        }
      }
    }
    ```

    **Success Response (`200 OK`)**
    The updated action object.
    ```json
    {
      "action": {
        "id": 3,
        "name": "my-updated-webhook",
        "webhook": { ... }
      }
    }
    ```

    **Error Responses:**
    - `404 Not Found`: If no action with the specified ID exists.
    - `409 Conflict`: If the new name conflicts with another existing action.
    - `422 Unprocessable Entity`: If the updated action configuration is invalid.

    **Example Usage:**
    ```bash
    curl -X PUT http://localhost:8080/actions/3 \
      -H "Authorization: Bearer your-secret-api-key-here" \
      -H "Content-Type: application/json" \
      -d '{"name": "my-updated-webhook", "webhook": {"url": "https://example.com/new-webhook", "message": {"title": "Updated Event", "body": "Updated Details: {{ transaction_hash }}"}}}'
    ```

### Delete an Action

-   **`DELETE /actions/{id}`**

    Deletes an action by its unique ID. Requires authentication.

    **URL Parameters:**
    - `id` (integer, required): The unique ID of the action to delete.

    **Success Response (`204 No Content`)**
    An empty response on successful deletion.

    **Error Responses:**
    - `404 Not Found`: If no action with the specified ID exists.
    - `409 Conflict`: If the action is currently in use by one or more monitors and cannot be deleted. The response body will include the names of the monitors using the action.
      ```json
      {
        "error": "Action is in use and cannot be deleted.",
        "monitors": [
          "Monitor Name 1",
          "Monitor Name 2"
        ]
      }
      ```

    **Example Usage:**
    ```bash
    curl -X DELETE http://localhost:8080/actions/3 \
      -H "Authorization: Bearer your-secret-api-key-here"
    ```

#### Upload an ABI

-   **`POST /abis`**

    Uploads a new contract ABI. The request body must be a JSON object containing the ABI's `name` and the `abi` content as a string. Requires authentication.

    **Request Body:**
    ```json
    {
      "name": "MyContractABI",
      "abi": "[{\"type\":\"function\",\"name\":\"transfer\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}]}]"
    }
    ```

    **Success Response (`201 Created`)**
    The newly created ABI object.
    ```json
    {
      "abi": {
        "name": "MyContractABI",
        "abi": "[{\"type\":\"function\",\"name\":\"transfer\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}]}]"
      }
    }
    ```

    **Error Responses:**
    - `400 Bad Request`: If the request body is malformed.
    - `409 Conflict`: If an ABI with the same name already exists.
    - `422 Unprocessable Entity`: If the provided `abi` content is not valid JSON.

    **Example Usage:**
    ```bash
    curl -X POST http://localhost:8080/abis \
      -H "Authorization: Bearer your-secret-api-key-here" \
      -H "Content-Type: application/json" \
      -d '{"name": "MyContractABI", "abi": "[{\"type\":\"function\",\"name\":\"transfer\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}]}]"}'
    ```

#### Get an ABI by Name

-   **`GET /abis/{name}`**

    Retrieves the content of a specific ABI by its name.

    **URL Parameters:**
    - `name` (string, required): The name of the ABI.

    **Success Response (`200 OK`)**
    The ABI object.
    ```json
    {
      "abi": {
        "name": "MyContractABI",
        "abi": "[{\"type\":\"function\",\"name\":\"transfer\",\"inputs\":[{\"name\":\"to\",\"type\":\"address\"},{\"name\":\"amount\",\"type\":\"uint256\"}]}]"
      }
    }
    ```

    **Error Response (`404 Not Found`)**
    If no ABI with the specified name exists.
    ```json
    {
      "error": "ABI not found"
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/abis/MyContractABI
    ```

#### List All ABIs

-   **`GET /abis`**

    Retrieves a list of all available ABI names.

    **Success Response (`200 OK`)**
    ```json
    {
      "abis": [
        "ERC20",
        "MyContractABI",
        "WETH"
      ]
    }
    ```

    **Example Usage:**
    ```bash
    curl http://localhost:8080/abis
    ```

#### Delete an ABI

-   **`DELETE /abis/{name}`**

    Deletes an ABI by its name. Requires authentication.

    **URL Parameters:**
    - `name` (string, required): The name of the ABI to delete.

    **Success Response (`204 No Content`)**
    An empty response on successful deletion.

    **Error Responses:**
    - `404 Not Found`: If no ABI with the specified name exists.
    - `409 Conflict`: If the ABI is currently in use by one or more monitors and cannot be deleted. The response body will include the names of the monitors using the ABI.
      ```json
      {
        "error": "ABI is in use and cannot be deleted.",
        "monitors": [
          "Monitor Name 1",
          "Monitor Name 2"
        ]
      }
      ```

    **Example Usage:**
    ```bash
    curl -X DELETE http://localhost:8080/abis/MyContractABI \
      -H "Authorization: Bearer your-secret-api-key-here"
    ```

