# Wyze Sense to MQTT Bridge (Rust): Web Control Interface Specification

This document outlines the design, REST API endpoints, and dashboard layout for the **Wyze Sense to MQTT Bridge (Rust) Web Control Interface**. The Web UI replaces and improves upon the interactive command-line tool, providing a unified dashboard for list, pair, unpair, fix, and diagnostic raw packet commands.

---

## 1. System Architecture

The Web Gateway runs as a separate binary task `bridge_web` using the **`axum`** web framework. It communicates with the `Engine` via a shared `Arc<tokio::sync::Mutex<Engine<T>>>` handle.

```
          +---------------------------------------------------+
          |                    Web Browser                    |
          |  - Dynamic Paired Sensors Table                   |
          |  - Scan & Pairing Wizard                          |
          |  - Raw Diagnostic Console Terminal                |
          +-------------------------+-------------------------+
                                    | HTTP (JSON)
                                    v
+----------------------------------+----------------------------------+
|               Wyze Sense to MQTT Bridge (Rust) Daemon               |
|                                                                     |
|   +---------------------------+      +--------------------------+   |
|   |     Axum HTTP Server      |      |      Engine Actor        |   |
|   |      (bridge_web)         |      |      (State Manager)     |   |
|   +-------------+-------------+      +------------+-------------+   |
|                 |                                 ^                 |
|                 +--- Shared Arc<Mutex<Engine>> ---+                 |
|                                                   |                 |
|                                                   v                 |
|                                      +------------+-------------+   |
|                                      |      USB HID Transport   |   |
|                                      +--------------------------+   |
+---------------------------------------------------------------------+
```

---

## 2. REST API Specifications

All endpoints return JSON payloads and conform to the following paths:

### 2.1 Dongle State
*   **`GET /api/dongle`**
    *   **Description**: Returns status metadata of the connected USB dongle.
    *   **Response (`200 OK`)**:
        ```json
        {
          "connected": true,
          "mac": "MACADDR1",
          "version": "V1.0.0"
        }
        ```

### 2.2 Paired Sensors
*   **`GET /api/sensors`**
    *   **Description**: Retrieves the list of paired sensor MAC addresses from the dongle.
    *   **Response (`200 OK`)**:
        ```json
        {
          "sensors": ["SENSO001", "SENSO002"]
        }
        ```

*   **`DELETE /api/sensors/:mac`**
    *   **Description**: Deletes/unpairs a sensor.
    *   **Response (`200 OK`)**:
        ```json
        {
          "success": true,
          "message": "Sensor SENSO001 successfully deleted"
        }
        ```

### 2.3 Pairing & Scans
*   **`POST /api/scan`**
    *   **Description**: Enables or disables sensor scan pairing mode on the dongle.
    *   **Request Body**:
        ```json
        {
          "enable": true
        }
        ```
    *   **Response (`200 OK`)**:
        ```json
        {
          "scan_active": true
        }
        ```

*   **`POST /api/verify`**
    *   **Description**: Verifies and binds a scanned sensor.
    *   **Request Body**:
        ```json
        {
          "mac": "SENSO001",
          "sensor_type": "ContactV1"
        }
        ```
    *   **Response (`200 OK`)**:
        ```json
        {
          "success": true,
          "message": "Sensor SENSO001 successfully verified"
        }
        ```

### 2.4 Maintenance & Diagnostics
*   **`POST /api/fix`**
    *   **Description**: Scans paired list and purges invalid/ghost sensors (e.g. non-alphanumeric).
    *   **Response (`200 OK`)**:
        ```json
        {
          "purged_count": 2,
          "purged_macs": ["00000000", "ABC\0\0\0"]
        }
        ```

*   **`POST /api/chime/:mac`**
    *   **Description**: Triggers a chime alarm on a sensor.
    *   **Response (`200 OK`)**:
        ```json
        {
          "success": true
        }
        ```

*   **`POST /api/raw`**
    *   **Description**: Diagnostic endpoint writing raw bytes to the transport line and capturing the reply.
    *   **Request Body**:
        ```json
        {
          "bytes": [170, 85, 67, 3, 4, 1, 73]
        }
        ```
    *   **Response (`200 OK`)**:
        ```json
        {
          "response_bytes": [85, 170, 67, 11, 5, 77, 65, 67, 65, 68, 68, 82, 49, 3, 111]
        }
        ```

---

## 3. UI Layout & Static Assets

To ensure single-binary, zero-dependency deployment, the static front-end files (HTML, JS, CSS) will be packed directly into the Rust binary using standard macro resources:
`const HTML_CONTENT: &str = include_str!("../static/index.html");`

### UI Dashboard Components (packed in `index.html`):
1.  **Dongle Status Header**: A green/red status badge showing connection.
2.  **Scanning Panel**: An interactive wizard with a progress spinner. When scan is active, it dynamically displays discovered sensors.
3.  **Active Sensors Table**: Shows paired MAC addresses, sensor type, with quick actions to test chime or trigger unpairing.
4.  **Diagnostics Hex Console**:
    - A code editor box accepting hexadecimal sequences (e.g. `AA,55,43,03,04,01,49`).
    - A "Send Hex" button.
    - A terminal output panel showing logged transactions.
