# Parking-Lot IoT System — Technical Design Document

**Document status:** Design baseline / development-stage architecture  
**Revision:** 1.0  
**Date:** 26 September 2026  
**Development platform:** Arch Linux  
**Target deployment:** Raspberry Pi  

---

## 1. Purpose and Scope

This document consolidates the technical design developed so far for the parking-lot IoT system. It records both the current implementation baseline and the design process that produced it.

The system is intended to connect parking-space sensor/camera nodes to a central host, persist operational information, expose video to applications, and provide the foundation for customer and manager workflows, including parking timers and payment-controlled gate operation.

The document deliberately describes architecture, interfaces, data models, protocols, hardware, processing, security, deployment, and design iterations rather than reproducing source-code listings.

### 1.1 Current implementation scope

The implemented baseline currently covers:

- ESP32-CAM slave connectivity over authenticated WebSocket Secure (WSS).
- Parking sensor reporting from a digital IR sensor.
- RHYX M21-45 camera acquisition as raw RGB565 frames.
- Optional camera operation and a placeholder-video mode.
- A single WSS channel carrying control/status metadata and binary video frames.
- Python host-side WSS processing.
- PostgreSQL persistence for registered devices and events.
- Device authentication using stored token hashes.
- Host-side conversion of RGB565 frames to JPEG.
- Filesystem storage of the latest JPEG frame per device.
- Development HTTP serving of generated JPEG frames.
- A development-machine workflow intended to migrate to Raspberry Pi without fundamental application redesign.

### 1.2 Planned system scope

The broader system design also establishes the foundation for:

- A customer-facing parking dashboard.
- Customer identification by license plate.
- Customer access restricted to the corresponding parking spot/data.
- Live/latest video display and parking timer information.
- Manager access to all parking nodes.
- Slave registration and management.
- Payment infrastructure.
- Gate/servo control after successful payment.
- Vehicle detection using computer vision.
- License-plate extraction and OCR.
- Expansion of the event/database model for payment and operational events.

These items are architectural targets rather than claims that all of them are already implemented.

---

# 2. Design Objectives

The design evolved around the following technical objectives:

1. **Use inexpensive distributed embedded nodes.** Each parking location can have an ESP32-based slave.
2. **Maintain a persistent central state.** Device identity, authorization, status, and events belong in a database rather than in ad-hoc flat files.
3. **Use one authenticated transport.** Sensor/status/control messages and video should travel through the same WSS connection.
4. **Support imperfect camera availability.** The node must continue operating when the physical camera is absent or disabled.
5. **Accommodate the RHYX M21-45's raw RGB565 output.** Video processing therefore moves partly to the host.
6. **Separate embedded acquisition from higher-level processing.** The ESP32 captures and transports; the host converts, stores, serves, and eventually analyzes video.
7. **Allow development on a conventional Linux machine before Raspberry Pi deployment.** Application interfaces should not depend on development-machine-specific behavior.
8. **Provide a path toward a full parking-management platform.** The current architecture must be extensible to users, payments, gate control, computer vision, and additional event types.

---

# 3. System-Level Architecture

## 3.1 Current architecture

The system is organized into three primary layers:

### Embedded layer

Each parking-space slave contains:

- ESP32-CAM-class controller.
- RHYX M21-45 camera.
- Digital IR parking sensor.
- Wi-Fi connectivity.
- TLS/WSS client.
- Local camera/placeholder selection logic.
- Device identity and authentication token.

### Host/service layer

The central host currently provides:

- Python WSS server.
- TLS termination.
- Device authentication against PostgreSQL.
- Sensor/status/event persistence.
- Binary video reception.
- RGB565-to-JPEG conversion.
- Latest-frame filesystem storage.
- Development HTTP delivery of JPEG frames.

The intended final host is a Raspberry Pi, while development is currently performed on an Arch Linux machine.

### Application layer

The planned application layer will provide:

- Customer login/identification.
- Parking-spot status.
- Video.
- Timer information.
- Manager administration.
- Device registration.
- Payment handling.
- Gate-control commands.
- Later computer-vision and license-plate functionality.

## 3.2 Logical data flow

A normal camera-enabled cycle is:

**Camera → ESP32 framebuffer → RGB565 binary frame → WSS/TLS → Python host → RGB565 decoder → JPEG → filesystem/application → HTTP/dashboard**

Sensor/status information follows a parallel metadata path:

**IR sensor / device state → JSON metadata → WSS/TLS → authentication and event handling → PostgreSQL**

Commands travel in the reverse direction:

**Manager/application → host → authenticated WSS connection → ESP32 → local actuator/control logic**

---

# 4. Design Process and Major Iterations

The architecture was not designed as a single fixed implementation. It was developed through successive hardware, communication, persistence, and video-processing decisions.

## 4.1 Iteration 1 — Distributed parking slave concept

The initial concept was to place an embedded node at each parking location and have a central host coordinate those nodes.

The early system requirements included:

- Parking-space occupancy sensing.
- Camera/video availability.
- Central monitoring.
- A future customer interface.
- A future manager interface.
- Payment-driven gate operation.

This established the slave/host model rather than treating the ESP32 as a standalone parking application.

## 4.2 Iteration 2 — ESP32-CAM as the parking slave

The embedded platform was consolidated around an ESP32-CAM-class board. This provided Wi-Fi, camera connectivity, and enough local processing to handle acquisition and protocol communication.

The design deliberately keeps application-heavy processing on the host instead of making the ESP32 responsible for database operations or complex computer vision.

## 4.3 Iteration 3 — Digital IR sensor integration

The parking sensor was implemented as a digital IR input. The current hardware interface uses GPIO13 on the ESP32 slave.

The sensor is treated as an event/status source rather than as a database in itself. The host receives the state and records meaningful events centrally.

The physical sensor hardware evolved during development; the current design uses the RHYX M21-45 for imaging while the IR sensor remains the parking-state input.

## 4.4 Iteration 4 — WebSocket Secure transport

A WSS architecture was selected so that each slave maintains a persistent authenticated connection to the host.

The design moved away from separate communication channels for sensor information and video. The same authenticated WSS connection carries:

- Device metadata.
- Sensor/status messages.
- Control commands.
- Video-frame metadata.
- Binary video payloads.

This reduces the number of network services required on the embedded node and gives the host a single device session to associate with authentication and state.

## 4.5 Iteration 5 — Authentication and device identity

The system introduced explicit device identifiers and per-device tokens.

A device is represented centrally by a unique `device_id`. The token is not stored as plaintext in the database; the host stores a SHA-256 token hash and hashes the supplied token during authentication.

A disabled device cannot authenticate even if it possesses a valid token.

This changed the system from an open telemetry server into an authenticated device-management architecture.

## 4.6 Iteration 6 — Flat-file persistence replaced by PostgreSQL

The earlier host design used JSON/JSONL-style local persistence for devices, sensors, and video records. As the system expanded toward multiple slaves, management, events, and payments, this became an inadequate central data model.

PostgreSQL was therefore introduced as the authoritative structured datastore.

The resulting separation is:

- **PostgreSQL:** device identity, authorization state, events, timestamps, metadata.
- **Filesystem:** latest binary-derived JPEG frame files.
- **WSS:** real-time transport.
- **HTTP/application layer:** video consumption.

This is an important architectural boundary: high-volume frame bytes are not inserted into PostgreSQL as the primary video-storage mechanism.

## 4.7 Iteration 7 — Camera availability and placeholder mode

Camera hardware was not always available during development. The firmware therefore gained an explicit distinction between:

- Camera video source.
- Placeholder video source.

The placeholder mode permits end-to-end WSS and host testing even when the camera is not installed or is intentionally disabled.

The host therefore receives a consistent video-frame workflow while being able to identify whether the source was the physical camera or the placeholder.

## 4.8 Iteration 8 — RHYX M21-45 and raw RGB565

The camera requirement changed from an assumed JPEG-oriented pipeline to the RHYX M21-45's raw RGB565 output.

The ESP32 camera configuration was consequently changed to:

- RGB565 pixel format.
- QVGA resolution.
- 320 × 240 pixels.
- Two bytes per pixel.
- 153,600 raw bytes per frame.

For a raw frame, the host must therefore perform the conversion to a browser-friendly/compressed image format.

The use of QVGA is significant because raw RGB565 frames become large quickly. The design remains conservative about resolution because the ESP32-camera environment is constrained when operating with non-JPEG formats.

## 4.9 Iteration 9 — Host-side RGB565-to-JPEG conversion

Instead of making the ESP32 perform JPEG compression, the host was updated to decode RGB565 and encode JPEG using Pillow.

This creates the following boundary:

**ESP32:** capture and transport raw pixels.  
**Host:** pixel-format conversion, compression, storage, and serving.

The current host assumes the Espressif RGB565 framebuffer byte order is big-endian/MSB-first. The converter can accommodate the alternative byte ordering if hardware testing demonstrates that the incoming stream needs it.

The implementation was validated with synthetic RGB565 frames for primary colors before moving toward physical-camera testing.

## 4.10 Iteration 10 — HTTP serving of generated JPEGs

A lightweight HTTP server was added for development access to the generated frames.

The current development endpoint follows the form:

`http://HOST:8080/frames/<device_id>.jpg`

The WSS server remains responsible for receiving the real-time stream, while HTTP provides a simple way for a browser or later dashboard to consume the latest converted image.

This is intentionally a development-oriented separation rather than the final public application architecture.

---

# 5. Embedded Slave Design

## 5.1 Controller

The slave is based on an ESP32-CAM-class board. The existing implementation targets the AI-Thinker camera pin arrangement and uses the ESP32 camera driver.

The design uses PSRAM when available for camera frame buffers.

## 5.2 Camera

Current camera:

- **Model:** RHYX M21-45.
- **Pixel format:** RGB565.
- **Resolution:** QVGA.
- **Frame dimensions:** 320 × 240.
- **Raw frame size:** 153,600 bytes.
- **Frame rate target:** one frame approximately every 100 ms in the current configuration, corresponding to a nominal 10 FPS transmission schedule.

The actual sustainable rate is dependent on camera acquisition, Wi-Fi throughput, TLS overhead, frame conversion/host processing, and device conditions.

## 5.3 Camera framebuffer strategy

With PSRAM, the design uses:

- Frame buffers in PSRAM.
- Two frame buffers.
- Latest-frame acquisition behavior.

Without PSRAM, the design falls back to:

- Frame buffer in internal DRAM.
- One frame buffer.
- Empty-buffer acquisition behavior.

This allows the firmware to remain operational across configurations with different memory availability, although the raw-video workload is more demanding without PSRAM.

## 5.4 Sensor

The parking IR sensor's digital output is connected to:

- **GPIO13** on the current slave firmware.

The firmware reports the sensor state to the host as structured metadata/events rather than relying on the host to infer the electrical state from a video image.

## 5.5 Device identity

Each slave has:

- A unique `DEVICE_ID`.
- A device authentication token.

The current development identity is `PARKING-ESP32-001`. Authentication credentials are configuration secrets and are intentionally excluded from this design document.

## 5.6 Wi-Fi

The slave connects to a configured Wi-Fi network and establishes the WSS connection to the host using the host's IP address, port, and path.

Development currently uses a private LAN. The architecture is designed so that the host address can later point to the Raspberry Pi without changing the application model.

## 5.7 WSS client

The embedded WSS implementation uses the installed WebSockets library version 2.7.2.

The selected TLS connection pattern uses a CA certificate through `beginSslWithCA` because the library version in use does not provide the desired `setCACert` API.

Connection behavior includes:

- Automatic reconnection interval: 5 seconds.
- Heartbeat interval: 15 seconds.
- Heartbeat timeout: 3 seconds.
- Allowed missed heartbeats: 2.

The TLS channel is authenticated using the configured CA and the device/application authentication mechanism.

---

# 6. WSS Protocol Design

## 6.1 Connection endpoint

The current endpoint is:

- **Transport:** WSS.
- **Port:** 8766.
- **Path:** `/parking`.

The connection is intended to remain persistent for the lifetime of a connected parking slave.

## 6.2 Message categories

The protocol contains several logical message categories:

### Device/status metadata

Used for identifying the device, communicating state, and maintaining host-side knowledge of the node.

### Sensor events

Used to report the parking IR sensor state and associated event information.

### Video-frame metadata

Sent before a binary frame so the receiver can interpret the following bytes. Current metadata identifies, at minimum:

- Device ID.
- Video source.
- Pixel format.
- Width.
- Height.
- Raw byte count.
- Frame ID.
- Timestamp.

For RGB565 frames, byte order can also be represented explicitly.

### Control commands

The host can send commands to change the video source. Current logical commands include:

- Select physical camera.
- Select placeholder.

The protocol is intended to expand later for gate/servo commands and other management operations.

## 6.3 Binary video framing

The protocol deliberately separates video metadata from the binary frame payload.

For RGB565:

`320 × 240 × 2 = 153,600 bytes/frame`

At a nominal 10 FPS, the uncompressed payload alone is approximately:

`1,536,000 bytes/s` or approximately `12.3 Mbit/s`

before WebSocket, TLS, Wi-Fi, and protocol overhead.

This bandwidth calculation is one of the principal reasons that raw RGB565 streaming must remain a controlled development/design choice and may require a lower frame rate, lower resolution, or host/network optimization for production deployment.

## 6.4 Maximum message size

The host WSS server currently permits messages up to 5 MiB. This provides substantial headroom above the 153,600-byte RGB565 frame size and accommodates metadata and future frame-size changes within reasonable limits.

---

# 7. Video Processing Architecture

## 7.1 Why JPEG conversion is performed on the host

The current architecture transfers raw RGB565 from the ESP32 and performs compression centrally.

Advantages of this split include:

- Lower image-processing responsibility on the ESP32.
- Direct preservation of camera output.
- Simpler embedded acquisition path.
- Centralized control of JPEG quality.
- A reusable host-side image pipeline for future computer vision.

The trade-off is increased network bandwidth between slave and host.

## 7.2 RGB565 representation

RGB565 allocates 16 bits per pixel:

- 5 bits red.
- 6 bits green.
- 5 bits blue.

The current host conversion assumes the camera framebuffer uses the big-endian byte order documented by the Espressif camera/image-conversion ecosystem.

The converter can be configured for little-endian data if physical testing indicates a byte-order mismatch.

## 7.3 JPEG output

The host uses Pillow to convert the RGB565 image to RGB and then encode JPEG.

Current conversion parameters:

- JPEG quality: 85.
- JPEG optimization enabled.
- Output dimensions preserved at 320 × 240.

The JPEG size is recorded separately from the raw frame size so that compression behavior can be monitored.

## 7.4 Placeholder frames

Placeholder frames remain JPEG images.

This creates two host input cases:

| Source | Input format | Host action | Stored format |
|---|---|---|---|
| Physical camera | RGB565 | Decode + JPEG encode | JPEG |
| Placeholder | JPEG | Validate + store | JPEG |

This avoids requiring the placeholder mechanism to emulate the physical camera's raw pixel pipeline.

## 7.5 Latest-frame storage

The host maintains a latest-frame representation per device and writes the latest JPEG under:

`data/frames/<device_id>.jpg`

The design currently emphasizes the latest image rather than creating a permanent video archive.

A future historical-video requirement would need a separate storage strategy because storing every JPEG independently can grow rapidly.

---

# 8. HTTP Video Service

## 8.1 Current development service

A lightweight HTTP server listens on:

- Host: `0.0.0.0`.
- Port: `8080`.

It serves the latest JPEG frame for a device using a path of the form:

`/frames/<device_id>.jpg`

## 8.2 HTTP behavior

The development server supports:

- GET.
- HEAD.
- JPEG content type.
- No-cache headers.
- Cross-origin access for development use.
- Basic path-traversal protection by restricting the requested filename to a simple filename.

The server is intentionally lightweight and should not be considered the final public-facing security boundary.

## 8.3 Intended future role

In the final dashboard architecture, the video path can be integrated into the application/reverse-proxy layer rather than exposing the development HTTP service directly.

---

# 9. PostgreSQL Data Architecture

## 9.1 Database role

PostgreSQL is the authoritative store for structured system state and events.

The database replaces the earlier flat-file approach and provides the relational foundation required for multiple devices, event history, management, and future payment-related entities.

## 9.2 Devices entity

The `devices` table represents each ESP32 slave.

Current conceptual attributes are:

- Database primary key.
- Unique device ID.
- Human-readable name.
- Parking spot identifier.
- Token hash.
- Enabled/disabled state.
- Last-seen timestamp.
- JSON metadata.
- Creation timestamp.
- Update timestamp.

The device ID is the stable application-level identity, while the database identity is an internal relational key.

## 9.3 Events entity

The `events` table provides a generic event stream associated with devices.

Current attributes are:

- Event primary key.
- Referenced device.
- Event type.
- Event timestamp from the device/event source.
- Host reception timestamp.
- JSON payload.
- JSON metadata.

The event model is intentionally generic so that sensor, status, video-status, payment-related, and future operational events can coexist without repeatedly redesigning the base schema.

## 9.4 Indexing

Indexes currently exist for:

- Device ID in events.
- Event type.
- Event time.
- Reception time.

These support common operational queries such as device history, recent events, and event-type filtering.

## 9.5 Device authentication

Authentication follows this conceptual sequence:

1. Slave presents its device ID and token.
2. Host retrieves the corresponding device record.
3. Host checks whether the device is enabled.
4. Host hashes the presented token using SHA-256.
5. Host compares the supplied hash with the stored token hash.
6. The connection is accepted only when the checks succeed.

Plaintext device tokens are not stored in PostgreSQL.

## 9.6 Last-seen tracking

Successful device communication updates `last_seen` in the database. This provides a persistent indication of connectivity independent of the transient WebSocket session.

---

# 10. Host Software Architecture

## 10.1 Main responsibilities

The Python host currently combines several services around a shared device/session model:

1. TLS/WSS server.
2. Device authentication.
3. Real-time metadata handling.
4. Event persistence.
5. Binary frame reception.
6. RGB565 image conversion.
7. Latest JPEG storage.
8. Development HTTP image serving.
9. Device/session monitoring.

## 10.2 Database access layer

Database-specific operations are separated into a `db.py` layer conceptually responsible for:

- Opening PostgreSQL connections.
- Hashing device tokens.
- Looking up devices.
- Authenticating devices.
- Updating `last_seen`.
- Inserting events.
- Registering/updating devices.

This separation prevents the WSS protocol logic from containing the complete SQL implementation.

## 10.3 Device registration

A separate registration workflow is used to create or update a device record with:

- Device ID.
- Authentication token.
- Name.
- Parking spot.
- Optional metadata.

Registration enables the host to recognize a slave before it attempts normal operation.

## 10.4 Frame handling

The host's frame pipeline is:

1. Receive video metadata.
2. Receive binary frame.
3. Determine source and format.
4. For RGB565, validate expected dimensions and byte count.
5. Decode RGB565.
6. Encode JPEG.
7. Save the latest frame.
8. Update the in-memory latest-frame record.
9. Record relevant event/status information.

JPEG inputs bypass RGB565 conversion after basic JPEG validation.

---

# 11. Reliability and Fault Handling

## 11.1 Network disconnection

The ESP32 automatically attempts reconnection after the configured interval. Heartbeats detect stale sessions.

The database's `last_seen` value provides a persistent host-side indication of the last successful communication.

## 11.2 Camera unavailable

The placeholder source provides a controlled fallback when the physical camera is absent or disabled.

This was an explicit design response to development hardware availability and also provides a useful diagnostic mode.

## 11.3 Invalid RGB565 frames

The host validates:

- Positive dimensions.
- Supported byte order.
- Exact expected raw frame size.

A 320 × 240 RGB565 frame must contain exactly 153,600 bytes. Frames with an inconsistent length are rejected rather than decoded using guessed dimensions.

## 11.4 Unsupported formats

The host accepts the formats explicitly defined by the protocol. Unsupported video formats are rejected rather than silently interpreted.

## 11.5 Unknown or disabled devices

An unknown device cannot authenticate. A known device marked disabled is also denied access.

## 11.6 HTTP path security

The development image server restricts filenames to prevent straightforward path traversal through frame URLs.

---

# 12. Security Architecture

## 12.1 Transport security

WSS provides TLS encryption for traffic between the ESP32 and host.

The design therefore protects:

- Authentication information transmitted during connection setup/application authentication.
- Sensor/status metadata.
- Control messages.
- Video data.

## 12.2 Device authentication

TLS alone identifies the secure transport; the application also authenticates each device using its unique ID and token.

The database stores the token hash rather than the plaintext token.

## 12.3 Certificate trust

The ESP32 uses a configured root CA certificate to validate the server certificate.

The current embedded WebSockets library API constrained the implementation to the available `beginSslWithCA` connection mechanism.

## 12.4 Development HTTP limitations

The HTTP JPEG server currently uses unauthenticated HTTP for development. It should therefore be treated as a local-network development service, not as the final Internet-facing customer video endpoint.

The final application should place authentication and authorization around customer video access.

---

# 13. Planned Application Architecture

## 13.1 Customer workflow

The intended customer-facing flow is based on license-plate identification.

A customer should be able to access only the parking information associated with that vehicle/parking session, including:

- Assigned parking spot.
- Parking status.
- Timer/session information.
- Relevant video.
- Payment status and infrastructure.

The current device/event architecture is designed to provide the backend data needed for this workflow.

## 13.2 Manager workflow

The manager interface is intended to expose broader administrative functions:

- View all parking slaves.
- Register slaves.
- Manage device state.
- Monitor parking events.
- Manage timers/sessions.
- Receive or confirm payments.
- Trigger gate operation after payment.

## 13.3 Gate control

A future command path will extend the existing WSS control mechanism so that the host can send a gate/servo command to the appropriate slave after a payment event.

This should remain an authenticated device command and should generate an auditable database event.

---

# 14. Computer Vision and License-Plate Roadmap

A later stage will add vehicle detection and license-plate processing.

The expected conceptual pipeline is:

**Camera frame → vehicle detection → vehicle region → license-plate extraction → OCR → normalized plate identifier → parking/customer association**

The current architecture intentionally leaves this processing on the host side. The ESP32 is not expected to run the full detection/OCR pipeline.

The existing RGB565-to-JPEG conversion stage can become the input boundary for later computer-vision processing, although a production implementation may choose to process frames before JPEG compression to reduce unnecessary conversions.

---

# 15. Performance Considerations

## 15.1 Embedded bandwidth

Raw RGB565 at 320 × 240 requires 153,600 bytes per frame.

At 10 FPS, this produces approximately 1.536 MB/s of raw image data, or approximately 12.3 Mbit/s before protocol and transport overhead.

This is a significant load for an ESP32 Wi-Fi link and must be treated as an important system constraint.

## 15.2 Host CPU load

The current implementation performs RGB565-to-JPEG conversion inside the host's video-handling path using Pillow.

At high frame rates and with multiple slaves, repeated image conversion can become CPU-intensive, particularly on a Raspberry Pi.

The present design is therefore appropriate as a development baseline but leaves room for future optimization, including lower frame rates, reduced resolution, asynchronous conversion, worker threads/processes, or a different video transport strategy.

## 15.3 Storage

Only the latest JPEG is currently stored per device. This avoids uncontrolled storage growth from continuously recording frames.

If historical video becomes a requirement, storage retention, compression, segmentation, indexing, and deletion policies will need to be specified separately.

---

# 16. Development and Deployment Strategy

## 16.1 Development environment

The host is currently developed and tested on Arch Linux.

The development environment provides:

- Python runtime.
- PostgreSQL.
- TLS certificates for WSS development.
- Local network connectivity to the ESP32.
- Pillow for image conversion.

## 16.2 Raspberry Pi target

The same service architecture is intended to run on the Raspberry Pi.

The objective is to avoid application logic that depends on the development laptop. The primary deployment-specific changes should be configuration-level values such as:

- Host IP/interface.
- TLS certificate/key paths.
- Database credentials.
- Storage paths.
- Service startup configuration.

## 16.3 Required Python dependency

The current RGB565 host pipeline requires Pillow in addition to the PostgreSQL/WebSocket dependencies.

## 16.4 Database deployment

PostgreSQL is initialized on the development host and will later need equivalent initialization on the Raspberry Pi or another designated database host.

The database URL is configuration, not source-code data.

---

# 17. Current Interface Summary

| Interface | Current value / behavior |
|---|---|
| Embedded controller | ESP32-CAM-class board |
| Camera | RHYX M21-45 |
| Camera format | RGB565 |
| Camera resolution | QVGA, 320 × 240 |
| Raw frame size | 153,600 bytes |
| Video interval | 100 ms nominal |
| Parking sensor | Digital IR sensor |
| IR input | GPIO13 |
| Device transport | WSS |
| WSS port | 8766 |
| WSS path | `/parking` |
| WSS heartbeat | 15 s interval / 3 s timeout / 2 missed |
| Reconnect interval | 5 s |
| Host database | PostgreSQL |
| Device token storage | SHA-256 hash |
| Latest-frame storage | `data/frames/<device_id>.jpg` |
| RGB565 converter | Pillow |
| JPEG quality | 85 |
| Development HTTP port | 8080 |
| HTTP video path | `/frames/<device_id>.jpg` |
| Host development OS | Arch Linux |
| Target host | Raspberry Pi |

---

# 18. Current Data Model Summary

## Device

Represents a physical parking slave and its administrative identity.

**Key concepts:** identity, name, parking spot, authorization, enabled state, last-seen state, metadata, timestamps.

## Event

Represents an occurrence associated with a device.

**Key concepts:** event type, event time, reception time, structured payload, structured metadata.

## Video frame

Not currently stored as a PostgreSQL binary object. The database/event system records frame-related metadata while the latest converted JPEG is stored on the filesystem.

This separation prevents the relational database from becoming the primary high-frequency binary-frame store.

---

# 19. Design Decisions and Rationale

| Decision | Rationale |
|---|---|
| ESP32 as distributed slave | Provides low-cost Wi-Fi-connected sensing and camera acquisition. |
| Persistent WSS connection | Provides real-time bidirectional communication with one authenticated channel. |
| WSS for both metadata and video | Avoids maintaining separate embedded communication services. |
| Device ID + token | Separates device identity from transport security and permits per-device authorization. |
| SHA-256 token hashes in DB | Avoids storing device tokens as plaintext. |
| PostgreSQL | Supports multiple devices, event history, administration, and future expansion. |
| Generic event table | Allows new event types without redesigning the base event schema. |
| Filesystem for latest JPEG | Avoids storing high-frequency image binaries in the relational database. |
| Placeholder video | Permits complete end-to-end development without camera hardware. |
| RGB565 on ESP32 | Matches the current RHYX M21-45 camera output requirement. |
| Host-side JPEG conversion | Moves computationally heavier image processing away from the ESP32. |
| QVGA | Controls raw frame size and embedded resource usage. |
| HTTP latest-frame endpoint | Provides a simple development integration point for browsers/dashboard work. |
| Development laptop before Raspberry Pi | Allows rapid development and debugging while preserving a portable service architecture. |

---

# 20. Testing Performed During Development

The following design elements have been tested or explicitly validated during the development process:

### Host software

- Python syntax compilation of the updated host server.
- PostgreSQL integration structure.
- Device registration/authentication logic.
- RGB565 conversion logic.
- Synthetic RGB565 color conversion for red, green, and blue.
- JPEG generation from RGB565.
- HTTP serving of generated JPEG frames.
- JPEG validation for placeholder/direct-JPEG input.

### Protocol/design validation

- WSS traffic was observed in Wireshark as encrypted TLS traffic.
- TLS secret access was considered for development packet inspection.
- The WebSockets library version and API limitations were accounted for in the ESP32 implementation.

### Hardware/software integration targets

- ESP32-CAM camera configuration was adapted to RGB565.
- IR sensor interface was moved/maintained at GPIO13 for the current hardware configuration.
- Placeholder mode allows host testing without requiring the physical camera.

---

# 21. Known Constraints and Open Technical Issues

## 21.1 Raw RGB565 bandwidth

The current nominal 10 FPS raw stream is bandwidth-heavy. Multi-device scaling will require measurement and likely optimization.

## 21.2 RGB565 byte order

The current host assumes big-endian RGB565 based on the Espressif camera framebuffer behavior. Physical M21-45 testing should verify color correctness. If colors appear byte-swapped, the configured byte order must be adjusted.

## 21.3 Host conversion scalability

Pillow conversion is currently part of the WSS processing path. Multiple cameras may require asynchronous image processing to prevent video conversion from interfering with control/status responsiveness.

## 21.4 Development HTTP security

The port-8080 image service is not a complete authenticated application service. It should remain a development integration point until the dashboard/backend authorization layer is implemented.

## 21.5 Historical video

The current design keeps the latest JPEG only. Requirements for recordings, playback, or evidence retention have not yet been fully specified.

## 21.6 Payment data model

The current generic event architecture is expandable, but dedicated payment/session entities have not yet been finalized.

## 21.7 Customer-to-device/spot association

The final database model for customer identity, license plates, parking sessions, and spot assignment remains to be defined.

## 21.8 Gate hardware interface

The future servo/gate command path has been conceptually defined but its final embedded actuator interface and safety behavior have not yet been finalized.

---

# 22. Recommended Next Design Stage

The next engineering stage should turn the current communication/device prototype into an integrated parking-session platform.

The logical sequence is:

1. Validate the physical RHYX M21-45 RGB565 stream end-to-end.
2. Measure actual WSS throughput and sustained FPS.
3. Validate RGB565 byte order, image orientation, and frame timing.
4. Test simultaneous sensor/status traffic during video transmission.
5. Test one host with multiple authenticated ESP32 slaves.
6. Define the parking-session/customer/license-plate database model.
7. Implement the customer and manager backend authorization model.
8. Add the gate/servo command and event/audit path.
9. Integrate vehicle detection and license-plate processing.
10. Optimize the video pipeline for Raspberry Pi resource limits.
11. Replace the development HTTP endpoint with the authenticated application/reverse-proxy video path.
12. Define production deployment, service startup, certificate management, backup, logging, and retention policies.

---

# 23. Architectural Baseline

At the current design stage, the system can be summarized as:

> **Authenticated ESP32 parking slaves acquire occupancy state and raw camera frames, transmit both through a persistent WSS channel, and rely on a central Python/PostgreSQL host for authentication, event persistence, image conversion, and application integration.**

The principal architectural evolution has been from a simple ESP32 telemetry/video prototype toward a layered parking-management platform:

**Embedded sensing → secure real-time transport → persistent event/device model → host-side image processing → application/dashboard → payment and gate control → computer vision.**

The current design intentionally keeps the embedded node relatively simple and places extensible business logic and computational processing on the host. This provides a practical path from the present Arch Linux development environment to the intended Raspberry Pi deployment while retaining the same fundamental device protocol and database model.

---

## Appendix A — Configuration Baseline

The current development configuration includes the following non-secret architectural values:

- WSS server port: **8766**.
- WSS path: **`/parking`**.
- HTTP development port: **8080**.
- IR sensor GPIO: **13**.
- Camera resolution: **320 × 240**.
- Camera pixel format: **RGB565**.
- Raw RGB565 frame size: **153,600 bytes**.
- Nominal video interval: **100 ms**.
- JPEG quality: **85**.
- Host WSS maximum message size: **5 MiB**.
- WSS reconnect interval: **5 seconds**.
- WSS heartbeat: **15-second interval, 3-second timeout, two missed heartbeats**.

Authentication tokens, Wi-Fi passwords, database passwords, private keys, and other credentials are deliberately excluded from this document.

## Appendix B — Terminology

**Slave:** ESP32-based parking-space node.  
**Host:** Central Python service currently developed on Arch Linux and intended for Raspberry Pi deployment.  
**WSS:** WebSocket over TLS.  
**RGB565:** 16-bit RGB pixel representation with 5 red, 6 green, and 5 blue bits.  
**JPEG:** Compressed image representation used by the host for browser/application delivery.  
**Placeholder:** Development video source used when the physical camera is unavailable or disabled.  
**Device ID:** Unique logical identity assigned to an ESP32 parking node.  
**Event:** Persistent structured occurrence associated with a device.  
**Last seen:** Database timestamp representing the latest successful device communication.  
