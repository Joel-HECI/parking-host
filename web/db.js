const deviceRows = document.getElementById('deviceRows');
const eventRows = document.getElementById('eventRows');
const deviceFilter = document.getElementById('deviceFilter');
const eventTypeFilter = document.getElementById('eventTypeFilter');
const limitInput = document.getElementById('limitInput');
const refreshEventsButton = document.getElementById('refreshEvents');
const deviceForm = document.getElementById('deviceForm');
const statusBar = document.getElementById('statusBar');

let devices = [];

const UTC_MINUS_5_LABEL = 'UTC-5';

function isPlainObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function setStatus(message) {
    statusBar.textContent = message;
}

function formatValue(value) {
    if (value === null || value === undefined || value === '') return '—';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}

function parseTimestamp(value) {
    if (!value) return null;

    if (value instanceof Date) return value;

    if (typeof value === 'string') {
        const trimmed = value.trim();

        if (trimmed.endsWith(' UTC')) {
            const base = trimmed.slice(0, -4).trim().replace(' ', 'T');
            return new Date(`${base}Z`);
        }

        if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(trimmed)) {
            return new Date(`${trimmed.replace(' ', 'T')}Z`);
        }
    }

    return new Date(value);
}

function formatTimestamp(value) {
    if (!value) return '—';

    const date = parseTimestamp(value);
    if (Number.isNaN(date.getTime())) return '—';

    const shifted = new Date(date.getTime() - (5 * 60 * 60 * 1000));
    const year = shifted.getUTCFullYear();
    const month = String(shifted.getUTCMonth() + 1).padStart(2, '0');
    const day = String(shifted.getUTCDate()).padStart(2, '0');
    const hours = String(shifted.getUTCHours()).padStart(2, '0');
    const minutes = String(shifted.getUTCMinutes()).padStart(2, '0');
    const seconds = String(shifted.getUTCSeconds()).padStart(2, '0');
    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds} ${UTC_MINUS_5_LABEL}`;
}

function clearTable(tbody) {
    tbody.innerHTML = '';
}

function buildCell(text) {
    const td = document.createElement('td');
    td.textContent = text;
    return td;
}

function buildSummaryCell(value) {
    const td = document.createElement('td');
    td.className = 'summary-cell';
    td.textContent = value;
    return td;
}

function summarizeScalar(value) {
    if (value === null || value === undefined || value === '') return '—';
    if (typeof value === 'boolean') return value ? 'yes' : 'no';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
}

function summarizeSensor(sensor) {
    if (!isPlainObject(sensor)) return '—';

    const parts = [];
    if (sensor.type) parts.push(String(sensor.type).toUpperCase());
    if (sensor.gpio !== undefined) parts.push(`GPIO ${sensor.gpio}`);

    if (sensor.value !== undefined) {
        let state = summarizeScalar(sensor.value);
        if (sensor.value === 0 || String(sensor.value).trim?.() === '0') {
            state = 'active';
        } else if (sensor.value === 1 || String(sensor.value).trim?.() === '1') {
            state = 'occupied';
        }
        parts.push(state);
        parts.push(`raw ${summarizeScalar(sensor.value)}`);
    }

    return parts.length ? parts.join(' • ') : '—';
}

function summarizeKeyValueObject(value) {
    if (!isPlainObject(value)) return summarizeScalar(value);

    return Object.entries(value)
        .filter(([, nestedValue]) => nestedValue !== null && nestedValue !== undefined && nestedValue !== '')
        .map(([key, nestedValue]) => `${key}: ${summarizeScalar(nestedValue)}`)
        .join(' • ') || '—';
}

function summarizeMetadata(metadata) {
    return summarizeKeyValueObject(metadata);
}

function summarizeEventPayload(event) {
    const payload = event.payload || {};
    const eventType = event.event_type;

    if (!isPlainObject(payload)) {
        return summarizeScalar(payload);
    }

    if (eventType === 'sensor') {
        const sensor = payload.sensor || {};
        const parts = [`sensor ${summarizeSensor(sensor)}`];
        if (payload.timestamp) parts.push(`at ${formatTimestamp(payload.timestamp)}`);
        if (payload.uptime_ms !== undefined) parts.push(`uptime ${payload.uptime_ms} ms`);
        return parts.join(' • ');
    }

    if (eventType === 'status') {
        const wifi = payload.wifi || {};
        const sensor = payload.sensor || {};
        const video = payload.video || {};
        const parts = [];
        if (wifi.rssi !== undefined) parts.push(`WiFi RSSI ${wifi.rssi}`);
        if (wifi.ip) parts.push(`IP ${wifi.ip}`);
        if (sensor) parts.push(`sensor ${summarizeSensor(sensor)}`);
        if (video.source) parts.push(`video ${video.source}`);
        if (payload.timestamp) parts.push(`at ${formatTimestamp(payload.timestamp)}`);
        return parts.join(' • ') || '—';
    }

    if (eventType === 'video_status') {
        const parts = [];
        if (payload.source) parts.push(`source ${payload.source}`);
        if (payload.camera_enabled !== undefined) parts.push(`camera ${payload.camera_enabled ? 'enabled' : 'disabled'}`);
        if (payload.camera_initialized !== undefined) parts.push(`initialized ${payload.camera_initialized ? 'yes' : 'no'}`);
        if (payload.timestamp) parts.push(`at ${formatTimestamp(payload.timestamp)}`);
        return parts.join(' • ') || '—';
    }

    const entries = [];
    for (const [key, value] of Object.entries(payload)) {
        if (key === 'server_timestamp') continue;
        if (isPlainObject(value)) {
            entries.push(`${key}: ${summarizeKeyValueObject(value)}`);
        } else {
            entries.push(`${key}: ${summarizeScalar(value)}`);
        }
    }

    return entries.join(' • ') || '—';
}

function populateDeviceFilter() {
    const current = deviceFilter.value;
    deviceFilter.innerHTML = '<option value="">All devices</option>';
    for (const device of devices) {
        const option = document.createElement('option');
        option.value = device.device_id;
        option.textContent = device.device_id;
        deviceFilter.appendChild(option);
    }
    if ([...deviceFilter.options].some((option) => option.value === current)) {
        deviceFilter.value = current;
    }
}

async function loadDevices() {
    const res = await fetch('/api/db/devices', { cache: 'no-store' });
    if (!res.ok) throw new Error('Unable to load devices');
    const data = await res.json();
    devices = data.devices || [];

    clearTable(deviceRows);

    for (const device of devices) {
        const tr = document.createElement('tr');
        tr.appendChild(buildCell(device.device_id));
        tr.appendChild(buildCell(formatValue(device.name)));
        tr.appendChild(buildCell(formatValue(device.spot)));
        tr.appendChild(buildCell(device.enabled ? 'yes' : 'no'));
        tr.appendChild(buildCell(formatTimestamp(device.last_seen)));
        tr.appendChild(buildCell(formatTimestamp(device.created_at)));
        tr.appendChild(buildCell(formatTimestamp(device.updated_at)));

        const actionsTd = document.createElement('td');
        const actions = document.createElement('div');
        actions.className = 'actions';

        const deleteButton = document.createElement('button');
        deleteButton.type = 'button';
        deleteButton.className = 'danger';
        deleteButton.textContent = 'Remove';
        deleteButton.addEventListener('click', async () => {
            if (!confirm(`Remove device ${device.device_id}? This deletes its events too.`)) return;
            const response = await fetch(`/api/db/devices/${encodeURIComponent(device.device_id)}`, {
                method: 'DELETE',
            });
            if (!response.ok) {
                const text = await response.text();
                throw new Error(text || 'Failed to remove device');
            }
            await refreshAll();
        });

        actions.appendChild(deleteButton);
        actionsTd.appendChild(actions);
        tr.appendChild(actionsTd);
        deviceRows.appendChild(tr);
    }

    populateDeviceFilter();
    setStatus(`Loaded ${devices.length} devices.`);
}

async function loadEvents() {
    const params = new URLSearchParams();
    if (eventTypeFilter.value) params.set('event_type', eventTypeFilter.value);
    if (deviceFilter.value) params.set('device_id', deviceFilter.value);
    params.set('limit', String(limitInput.value || 100));
    params.set('offset', '0');

    const res = await fetch(`/api/db/events?${params.toString()}`, { cache: 'no-store' });
    if (!res.ok) throw new Error('Unable to load events');
    const data = await res.json();
    const events = data.events || [];

    clearTable(eventRows);

    for (const event of events) {
        const tr = document.createElement('tr');
        tr.appendChild(buildCell(event.id));
        tr.appendChild(buildCell(event.device_id));
        tr.appendChild(buildCell(event.event_type));
        tr.appendChild(buildCell(formatTimestamp(event.event_time)));
        tr.appendChild(buildCell(formatTimestamp(event.received_at)));
        tr.appendChild(buildSummaryCell(summarizeEventPayload(event)));
        eventRows.appendChild(tr);
    }

    setStatus(`Loaded ${events.length} events.`);
}

async function refreshAll() {
    setStatus('Loading…');
    await loadDevices();
    await loadEvents();
}

deviceForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const formData = new FormData(deviceForm);
    const payload = {
        device_id: formData.get('device_id'),
        token: formData.get('token'),
        name: formData.get('name') || null,
        spot: formData.get('spot') || null,
    };

    const res = await fetch('/api/db/devices', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
    });

    if (!res.ok) {
        const text = await res.text();
        setStatus(text || 'Failed to add device.');
        return;
    }

    deviceForm.reset();
    await refreshAll();
});

refreshEventsButton.addEventListener('click', async () => {
    try {
        await loadEvents();
    } catch (error) {
        setStatus(String(error));
    }
});

eventTypeFilter.addEventListener('change', async () => {
    try {
        await loadEvents();
    } catch (error) {
        setStatus(String(error));
    }
});

deviceFilter.addEventListener('change', async () => {
    try {
        await loadEvents();
    } catch (error) {
        setStatus(String(error));
    }
});

limitInput.addEventListener('change', async () => {
    try {
        await loadEvents();
    } catch (error) {
        setStatus(String(error));
    }
});

refreshAll().catch((error) => setStatus(String(error)));
setInterval(() => {
    refreshAll().catch((error) => setStatus(String(error)));
}, 10000);
