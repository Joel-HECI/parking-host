const select = document.getElementById('deviceSelect');
const video = document.getElementById('video');
const statusText = document.getElementById('statusText');
const irStatus = document.getElementById('irStatus');
const irLed = document.getElementById('irLed');
const irLabel = document.getElementById('irLabel');
let lastDevice = null;
let deviceSummaries = [];

function normalizeIrState(value) {
    if (value === null || value === undefined) return { state: 'unknown', label: 'IR sensor: unknown' };

    if (typeof value === 'boolean') {
        return value
            ? { state: 'off', label: 'IR sensor: occupied' }
            : { state: 'on', label: 'IR sensor: active' };
    }

    if (typeof value === 'number') {
        return value === 0
            ? { state: 'on', label: 'IR sensor: active' }
            : { state: 'off', label: 'IR sensor: occupied' };
    }

    const text = String(value).trim().toLowerCase();
    if (['1', 'true', 'on', 'high', 'active', 'detected', 'blocked'].includes(text)) {
        return { state: 'off', label: 'IR sensor: occupied' };
    }
    if (['0', 'false', 'off', 'low', 'clear', 'inactive', 'released'].includes(text)) {
        return { state: 'on', label: 'IR sensor: active' };
    }

    return { state: 'unknown', label: `IR sensor: ${String(value)}` };
}

function setIrIndicator(value) {
    const { state, label } = normalizeIrState(value);
    irStatus.classList.remove('is-on', 'is-off', 'is-unknown');
    irStatus.classList.add(`is-${state}`);
    irLed.setAttribute('data-state', state);
    irLabel.textContent = label;
}

function applyDeviceSensor(deviceId) {
    const device = deviceSummaries.find((item) => item.device_id === deviceId);
    if (!device) {
        setIrIndicator(null);
        return;
    }

    const sensorValue = device.ir_active !== undefined
        ? device.ir_active
        : device.ir !== undefined
            ? device.ir
            : device.sensor?.sensor?.value;

    setIrIndicator(sensorValue);
}

function updateVideoSource(deviceId) {
    if (!deviceId) return;
    const src = `/video/${encodeURIComponent(deviceId)}.mjpg?cacheBust=${Date.now()}`;
    video.src = src;
    lastDevice = deviceId;
    statusText.textContent = `Streaming ${deviceId}`;
    applyDeviceSensor(deviceId);
}

async function refreshDevices() {
    try {
        const res = await fetch('/api/devices', { cache: 'no-store' });
        if (!res.ok) throw new Error('devices unavailable');
        const devices = await res.json();
        const items = devices.devices || [];
        deviceSummaries = items;

        const current = select.value || lastDevice || items[0]?.device_id || '';
        select.innerHTML = '';

        if (!items.length) {
            const option = new Option('No devices connected', '');
            select.appendChild(option);
            statusText.textContent = 'No device is currently connected.';
            setIrIndicator(null);
            return;
        }

        for (const deviceId of items) {
            const option = new Option(deviceId.device_id, deviceId.device_id);
            if (deviceId.device_id === current) option.selected = true;
            select.appendChild(option);
        }

        if (!items.some((device) => device.device_id === current)) {
            updateVideoSource(items[0].device_id);
        } else {
            updateVideoSource(current);
        }
    } catch (err) {
        statusText.textContent = 'Unable to load device list.';
        setIrIndicator(null);
    }
}

select.addEventListener('change', (event) => {
    updateVideoSource(event.target.value);
});

video.addEventListener('error', () => {
    statusText.textContent = 'Stream unavailable. Waiting for camera…';
});

video.addEventListener('load', () => {
    statusText.textContent = `Streaming ${lastDevice || 'device'}`;
});

refreshDevices();
setInterval(refreshDevices, 5000);
