// app.js — Frontend Logic for Decentralised Web

const state = {
    ws: null,
    authorId: null,
    files: [],
    connectedPeers: [],
    searchQuery: '',
    activeTab: 'browse',
    expandedFileId: null,
    sidePanelVisible: true,
    storage: { own: 0, replicas: 0, available: 0, total: 0 },
    health: 'reconnecting'
};

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------

async function init() {
    try {
        const resp = await fetch('/api/status');
        if (resp.status === 401) {
            window.location.href = '/login';
            return;
        }
        const status = await resp.json();
        state.authorId = status.author_id;
        document.getElementById('authorName').textContent = status.author_id ? `@${status.author_id.slice(0, 8)}` : '';
        document.getElementById('nodeIdDisplay').textContent = status.node_id.slice(0, 12) + '...';
        state.health = status.health;
    } catch (e) {
        window.location.href = '/login';
        return;
    }

    connectWebSocket();
    await loadFiles();
    await loadPeers();
    await loadStorage();
    await loadNetworkName();
    renderAll();
}

function connectWebSocket() {
    const protocol = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${location.host}/ws`;
    state.ws = new WebSocket(wsUrl);

    state.ws.onmessage = handleWSMessage;
    state.ws.onclose = () => {
        updateHealth('reconnecting');
        setTimeout(connectWebSocket, 2000);
    };
    state.ws.onerror = () => {
        updateHealth('reconnecting');
    };
}

// ---------------------------------------------------------------------------
// WebSocket
// ---------------------------------------------------------------------------

function handleWSMessage(event) {
    const data = JSON.parse(event.data);
    switch (data.type) {
        case 'peer_update':
            loadPeers().then(renderPeerPanel);
            break;
        case 'file_update':
            loadFiles().then(renderFileList);
            break;
        case 'storage_update':
            if (data.storage) {
                state.storage = data.storage;
                renderBottomBar();
            }
            break;
        case 'health_update':
            updateHealth(data.status);
            break;
        case 'download_progress':
            if (data.status === 'complete') {
                loadFiles().then(renderFileList);
            }
            break;
        case 'share_response':
            showShareModal(data.url || '');
            break;
    }
}

function updateHealth(status) {
    state.health = status;
    const dot = document.querySelector('.status-dot');
    const text = document.getElementById('healthText');
    dot.className = 'status-dot ' + status;
    text.textContent = status === 'healthy' ? 'Connected' : status === 'reconnecting' ? 'Reconnecting...' : 'Offline';
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

async function loadFiles() {
    const url = state.activeTab === 'myfiles' ? '/api/files/my' : '/api/files';
    const resp = await fetch(url);
    state.files = await resp.json();
}

async function loadPeers() {
    const resp = await fetch('/api/peers');
    state.connectedPeers = (await resp.json()).filter(p => p.connected);
}

async function loadStorage() {
    const resp = await fetch('/api/storage/config');
    const data = await resp.json();
    state.storage = {
        own: data.used_own || 0,
        replicas: data.used_replicas || 0,
        available: data.available || 0,
        total: data.total_mb * 1024 * 1024
    };
}

async function loadNetworkName() {
    const resp = await fetch('/api/network-name');
    const data = await resp.json();
    document.getElementById('networkName').textContent = data.name;
}

// ---------------------------------------------------------------------------
// Render
// ---------------------------------------------------------------------------

function renderAll() {
    renderFileList();
    renderPeerPanel();
    renderBottomBar();
    updateSidePanel();
}

function renderFileList() {
    const container = document.getElementById('fileList');
    let files = state.files;
    if (state.searchQuery) {
        const q = state.searchQuery.toLowerCase();
        files = files.filter(f => f.file_name.toLowerCase().includes(q) || f.file_id.includes(q));
    }

    container.innerHTML = files.map(f => `
        <div class="file-row${state.expandedFileId === f.file_id ? ' expanded' : ''}" data-file-id="${f.file_id}">
            <span class="file-icon">📄</span>
            <div class="file-info">
                <div class="file-name">${escapeHtml(f.file_name)}</div>
                <div class="file-meta">${formatSize(f.file_size)} · ${f.replica_count} replicas</div>
            </div>
            <div class="file-actions">
                <button class="btn btn-ghost btn-sm" data-action="download" data-file-id="${f.file_id}">⬇</button>
                <button class="btn btn-ghost btn-sm" data-action="open" data-file-id="${f.file_id}">🔗</button>
                <button class="btn btn-ghost btn-sm" data-action="share" data-file-id="${f.file_id}">📤</button>
            </div>
        </div>
        <div class="file-details" data-file-id="${f.file_id}">
            <div>ID: ${f.file_id.slice(0,16)}...</div>
            <div>Author: ${f.author_id || 'unknown'}</div>
            <div>MIME: ${f.mime_type || 'unknown'}</div>
        </div>
    `).join('');

    // Event listeners
    container.querySelectorAll('.file-row').forEach(row => {
        row.addEventListener('click', (e) => {
            if (e.target.closest('[data-action]')) return;
            const fid = row.dataset.fileId;
            state.expandedFileId = state.expandedFileId === fid ? null : fid;
            renderFileList();
        });
    });

    container.querySelectorAll('[data-action="download"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            downloadFile(btn.dataset.fileId);
        });
    });
    container.querySelectorAll('[data-action="open"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            openFile(btn.dataset.fileId);
        });
    });
    container.querySelectorAll('[data-action="share"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            shareFile(btn.dataset.fileId);
        });
    });
}

function renderPeerPanel() {
    const container = document.getElementById('peerList');
    container.innerHTML = state.connectedPeers.map(p => `
        <div class="peer-item connected">
            <span class="status-dot healthy"></span>
            <span>${p.node_id.slice(0,8)}...</span>
        </div>
    `).join('');
}

function renderBottomBar() {
    const used = state.storage.own + state.storage.replicas;
    const total = state.storage.total;
    const pct = total > 0 ? Math.min(100, (used / total) * 100) : 0;
    document.getElementById('storageBar').style.width = pct + '%';
    document.getElementById('storageText').textContent = `${formatSize(used)} / ${formatSize(total)}`;
    document.getElementById('peerCount').textContent = `${state.connectedPeers.length} peers`;
}

function updateSidePanel() {
    const panel = document.getElementById('sidePanel');
    if (!state.sidePanelVisible || state.connectedPeers.length >= 2) {
        panel.classList.add('collapsed');
    } else {
        panel.classList.remove('collapsed');
    }
}

// ---------------------------------------------------------------------------
// Actions
// ---------------------------------------------------------------------------

function downloadFile(fileId) {
    window.open(`/api/files/${fileId}/download`, '_blank');
}

function openFile(fileId) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: 'open', file_id: fileId }));
    }
    window.open(`/api/files/${fileId}/open`, '_blank');
}

function shareFile(fileId) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: 'share', file_id: fileId }));
    }
}

function showShareModal(url) {
    document.getElementById('shareUrlInput').value = url;
    document.getElementById('shareModal').style.display = 'flex';
}

function uploadFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    fetch('/api/files/upload', { method: 'POST', body: formData })
        .then(r => r.json())
        .then(() => loadFiles().then(renderFileList));
}

function connectPeer(url) {
    fetch('/api/peers/connect', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url })
    }).then(() => loadPeers().then(renderPeerPanel));
}

// ---------------------------------------------------------------------------
// Event Listeners
// ---------------------------------------------------------------------------

document.addEventListener('DOMContentLoaded', () => {
    init();

    // Tabs
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            tab.classList.add('active');
            state.activeTab = tab.dataset.tab;
            if (state.activeTab === 'settings') {
                document.getElementById('fileList').style.display = 'none';
                document.getElementById('settingsPanel').style.display = 'block';
                document.querySelector('.toolbar').style.display = 'none';
                loadSettingsTab();
            } else {
                document.getElementById('fileList').style.display = '';
                document.getElementById('settingsPanel').style.display = 'none';
                document.querySelector('.toolbar').style.display = '';
                loadFiles().then(renderFileList);
            }
        });
    });

    // Search
    document.getElementById('searchBox').addEventListener('keyup', (e) => {
        state.searchQuery = e.target.value;
        renderFileList();
    });

    // Upload
    document.getElementById('uploadBtn').addEventListener('click', () => {
        document.getElementById('uploadInput').click();
    });
    document.getElementById('uploadInput').addEventListener('change', (e) => {
        Array.from(e.target.files).forEach(uploadFile);
        e.target.value = '';
    });

    // Side panel
    document.getElementById('shareNetworkBtn').addEventListener('click', () => {
        state.sidePanelVisible = true;
        updateSidePanel();
        loadQR();
    });
    document.getElementById('hideSidePanel').addEventListener('click', () => {
        state.sidePanelVisible = false;
        updateSidePanel();
    });

    // Side panel actions
    document.getElementById('copyLinkBtn').addEventListener('click', async () => {
        try {
            const resp = await fetch('/api/qr');
            const data = await resp.json();
            await navigator.clipboard.writeText(data.url);
        } catch (e) {
            // fallback
        }
    });

    document.getElementById('pastePeerBtn').addEventListener('click', async () => {
        try {
            const text = await navigator.clipboard.readText();
            if (text) connectPeer(text);
        } catch (e) {}
    });

    // Share modal
    document.getElementById('closeShareModal').addEventListener('click', () => {
        document.getElementById('shareModal').style.display = 'none';
    });
    document.getElementById('copyShareUrlBtn').addEventListener('click', () => {
        const input = document.getElementById('shareUrlInput');
        input.select();
        document.execCommand('copy');
    });

    // Logout
    document.getElementById('logoutBtn').addEventListener('click', async () => {
        await fetch('/api/logout', { method: 'POST' });
        window.location.href = '/login';
    });

    // Settings
    document.getElementById('saveQuotaBtn').addEventListener('click', async () => {
        const mb = parseInt(document.getElementById('quotaInput').value);
        const resp = await fetch('/api/storage/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ total_mb: mb })
        });
        const data = await resp.json();
        if (resp.ok) {
            loadStorage().then(renderBottomBar);
        } else {
            document.getElementById('quotaWarning').textContent = data.error || 'Failed to set quota';
        }
    });

    document.getElementById('saveNetworkNameBtn').addEventListener('click', async () => {
        const name = document.getElementById('networkNameInput').value.trim();
        await fetch('/api/network-name', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name })
        });
        document.getElementById('networkName').textContent = name;
    });

    // Tab close notification
    window.addEventListener('beforeunload', () => {
        if (state.ws && state.ws.readyState === WebSocket.OPEN) {
            state.ws.send(JSON.stringify({ type: 'tab_closed', file_id: '' }));
        }
    });
});

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatSize(bytes) {
    if (!bytes || bytes < 1024) return (bytes || 0) + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

async function loadQR() {
    try {
        const resp = await fetch('/api/qr');
        const data = await resp.json();
        const canvas = document.getElementById('qrCanvas');
        // Simple QR: just show text if no QR library
        const ctx = canvas.getContext('2d');
        canvas.width = 200;
        canvas.height = 200;
        ctx.fillStyle = '#000';
        ctx.fillRect(0, 0, 200, 200);
        ctx.fillStyle = '#fff';
        ctx.font = '10px monospace';
        ctx.fillText(data.url.slice(0, 50), 5, 100);
    } catch (e) {}
}

async function loadSettingsTab() {
    const resp = await fetch('/api/storage/config');
    const data = await resp.json();
    document.getElementById('quotaInput').value = data.total_mb;

    const nameResp = await fetch('/api/network-name');
    const nameData = await nameResp.json();
    document.getElementById('networkNameInput').value = nameData.name;
}
