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
    health: 'reconnecting',
    downloadProgress: {}  // fileId -> {current, total, name}
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
                delete state.downloadProgress[data.file_id];
                loadFiles().then(renderFileList);
            } else if (data.current !== undefined && data.total !== undefined) {
                state.downloadProgress[data.file_id] = {
                    current: data.current,
                    total: data.total,
                    name: data.file_name || data.file_id?.slice(0, 8) || 'file'
                };
            }
            renderDownloadProgress();
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
    renderDownloadProgress();
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
                <button class="btn btn-ghost btn-sm" data-action="download" data-file-id="${f.file_id}" title="Download">⬇</button>
                <button class="btn btn-ghost btn-sm" data-action="open" data-file-id="${f.file_id}" title="Open">🔗</button>
                <button class="btn btn-ghost btn-sm" data-action="share" data-file-id="${f.file_id}" title="Share">📤</button>
                <button class="btn btn-ghost btn-sm" data-action="update" data-file-id="${f.file_id}" title="Update">✏️</button>
                <button class="btn btn-ghost btn-sm btn-danger" data-action="delete" data-file-id="${f.file_id}" title="Delete">🗑</button>
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
    container.querySelectorAll('[data-action="update"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            updateFile(btn.dataset.fileId);
        });
    });
    container.querySelectorAll('[data-action="delete"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.stopPropagation();
            deleteFile(btn.dataset.fileId);
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
    // Try inline preview for common types
    const file = state.files.find(f => f.file_id === fileId);
    if (file && (file.mime_type?.startsWith('text/') || file.mime_type?.startsWith('image/'))) {
        showFilePreview(fileId, file.file_name, file.mime_type);
    } else {
        window.open(`/api/files/${fileId}/open`, '_blank');
    }
}

function shareFile(fileId) {
    if (state.ws && state.ws.readyState === WebSocket.OPEN) {
        state.ws.send(JSON.stringify({ type: 'share', file_id: fileId }));
    }
}

function updateFile(fileId) {
    const input = document.getElementById('updateInput');
    input.dataset.fileId = fileId;
    input.click();
}

function deleteFile(fileId) {
    if (!confirm('Delete this file? This cannot be undone.')) return;
    fetch(`/api/files/${fileId}/delete`, { method: 'DELETE' })
        .then(r => r.json())
        .then(() => loadFiles().then(renderFileList))
        .catch(e => console.error('Delete failed:', e));
}

function showFilePreview(fileId, fileName, mimeType) {
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.id = 'previewModal';
    overlay.style.display = 'flex';
    
    let content = '';
    if (mimeType?.startsWith('image/')) {
        content = `<img src="/api/files/${fileId}/open" alt="${escapeHtml(fileName)}" style="max-width:100%;max-height:70vh;border-radius:8px;">`;
    } else if (mimeType?.startsWith('text/')) {
        content = `<iframe src="/api/files/${fileId}/open" style="width:100%;height:70vh;border:none;background:#fff;border-radius:8px;"></iframe>`;
    } else if (mimeType === 'application/pdf') {
        content = `<iframe src="/api/files/${fileId}/open" style="width:100%;height:70vh;border:none;border-radius:8px;"></iframe>`;
    } else {
        content = `<p>Preview not available for ${mimeType}. <a href="/api/files/${fileId}/download" target="_blank">Download instead</a></p>`;
    }
    
    overlay.innerHTML = `
        <div class="modal" style="max-width:90vw;">
            <h3>${escapeHtml(fileName)}</h3>
            ${content}
            <div class="modal-actions" style="margin-top:12px;">
                <button class="btn btn-primary" onclick="this.closest('.modal-overlay').remove()">Close</button>
            </div>
        </div>`;
    document.body.appendChild(overlay);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
}

function showShareModal(url) {
    document.getElementById('shareUrlInput').value = url;
    document.getElementById('shareModal').style.display = 'flex';
}

function renderDownloadProgress() {
    let container = document.getElementById('downloadProgressBar');
    if (!container) {
        container = document.createElement('div');
        container.id = 'downloadProgressBar';
        container.style.cssText = 'position:fixed;bottom:48px;right:16px;z-index:1000;display:flex;flex-direction:column;gap:4px;';
        document.body.appendChild(container);
    }
    const entries = Object.entries(state.downloadProgress);
    if (entries.length === 0) {
        container.innerHTML = '';
        return;
    }
    container.innerHTML = entries.map(([fid, p]) => {
        const pct = p.total > 0 ? Math.round((p.current / p.total) * 100) : 0;
        return `<div class="download-toast">
            <span>⬇ ${escapeHtml(p.name)} ${pct}%</span>
            <div class="mini-progress-bar"><div style="width:${pct}%"></div></div>
        </div>`;
    }).join('');
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

    document.getElementById('scanQrBtn').addEventListener('click', () => {
        // Show a text input modal as fallback for QR scanning
        const overlay = document.createElement('div');
        overlay.className = 'modal-overlay';
        overlay.style.display = 'flex';
        overlay.innerHTML = `
            <div class="modal">
                <h3>Paste Peer URL</h3>
                <p style="color:var(--text-secondary);margin-bottom:12px;">Scan the QR code with any QR scanner app, then paste the URL below:</p>
                <input type="text" id="scanUrlInput" placeholder="Paste URL here..." style="width:100%;padding:8px;border-radius:var(--radius);border:1px solid var(--border);background:var(--bg);color:var(--text);margin-bottom:12px;">
                <div class="modal-actions">
                    <button class="btn btn-primary" id="submitScanUrl">Connect</button>
                    <button class="btn btn-ghost" id="cancelScanUrl">Cancel</button>
                </div>
            </div>`;
        document.body.appendChild(overlay);
        
        overlay.querySelector('#submitScanUrl').addEventListener('click', () => {
            const url = overlay.querySelector('#scanUrlInput').value.trim();
            if (url) connectPeer(url);
            overlay.remove();
        });
        overlay.querySelector('#cancelScanUrl').addEventListener('click', () => overlay.remove());
        overlay.addEventListener('click', (e) => { if (e.target === overlay) overlay.remove(); });
        setTimeout(() => overlay.querySelector('#scanUrlInput').focus(), 100);
    });

    // Update file input
    document.getElementById('updateInput').addEventListener('change', (e) => {
        const file = e.target.files[0];
        const fileId = e.target.dataset.fileId;
        e.target.value = '';
        if (!file || !fileId) return;
        const formData = new FormData();
        formData.append('file', file);
        fetch(`/api/files/${fileId}/update`, { method: 'POST', body: formData })
            .then(r => r.json())
            .then(() => loadFiles().then(renderFileList))
            .catch(e => console.error('Update failed:', e));
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
        const size = 200;
        const gridSize = 21;
        const moduleSize = Math.floor(size / (gridSize + 8));
        const offset = 4 * moduleSize;

        canvas.width = size;
        canvas.height = size;
        const ctx = canvas.getContext('2d');

        // White background
        ctx.fillStyle = '#ffffff';
        ctx.fillRect(0, 0, size, size);

        // Generate a deterministic grid pattern from the URL string
        const url = data.url || '';
        let hash = 0;
        for (let i = 0; i < url.length; i++) {
            hash = ((hash << 5) - hash) + url.charCodeAt(i);
            hash |= 0;
        }

        // Draw grid
        ctx.fillStyle = '#000000';
        for (let row = 0; row < gridSize; row++) {
            for (let col = 0; col < gridSize; col++) {
                const seed = Math.abs(hash + row * 31 + col * 17) % 100;
                // Finder patterns in corners
                const isFinder = (row < 7 && col < 7) || (row < 7 && col >= gridSize - 7) || (row >= gridSize - 7 && col < 7);
                const isFinderBorder = (row === 0 || row === 6 || col === 0 || col === 6) && 
                    ((row < 7 && col < 7) || (row < 7 && col >= gridSize - 7) || (row >= gridSize - 7 && col < 7));
                const isFinderInner = (row >= 2 && row <= 4 && col >= 2 && col <= 4) &&
                    ((row < 7 && col < 7) || (row < 7 && col >= gridSize - 7) || (row >= gridSize - 7 && col < 7));
                
                if (isFinderBorder || isFinderInner || (!isFinder && seed < 45)) {
                    const x = offset + col * moduleSize;
                    const y = offset + row * moduleSize;
                    ctx.fillRect(x, y, moduleSize, moduleSize);
                }
            }
        }
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
