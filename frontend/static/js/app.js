/* ===== BoonScroll Frontend ===== */
'use strict';

const API = '';  // same origin

// ---- Avatar colors ----
const AVATAR_COLORS = [
  '#6366f1', '#8b5cf6', '#ec4899', '#ef4444',
  '#f97316', '#eab308', '#22c55e', '#14b8a6',
  '#0ea5e9', '#64748b',
];

// ---- State ----
let state = {
  users: [],
  currentUser: null,
  feedDates: [],
  currentDateIdx: 0,
  feed: [],
  interestsPanelOpen: false,
};

// ---- DOM refs ----
const $ = id => document.getElementById(id);
const screens = {
  users: $('screen-users'),
  feed: $('screen-feed'),
  shared: $('screen-shared'),
};

// ---- Utilities ----
function showScreen(name) {
  Object.entries(screens).forEach(([k, el]) => {
    el.classList.toggle('active', k === name);
    el.classList.toggle('hidden', k !== name);
  });
}

async function api(path, opts = {}) {
  const res = await fetch(API + path, {
    headers: { 'Content-Type': 'application/json' },
    ...opts,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.detail || `HTTP ${res.status}`);
  }
  if (res.status === 204) return null;
  return res.json();
}

function avatarInitials(name) {
  return name.split(' ').map(w => w[0]).join('').toUpperCase().slice(0, 2);
}

function formatDate(isoDate) {
  const d = new Date(isoDate + 'T00:00:00');
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const diff = Math.round((d - today) / 86400000);
  if (diff === 0) return 'Today';
  if (diff === -1) return 'Yesterday';
  return d.toLocaleDateString(undefined, { weekday: 'long', month: 'short', day: 'numeric' });
}

function showToast(msg) {
  const t = $('share-toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  setTimeout(() => t.classList.add('hidden'), 2500);
}

function stripHtml(html) {
  const tmp = document.createElement('div');
  tmp.innerHTML = html;
  return tmp.textContent || tmp.innerText || '';
}

// ---- Routing (shared items) ----
function checkSharedRoute() {
  const path = window.location.pathname;
  const m = path.match(/^\/shared\/([a-f0-9]+)$/);
  if (m) {
    renderSharedItem(m[1]);
    return true;
  }
  return false;
}

async function renderSharedItem(token) {
  showScreen('shared');
  const wrap = $('shared-card-wrap');
  try {
    const item = await api(`/api/shared/${token}`);
    wrap.innerHTML = `
      <p style="font-size:13px;color:var(--text-muted);margin-bottom:12px;">
        Shared by <strong>${item.shared_by}</strong> from their BoonScroll feed on ${formatDate(item.feed_date)}
      </p>
      ${buildCard(item, false)}
    `;
  } catch (e) {
    wrap.innerHTML = `<p style="text-align:center;color:var(--text-secondary)">Item not found or link expired.</p>`;
  }
}

// ---- User select screen ----
async function loadUsers() {
  state.users = await api('/api/users');
  renderUserList();
}

function renderUserList() {
  const list = $('user-list');
  if (!state.users.length) {
    list.innerHTML = '<p style="color:var(--text-muted);font-size:14px;">No users yet. Add one to get started.</p>';
    return;
  }
  list.innerHTML = state.users.map(u => `
    <div class="user-card" data-id="${u.id}">
      <div class="avatar lg" style="background:${u.avatar_color}">${avatarInitials(u.display_name)}</div>
      <span class="name">${u.display_name}</span>
    </div>
  `).join('');

  list.querySelectorAll('.user-card').forEach(card => {
    card.addEventListener('click', () => selectUser(Number(card.dataset.id)));
  });
}

async function selectUser(userId) {
  const user = await api(`/api/users/${userId}`);
  state.currentUser = user;

  // Update header
  const avatar = $('header-avatar');
  avatar.style.background = user.avatar_color;
  avatar.textContent = avatarInitials(user.display_name);
  $('header-username').textContent = user.display_name;

  // Load feed dates
  await loadFeedDates();
  showScreen('feed');
  renderInterests();
}

// ---- Feed dates ----
async function loadFeedDates() {
  const data = await api(`/api/users/${state.currentUser.id}/feed/dates`);
  state.feedDates = data.dates;

  // If today has no feed, prepend today's date anyway
  const today = new Date().toISOString().split('T')[0];
  if (!state.feedDates.includes(today)) {
    state.feedDates.unshift(today);
  }
  state.currentDateIdx = 0;
  updateDateNav();
  await loadFeed();
}

function updateDateNav() {
  const label = $('current-date-label');
  const prevBtn = $('prev-date');
  const nextBtn = $('next-date');
  const d = state.feedDates[state.currentDateIdx];
  label.textContent = formatDate(d);
  prevBtn.disabled = state.currentDateIdx >= state.feedDates.length - 1;
  nextBtn.disabled = state.currentDateIdx <= 0;
}

$('prev-date').addEventListener('click', async () => {
  if (state.currentDateIdx < state.feedDates.length - 1) {
    state.currentDateIdx++;
    updateDateNav();
    await loadFeed();
  }
});

$('next-date').addEventListener('click', async () => {
  if (state.currentDateIdx > 0) {
    state.currentDateIdx--;
    updateDateNav();
    await loadFeed();
  }
});

// ---- Feed ----
async function loadFeed() {
  const grid = $('feed-grid');
  const empty = $('feed-empty');
  const loading = $('feed-loading');

  grid.innerHTML = '';
  empty.classList.add('hidden');
  loading.classList.remove('hidden');

  try {
    const d = state.feedDates[state.currentDateIdx];
    const data = await api(`/api/users/${state.currentUser.id}/feed?feed_date=${d}`);
    state.feed = data.items;
    loading.classList.add('hidden');

    if (!state.feed.length) {
      empty.classList.remove('hidden');
      return;
    }
    grid.innerHTML = state.feed.map(item => buildCard(item, true)).join('');
    attachCardListeners();
  } catch (e) {
    loading.classList.add('hidden');
    empty.classList.remove('hidden');
    console.error(e);
  }
}

// ---- Card builder ----
function buildCard(item, showShare = true) {
  const badge = `<span class="card-type-badge badge-${item.item_type}">${item.item_type}</span>`;
  const shareBtn = showShare ? `
    <button class="share-btn" data-token="${item.share_token}" title="Copy share link">
      ⬡ Share
    </button>` : '';

  const footer = `
    <div class="card-footer">
      <span class="card-source">${item.source_name || ''}</span>
      ${shareBtn}
    </div>`;

  if (item.item_type === 'stock') {
    const dir = item.stock_change >= 0 ? 'up' : 'down';
    const arrow = item.stock_change >= 0 ? '▲' : '▼';
    return `
    <div class="feed-card" data-id="${item.id}">
      ${badge}
      <div class="card-body" style="gap:6px;justify-content:center;min-height:140px;">
        <div class="stock-ticker">${item.ticker}</div>
        <div class="stock-price">$${item.stock_price?.toFixed(2)}</div>
        <div class="stock-change ${dir}">${arrow} ${Math.abs(item.stock_change)?.toFixed(2)} (${Math.abs(item.stock_change_pct)?.toFixed(2)}%)</div>
        ${item.source_url ? `<a href="${item.source_url}" target="_blank" rel="noopener" class="card-source" style="margin-top:4px">View on Yahoo Finance →</a>` : ''}
      </div>
      ${footer}
    </div>`;
  }

  if (item.item_type === 'fact') {
    return `
    <div class="feed-card" data-id="${item.id}">
      ${badge}
      <div class="card-thumb-placeholder">💡</div>
      <div class="card-body">
        <div class="card-title">${item.title}</div>
        <div class="fact-content">${item.content || ''}</div>
      </div>
      ${footer}
    </div>`;
  }

  if (item.item_type === 'video') {
    const embed = item.media_url
      ? `<div class="video-embed-wrap"><iframe src="${item.media_url}" allowfullscreen loading="lazy"></iframe></div>`
      : `<div class="card-thumb-placeholder">▶</div>`;
    return `
    <div class="feed-card" data-id="${item.id}">
      ${badge}
      ${embed}
      <div class="card-body">
        <div class="card-title">${item.source_url ? `<a href="${item.source_url}" target="_blank" rel="noopener">${item.title}</a>` : item.title}</div>
        ${item.summary ? `<div class="card-summary">${stripHtml(item.summary)}</div>` : ''}
      </div>
      ${footer}
    </div>`;
  }

  // news / image / default
  const thumb = item.thumbnail_url
    ? `<img class="card-thumb" src="${item.thumbnail_url}" alt="" loading="lazy" onerror="this.style.display='none'" />`
    : `<div class="card-thumb-placeholder">📰</div>`;

  return `
    <div class="feed-card" data-id="${item.id}">
      ${badge}
      ${thumb}
      <div class="card-body">
        <div class="card-title">
          ${item.source_url ? `<a href="${item.source_url}" target="_blank" rel="noopener">${item.title}</a>` : item.title}
        </div>
        ${item.summary ? `<div class="card-summary">${stripHtml(item.summary)}</div>` : ''}
      </div>
      ${footer}
    </div>`;
}

function attachCardListeners() {
  document.querySelectorAll('.share-btn').forEach(btn => {
    btn.addEventListener('click', async () => {
      const token = btn.dataset.token;
      const url = `${window.location.origin}/shared/${token}`;
      try {
        await navigator.clipboard.writeText(url);
        showToast('Link copied to clipboard!');
      } catch {
        prompt('Copy this link:', url);
      }
    });
  });
}

// ---- Trigger generate ----
$('trigger-generate').addEventListener('click', async () => {
  const btn = $('trigger-generate');
  btn.disabled = true;
  btn.textContent = 'Generating…';
  try {
    const d = state.feedDates[state.currentDateIdx];
    await api('/api/feed/generate', { method: 'POST', body: { feed_date: d } });
    showToast('Feed generation started! Refresh in a moment.');
    setTimeout(async () => {
      await loadFeedDates();
      btn.disabled = false;
      btn.textContent = 'Refresh feed';
    }, 5000);
  } catch (e) {
    showToast('Error: ' + e.message);
    btn.disabled = false;
    btn.textContent = 'Refresh feed';
  }
});

// ---- Back to users ----
$('back-to-users').addEventListener('click', () => {
  state.currentUser = null;
  state.feed = [];
  showScreen('users');
  loadUsers();
});

// ---- Interests panel ----
$('open-interests').addEventListener('click', openInterests);
$('close-interests').addEventListener('click', closeInterests);

function openInterests() {
  $('panel-interests').classList.remove('hidden');
  state.interestsPanelOpen = true;
}
function closeInterests() {
  $('panel-interests').classList.add('hidden');
  state.interestsPanelOpen = false;
}

function renderInterests() {
  const list = $('interests-list');
  const interests = state.currentUser?.interests || [];
  if (!interests.length) {
    list.innerHTML = '<p style="color:var(--text-muted);font-size:14px;">No interests yet. Add some below!</p>';
    return;
  }
  list.innerHTML = interests.map(i => `
    <div class="interest-chip">
      <span style="flex:1">${i.description}</span>
      <button class="interest-del" data-id="${i.id}" title="Remove">✕</button>
    </div>
  `).join('');

  list.querySelectorAll('.interest-del').forEach(btn => {
    btn.addEventListener('click', async () => {
      const id = Number(btn.dataset.id);
      await api(`/api/users/${state.currentUser.id}/interests/${id}`, { method: 'DELETE' });
      state.currentUser.interests = state.currentUser.interests.filter(i => i.id !== id);
      renderInterests();
    });
  });
}

$('save-interest').addEventListener('click', async () => {
  const text = $('new-interest-text').value.trim();
  if (!text) return;
  const interest = await api(`/api/users/${state.currentUser.id}/interests`, {
    method: 'POST',
    body: { description: text },
  });
  state.currentUser.interests.push(interest);
  $('new-interest-text').value = '';
  renderInterests();
});

// ---- Add user modal ----
let selectedColor = AVATAR_COLORS[0];

function initColorPicker() {
  const picker = $('color-picker');
  picker.innerHTML = AVATAR_COLORS.map(c => `
    <div class="color-swatch ${c === selectedColor ? 'selected' : ''}" style="background:${c}" data-color="${c}"></div>
  `).join('');
  picker.querySelectorAll('.color-swatch').forEach(sw => {
    sw.addEventListener('click', () => {
      selectedColor = sw.dataset.color;
      picker.querySelectorAll('.color-swatch').forEach(s => s.classList.remove('selected'));
      sw.classList.add('selected');
    });
  });
}

$('show-add-user').addEventListener('click', () => {
  initColorPicker();
  $('modal-add-user').classList.remove('hidden');
});

$('cancel-add-user').addEventListener('click', () => {
  $('modal-add-user').classList.add('hidden');
});

$('save-add-user').addEventListener('click', async () => {
  const displayName = $('new-display-name').value.trim();
  const username = $('new-username').value.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
  if (!displayName || !username) {
    showToast('Please fill in both fields');
    return;
  }
  try {
    await api('/api/users', {
      method: 'POST',
      body: { name: username, display_name: displayName, avatar_color: selectedColor },
    });
    $('modal-add-user').classList.add('hidden');
    $('new-display-name').value = '';
    $('new-username').value = '';
    await loadUsers();
  } catch (e) {
    showToast('Error: ' + e.message);
  }
});

// Close modal on backdrop click
$('modal-add-user').addEventListener('click', e => {
  if (e.target === $('modal-add-user')) $('modal-add-user').classList.add('hidden');
});

// ---- Init ----
(async function init() {
  if (checkSharedRoute()) return;
  showScreen('users');
  await loadUsers();
})();
