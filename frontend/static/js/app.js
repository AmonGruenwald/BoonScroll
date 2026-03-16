/* ===== BoonScroll Frontend ===== */
'use strict';

const API = '';

const AVATAR_COLORS = [
  '#1F6B47', '#2E6B8A', '#7A3B6E', '#B05A1A',
  '#5A3B8A', '#1A6B6B', '#8A5A1A', '#3B5A8A',
  '#6B1A3B', '#4A6B3B',
];

const USER_STORAGE_KEY = 'boonscroll_user_id';

// ---- State ----
let state = {
  users: [],
  currentUser: null,
  feedDates: [],
  currentDateIdx: 0,
  feed: [],
};

// ---- DOM ----
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
  const today = new Date(); today.setHours(0, 0, 0, 0);
  const diff = Math.round((d - today) / 86400000);
  if (diff === 0) return 'Today';
  if (diff === -1) return 'Yesterday';
  return d.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' });
}

function showToast(msg, duration = 2500) {
  const t = $('share-toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  setTimeout(() => t.classList.add('hidden'), duration);
}

function stripHtml(html) {
  const d = document.createElement('div');
  d.innerHTML = html;
  return d.textContent || d.innerText || '';
}

// ---- Routing ----
function checkSharedRoute() {
  const m = window.location.pathname.match(/^\/shared\/([a-f0-9]+)$/);
  if (m) { renderSharedItem(m[1]); return true; }
  return false;
}

async function renderSharedItem(token) {
  showScreen('shared');
  const wrap = $('shared-card-wrap');
  try {
    const item = await api(`/api/shared/${token}`);
    wrap.innerHTML = `
      <p style="font-size:12px;color:var(--text-meta);margin-bottom:10px;padding:0 4px;">
        Shared by <strong>${item.shared_by}</strong> · ${formatDate(item.feed_date)}
      </p>
      ${buildCard(item, false, true)}`;
  } catch {
    wrap.innerHTML = `<p style="text-align:center;color:var(--text-meta);padding:40px 0">Item not found.</p>`;
  }
}

// ---- User select ----
async function loadUsers() {
  state.users = await api('/api/users');
  renderUserList();
}

function renderUserList() {
  const list = $('user-list');
  if (!state.users.length) {
    list.innerHTML = '<p style="color:var(--text-meta);font-size:14px">No profiles yet. Add one to get started.</p>';
    return;
  }
  list.innerHTML = state.users.map(u => `
    <div class="user-card" data-id="${u.id}">
      <button class="user-delete-btn" data-id="${u.id}" title="Delete ${u.display_name}">✕</button>
      <div class="avatar lg" style="background:${u.avatar_color}">${avatarInitials(u.display_name)}</div>
      <span class="name">${u.display_name}</span>
    </div>`).join('');

  list.querySelectorAll('.user-card').forEach(card => {
    card.addEventListener('click', e => {
      if (e.target.closest('.user-delete-btn')) return; // let delete button handle it
      selectUser(Number(card.dataset.id));
    });
  });

  list.querySelectorAll('.user-delete-btn').forEach(btn => {
    btn.addEventListener('click', async e => {
      e.stopPropagation();
      const id = Number(btn.dataset.id);
      const user = state.users.find(u => u.id === id);
      if (!confirm(`Delete ${user?.display_name ?? 'this user'}? This also deletes all their feed data.`)) return;
      try {
        await api(`/api/users/${id}`, { method: 'DELETE' });
        state.users = state.users.filter(u => u.id !== id);
        renderUserList();
      } catch (err) {
        showToast('Error deleting user: ' + err.message);
      }
    });
  });
}

async function selectUser(userId) {
  const user = await api(`/api/users/${userId}`);
  state.currentUser = user;
  localStorage.setItem(USER_STORAGE_KEY, String(userId));
  await loadFeedDates();
  showScreen('feed');
  renderInterests();
}

function logout() {
  localStorage.removeItem(USER_STORAGE_KEY);
  state.currentUser = null;
  state.feed = [];
  $('panel-interests').classList.add('hidden');
  showScreen('users');
  loadUsers();
}

// ---- Feed dates ----
async function loadFeedDates() {
  const data = await api(`/api/users/${state.currentUser.id}/feed/dates`);
  state.feedDates = data.dates;
  const today = new Date().toISOString().split('T')[0];
  if (!state.feedDates.includes(today)) state.feedDates.unshift(today);
  state.currentDateIdx = 0;
  updateDateNav();
  await loadFeed();
}

function updateDateNav() {
  const d = state.feedDates[state.currentDateIdx];
  $('current-date-label').textContent = formatDate(d);
  const atOldest = state.currentDateIdx >= state.feedDates.length - 1;
  const atNewest = state.currentDateIdx <= 0;
  ['prev-date', 'bnav-prev'].forEach(id => { const el = $(id); if (el) el.disabled = atOldest; });
  ['next-date', 'bnav-next'].forEach(id => { const el = $(id); if (el) el.disabled = atNewest; });
}

async function goToPrevDate() {
  if (state.currentDateIdx < state.feedDates.length - 1) {
    state.currentDateIdx++;
    updateDateNav();
    await loadFeed();
  }
}
async function goToNextDate() {
  if (state.currentDateIdx > 0) {
    state.currentDateIdx--;
    updateDateNav();
    await loadFeed();
  }
}

$('prev-date').addEventListener('click', goToPrevDate);
$('next-date').addEventListener('click', goToNextDate);

// ---- Feed ----
async function loadFeed() {
  const list = $('feed-list');
  const empty = $('feed-empty');
  const loading = $('feed-loading');
  list.innerHTML = '';
  empty.classList.add('hidden');
  loading.classList.remove('hidden');
  try {
    const d = state.feedDates[state.currentDateIdx];
    const data = await api(`/api/users/${state.currentUser.id}/feed?feed_date=${d}`);
    state.feed = data.items;
    loading.classList.add('hidden');
    if (!state.feed.length) { empty.classList.remove('hidden'); return; }
    list.innerHTML = state.feed.map(item => buildCard(item, true, false)).join('');
    attachCardListeners();
  } catch (e) {
    loading.classList.add('hidden');
    empty.classList.remove('hidden');
    console.error(e);
  }
}

// ---- Card builder ----
const TYPE_LABELS = { news: 'News', fact: 'Fact', video: 'Video', stock: 'Stock', image: 'Image' };

function tagPills(tags) {
  if (!tags || !tags.length) return '';
  return tags.map(t =>
    `<span class="tag-pill">${t}</span>`
  ).join('');
}

function postMeta(item) {
  return `
    <div class="post-meta">
      <span class="post-type-dot dot-${item.item_type}"></span>
      <span class="post-source">${TYPE_LABELS[item.item_type] || item.item_type}</span>
      ${item.source_name ? `<span class="post-source-sep">·</span><span>${item.source_name}</span>` : ''}
      <span class="meta-spacer"></span>
      ${tagPills(item.tags)}
    </div>`;
}

function postActions(item, showShare) {
  const shareBtn = showShare ? `
    <button class="action-btn share" data-token="${item.share_token}">
      <svg viewBox="0 0 20 20" fill="currentColor"><path d="M15 8a3 3 0 10-2.977-2.63l-4.94 2.47a3 3 0 100 4.319l4.94 2.47a3 3 0 10.895-1.789l-4.94-2.47a3.027 3.027 0 000-.74l4.94-2.47C13.456 7.68 14.19 8 15 8z"/></svg>
      Share
    </button>` : '';
  const label = item.content ? 'Read original' : 'Open';
  const extLink = item.source_url ? `
    <a class="action-btn" href="${item.source_url}" target="_blank" rel="noopener">
      <svg viewBox="0 0 20 20" fill="currentColor"><path d="M11 3a1 1 0 100 2h2.586l-6.293 6.293a1 1 0 101.414 1.414L15 6.414V9a1 1 0 102 0V4a1 1 0 00-1-1h-5z"/><path d="M5 5a2 2 0 00-2 2v8a2 2 0 002 2h8a2 2 0 002-2v-3a1 1 0 10-2 0v3H5V7h3a1 1 0 000-2H5z"/></svg>
      ${label}
    </a>` : '';
  return `<div class="post-actions">${shareBtn}${extLink}</div>`;
}

// expanded = whether to force-open (used on shared page)
function buildCard(item, showShare = true, expanded = false) {
  const expandedAttr = expanded ? 'data-expanded="true"' : 'data-expanded="false"';

  if (item.item_type === 'stock') {
    const dir = (item.stock_change ?? 0) >= 0 ? 'up' : 'down';
    const arrow = dir === 'up' ? '▲' : '▼';
    return `
    <div class="post-card" data-id="${item.id}" ${expandedAttr}>
      ${postMeta(item)}
      <div class="post-header collapsible-trigger">
        <div class="post-stock-body">
          <div class="stock-row">
            <span class="stock-ticker">${item.ticker}</span>
            <span class="stock-price">$${item.stock_price?.toFixed(2)}</span>
            <span class="stock-change ${dir}">${arrow} ${Math.abs(item.stock_change ?? 0).toFixed(2)} (${Math.abs(item.stock_change_pct ?? 0).toFixed(2)}%)</span>
          </div>
        </div>
      </div>
      ${postActions(item, showShare)}
    </div>`;
  }

  if (item.item_type === 'video') {
    const embed = item.media_url
      ? `<div class="post-video-wrap post-body-collapsed"><iframe src="${item.media_url}" allowfullscreen loading="lazy"></iframe></div>`
      : '';
    return `
    <div class="post-card" data-id="${item.id}" ${expandedAttr}>
      ${postMeta(item)}
      <div class="post-header collapsible-trigger">
        <div class="post-body">
          <div class="post-text">
            <div class="post-title">${item.title}</div>
            ${item.summary ? `<div class="post-summary post-body-collapsed">${stripHtml(item.summary)}</div>` : ''}
          </div>
          <span class="expand-chevron">›</span>
        </div>
      </div>
      ${embed}
      ${postActions(item, showShare)}
    </div>`;
  }

  // news / fact / default — collapsible body
  const thumb = item.thumbnail_url
    ? `<img class="post-thumb" src="${item.thumbnail_url}" alt="" loading="lazy" onerror="this.style.display='none'">`
    : '';

  const bodyContent = item.item_type === 'fact'
    ? `<div class="post-digest post-body-collapsed">${item.content || ''}</div>`
    : item.content
      ? `<div class="post-digest post-body-collapsed">${item.content}</div>`
      : item.summary
        ? `<div class="post-digest post-body-collapsed">${stripHtml(item.summary)}</div>`
        : '';

  return `
    <div class="post-card" data-id="${item.id}" ${expandedAttr}>
      ${postMeta(item)}
      <div class="post-header collapsible-trigger">
        <div class="post-body">
          <div class="post-text">
            <div class="post-title">${item.title}</div>
          </div>
          ${thumb}
          <span class="expand-chevron">›</span>
        </div>
      </div>
      ${bodyContent}
      ${postActions(item, showShare)}
    </div>`;
}

function attachCardListeners() {
  // Expand/collapse on header click
  document.querySelectorAll('.collapsible-trigger').forEach(header => {
    header.addEventListener('click', () => {
      const card = header.closest('.post-card');
      const expanded = card.dataset.expanded === 'true';
      card.dataset.expanded = expanded ? 'false' : 'true';
    });
  });

  // Share buttons
  document.querySelectorAll('.action-btn.share').forEach(btn => {
    btn.addEventListener('click', async e => {
      e.stopPropagation();
      const url = `${window.location.origin}/shared/${btn.dataset.token}`;
      let copied = false;
      if (navigator.clipboard) {
        try { await navigator.clipboard.writeText(url); copied = true; } catch {}
      }
      if (!copied) {
        // Fallback for HTTP (clipboard API requires HTTPS)
        const ta = document.createElement('textarea');
        ta.value = url; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.focus(); ta.select();
        try { copied = document.execCommand('copy'); } catch {}
        document.body.removeChild(ta);
      }
      showToast(copied ? 'Link copied!' : 'Copy failed — open share link manually');
    });
  });
}

// ---- Refresh ----
async function triggerGenerate() {
  ['trigger-generate', 'bnav-refresh'].forEach(id => { const el = $(id); if (el) el.disabled = true; });
  try {
    const d = state.feedDates[state.currentDateIdx];
    await api('/api/feed/generate', { method: 'POST', body: { feed_date: d } });
    showToast('Generating… refresh in about 30 seconds', 4000);
    setTimeout(async () => {
      await loadFeedDates();
      ['trigger-generate', 'bnav-refresh'].forEach(id => { const el = $(id); if (el) el.disabled = false; });
    }, 30000);
  } catch (e) {
    showToast('Error: ' + e.message);
    ['trigger-generate', 'bnav-refresh'].forEach(id => { const el = $(id); if (el) el.disabled = false; });
  }
}

$('trigger-generate').addEventListener('click', triggerGenerate);
$('logout-btn').addEventListener('click', logout);

// ---- Bottom nav ----
$('bnav-prev').addEventListener('click', goToPrevDate);
$('bnav-next').addEventListener('click', goToNextDate);
$('bnav-refresh').addEventListener('click', triggerGenerate);
$('bnav-interests').addEventListener('click', () => $('panel-interests').classList.remove('hidden'));
$('bnav-logout').addEventListener('click', logout);

// ---- Interests panel ----
$('open-interests').addEventListener('click', () => $('panel-interests').classList.remove('hidden'));
$('close-interests').addEventListener('click', () => $('panel-interests').classList.add('hidden'));

function renderInterests() {
  const list = $('interests-list');
  const interests = state.currentUser?.interests || [];
  if (!interests.length) {
    list.innerHTML = '<p style="color:var(--text-meta);font-size:13px">No interests yet. Add some below!</p>';
    return;
  }
  list.innerHTML = interests.map(i => `
    <div class="interest-chip" data-id="${i.id}">
      <span style="flex:1">${i.description}</span>
      <button class="interest-del" data-id="${i.id}" title="Remove">✕</button>
    </div>`).join('');

  list.querySelectorAll('.interest-del').forEach(btn => {
    btn.addEventListener('click', async () => {
      const id = Number(btn.dataset.id);
      const chip = list.querySelector(`.interest-chip[data-id="${id}"]`);
      if (chip) chip.style.opacity = '0.4';
      try {
        await api(`/api/users/${state.currentUser.id}/interests/${id}`, { method: 'DELETE' });
        state.currentUser.interests = state.currentUser.interests.filter(i => i.id !== id);
        renderInterests();
      } catch (err) {
        if (chip) chip.style.opacity = '1';
        showToast('Could not delete: ' + err.message);
      }
    });
  });
}

$('save-interest').addEventListener('click', async () => {
  const text = $('new-interest-text').value.trim();
  if (!text) return;
  try {
    const interest = await api(`/api/users/${state.currentUser.id}/interests`, {
      method: 'POST', body: { description: text },
    });
    state.currentUser.interests.push(interest);
    $('new-interest-text').value = '';
    renderInterests();
  } catch (err) {
    showToast('Could not save: ' + err.message);
  }
});

// ---- Add user modal ----
let selectedColor = AVATAR_COLORS[0];

function initColorPicker() {
  const picker = $('color-picker');
  picker.innerHTML = AVATAR_COLORS.map(c => `
    <div class="color-swatch ${c === selectedColor ? 'selected' : ''}" style="background:${c}" data-color="${c}"></div>`
  ).join('');
  picker.querySelectorAll('.color-swatch').forEach(sw =>
    sw.addEventListener('click', () => {
      selectedColor = sw.dataset.color;
      picker.querySelectorAll('.color-swatch').forEach(s => s.classList.remove('selected'));
      sw.classList.add('selected');
    })
  );
}

$('show-add-user').addEventListener('click', () => { initColorPicker(); $('modal-add-user').classList.remove('hidden'); });
$('cancel-add-user').addEventListener('click', () => $('modal-add-user').classList.add('hidden'));
$('modal-add-user').addEventListener('click', e => { if (e.target === $('modal-add-user')) $('modal-add-user').classList.add('hidden'); });

$('save-add-user').addEventListener('click', async () => {
  const displayName = $('new-display-name').value.trim();
  const username = $('new-username').value.trim().toLowerCase().replace(/[^a-z0-9_-]/g, '');
  if (!displayName || !username) { showToast('Fill in both fields'); return; }
  try {
    await api('/api/users', { method: 'POST', body: { name: username, display_name: displayName, avatar_color: selectedColor } });
    $('modal-add-user').classList.add('hidden');
    $('new-display-name').value = ''; $('new-username').value = '';
    await loadUsers();
  } catch (e) { showToast('Error: ' + e.message); }
});

// ---- Init ----
(async function init() {
  if (checkSharedRoute()) return;

  const storedId = localStorage.getItem(USER_STORAGE_KEY);
  if (storedId) {
    try { await selectUser(Number(storedId)); return; }
    catch { localStorage.removeItem(USER_STORAGE_KEY); }
  }
  showScreen('users');
  await loadUsers();
})();
