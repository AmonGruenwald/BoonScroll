/* ===== BoonScroll Frontend ===== */
'use strict';

const API = '';

const AVATAR_COLORS = [
  '#FF4500', '#FF6314', '#46D160', '#0DD3BB',
  '#7193FF', '#FF585B', '#FFB000', '#46A2DA',
  '#FF66AC', '#878A8C',
];

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

function showToast(msg) {
  const t = $('share-toast');
  t.textContent = msg;
  t.classList.remove('hidden');
  setTimeout(() => t.classList.add('hidden'), 2500);
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
      ${buildCard(item, false)}`;
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
      <div class="avatar lg" style="background:${u.avatar_color}">${avatarInitials(u.display_name)}</div>
      <span class="name">${u.display_name}</span>
    </div>`).join('');
  list.querySelectorAll('.user-card').forEach(card =>
    card.addEventListener('click', () => selectUser(Number(card.dataset.id)))
  );
}

async function selectUser(userId) {
  const user = await api(`/api/users/${userId}`);
  state.currentUser = user;
  const av = $('header-avatar');
  av.style.background = user.avatar_color;
  av.textContent = avatarInitials(user.display_name);
  await loadFeedDates();
  showScreen('feed');
  renderInterests();
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
    list.innerHTML = state.feed.map(item => buildCard(item, true)).join('');
    attachCardListeners();
  } catch (e) {
    loading.classList.add('hidden');
    empty.classList.remove('hidden');
    console.error(e);
  }
}

// ---- Card builder ----
const TYPE_LABELS = { news: 'News', fact: 'Fun Fact', video: 'Video', stock: 'Stock', image: 'Image' };

function postMeta(item) {
  return `
    <div class="post-meta">
      <span class="post-type-dot dot-${item.item_type}"></span>
      <span class="post-source">${TYPE_LABELS[item.item_type] || item.item_type}</span>
      ${item.source_name ? `<span class="post-source-sep">·</span><span>${item.source_name}</span>` : ''}
    </div>`;
}

function postActions(item, showShare) {
  const shareBtn = showShare ? `
    <button class="action-btn share" data-token="${item.share_token}">
      <svg viewBox="0 0 20 20" fill="currentColor"><path d="M15 8a3 3 0 10-2.977-2.63l-4.94 2.47a3 3 0 100 4.319l4.94 2.47a3 3 0 10.895-1.789l-4.94-2.47a3.027 3.027 0 000-.74l4.94-2.47C13.456 7.68 14.19 8 15 8z"/></svg>
      Share
    </button>` : '';
  const extLink = item.source_url ? `
    <a class="action-btn" href="${item.source_url}" target="_blank" rel="noopener">
      <svg viewBox="0 0 20 20" fill="currentColor"><path d="M11 3a1 1 0 100 2h2.586l-6.293 6.293a1 1 0 101.414 1.414L15 6.414V9a1 1 0 102 0V4a1 1 0 00-1-1h-5z"/><path d="M5 5a2 2 0 00-2 2v8a2 2 0 002 2h8a2 2 0 002-2v-3a1 1 0 10-2 0v3H5V7h3a1 1 0 000-2H5z"/></svg>
      Open
    </a>` : '';
  return `<div class="post-actions">${shareBtn}${extLink}</div>`;
}

function buildCard(item, showShare = true) {
  if (item.item_type === 'stock') {
    const dir = (item.stock_change ?? 0) >= 0 ? 'up' : 'down';
    const arrow = dir === 'up' ? '▲' : '▼';
    return `
    <div class="post-card" data-id="${item.id}">
      ${postMeta(item)}
      <div class="post-stock-body">
        <div class="stock-row">
          <span class="stock-ticker">${item.ticker}</span>
          <span class="stock-price">$${item.stock_price?.toFixed(2)}</span>
          <span class="stock-change ${dir}">${arrow} ${Math.abs(item.stock_change ?? 0).toFixed(2)} (${Math.abs(item.stock_change_pct ?? 0).toFixed(2)}%)</span>
        </div>
      </div>
      ${postActions(item, showShare)}
    </div>`;
  }

  if (item.item_type === 'video') {
    const embed = item.media_url
      ? `<div class="post-video-wrap"><iframe src="${item.media_url}" allowfullscreen loading="lazy"></iframe></div>`
      : '';
    return `
    <div class="post-card" data-id="${item.id}">
      ${postMeta(item)}
      ${embed}
      <div class="post-body">
        <div class="post-text">
          <div class="post-title">${item.source_url ? `<a href="${item.source_url}" target="_blank" rel="noopener">${item.title}</a>` : item.title}</div>
          ${item.summary ? `<div class="post-summary">${stripHtml(item.summary)}</div>` : ''}
        </div>
      </div>
      ${postActions(item, showShare)}
    </div>`;
  }

  if (item.item_type === 'fact') {
    return `
    <div class="post-card" data-id="${item.id}">
      ${postMeta(item)}
      <div class="post-body">
        <div class="post-text">
          <div class="post-title">${item.title}</div>
        </div>
        <div class="post-thumb-placeholder">💡</div>
      </div>
      <div class="post-fact-content">${item.content || ''}</div>
      ${postActions(item, showShare)}
    </div>`;
  }

  // news / image / default — thumbnail on right
  const thumb = item.thumbnail_url
    ? `<img class="post-thumb" src="${item.thumbnail_url}" alt="" loading="lazy" onerror="this.style.display='none'">`
    : `<div class="post-thumb-placeholder">📰</div>`;

  return `
    <div class="post-card" data-id="${item.id}">
      ${postMeta(item)}
      <div class="post-body">
        <div class="post-text">
          <div class="post-title">${item.source_url ? `<a href="${item.source_url}" target="_blank" rel="noopener">${item.title}</a>` : item.title}</div>
          ${item.summary ? `<div class="post-summary">${stripHtml(item.summary)}</div>` : ''}
        </div>
        ${thumb}
      </div>
      ${postActions(item, showShare)}
    </div>`;
}

function attachCardListeners() {
  document.querySelectorAll('.share-btn, .action-btn.share').forEach(btn => {
    btn.addEventListener('click', async () => {
      const token = btn.dataset.token;
      const url = `${window.location.origin}/shared/${token}`;
      try { await navigator.clipboard.writeText(url); } catch { prompt('Copy link:', url); }
      showToast('Link copied!');
    });
  });
}

// ---- Refresh ----
async function triggerGenerate() {
  const btns = ['trigger-generate', 'bnav-refresh'];
  btns.forEach(id => { const el = $(id); if (el) { el.disabled = true; } });
  try {
    const d = state.feedDates[state.currentDateIdx];
    await api('/api/feed/generate', { method: 'POST', body: { feed_date: d } });
    showToast('Generating… check back in a moment');
    setTimeout(async () => {
      await loadFeedDates();
      btns.forEach(id => { const el = $(id); if (el) el.disabled = false; });
    }, 6000);
  } catch (e) {
    showToast('Error: ' + e.message);
    btns.forEach(id => { const el = $(id); if (el) el.disabled = false; });
  }
}

$('trigger-generate').addEventListener('click', triggerGenerate);

// ---- Back to users ----
$('back-to-users').addEventListener('click', () => {
  state.currentUser = null; state.feed = [];
  showScreen('users'); loadUsers();
});

// ---- Bottom nav wiring ----
$('bnav-prev').addEventListener('click', goToPrevDate);
$('bnav-next').addEventListener('click', goToNextDate);
$('bnav-refresh').addEventListener('click', triggerGenerate);
$('bnav-interests').addEventListener('click', openInterests);
$('bnav-user').addEventListener('click', () => {
  state.currentUser = null; state.feed = [];
  showScreen('users'); loadUsers();
});

// ---- Interests panel ----
$('open-interests').addEventListener('click', openInterests);
$('close-interests').addEventListener('click', closeInterests);

function openInterests() { $('panel-interests').classList.remove('hidden'); }
function closeInterests() { $('panel-interests').classList.add('hidden'); }

function renderInterests() {
  const list = $('interests-list');
  const interests = state.currentUser?.interests || [];
  if (!interests.length) {
    list.innerHTML = '<p style="color:var(--text-meta);font-size:13px">No interests yet. Add some below!</p>';
    return;
  }
  list.innerHTML = interests.map(i => `
    <div class="interest-chip">
      <span style="flex:1">${i.description}</span>
      <button class="interest-del" data-id="${i.id}">✕</button>
    </div>`).join('');
  list.querySelectorAll('.interest-del').forEach(btn =>
    btn.addEventListener('click', async () => {
      await api(`/api/users/${state.currentUser.id}/interests/${btn.dataset.id}`, { method: 'DELETE' });
      state.currentUser.interests = state.currentUser.interests.filter(i => i.id !== Number(btn.dataset.id));
      renderInterests();
    })
  );
}

$('save-interest').addEventListener('click', async () => {
  const text = $('new-interest-text').value.trim();
  if (!text) return;
  const interest = await api(`/api/users/${state.currentUser.id}/interests`, {
    method: 'POST', body: { description: text },
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
  showScreen('users');
  await loadUsers();
})();
