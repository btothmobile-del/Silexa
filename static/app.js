// Auth
function getToken() {
  return localStorage.getItem("silexa_token");
}

function logout() {
  localStorage.removeItem("silexa_token");
  localStorage.removeItem("silexa_email");
  window.location.href = "login.html";
}

function requireAuth() {
  if (!getToken()) window.location.href = "index.html";
}

async function apiFetch(url, options = {}) {
  const token = getToken();
  const headers = { "Content-Type": "application/json", ...(options.headers || {}) };
  if (token) headers["Authorization"] = `Bearer ${token}`;
  const res = await fetch(url, { ...options, headers });
  if (res.status === 401) {
    logout();
    throw new Error("Lejárt munkamenet.");
  }
  return res;
}

// Beállítások — szerver oldalról töltjük, localStorage csak cache
const DEFAULT_SETTINGS = {
  language: "magyar",
  interests: ["világ", "közélet"],
  countries: ["usa", "uk", "germany", "france", "brazil", "italy", "hungary"],
  is_premium: false,
  premium_feeds: {},
  voice: "nova",
  briefing_time: "06:00",
  timezone: "Europe/Budapest",
};

let _settingsCache = null;

async function loadSettings() {
  try {
    const res = await apiFetch("/api/user/settings");
    if (res.ok) {
      _settingsCache = await res.json();
      return _settingsCache;
    }
  } catch {}
  return { ...DEFAULT_SETTINGS };
}

function getSettings() {
  if (_settingsCache) return _settingsCache;
  // Fallback localStorage (offline vagy első töltés előtt)
  try {
    const raw = localStorage.getItem("newsreader_settings");
    return raw ? { ...DEFAULT_SETTINGS, ...JSON.parse(raw) } : { ...DEFAULT_SETTINGS };
  } catch {
    return { ...DEFAULT_SETTINGS };
  }
}

async function saveSettings(settings) {
  _settingsCache = settings;
  localStorage.setItem("newsreader_settings", JSON.stringify(settings));
  try {
    await apiFetch("/api/user/settings", {
      method: "POST",
      body: JSON.stringify(settings),
    });
  } catch {}
}

let _meCache = null;

async function loadMe() {
  if (_meCache) return _meCache;
  try {
    const res = await apiFetch("/api/auth/me");
    if (res.ok) { _meCache = await res.json(); return _meCache; }
  } catch {}
  return null;
}

async function renderAdminNav() {
  const me = await loadMe();
  if (me && me.status === "admin") {
    const nav = document.querySelector("nav");
    if (!nav || nav.querySelector(".admin-nav-link")) return;
    const link = document.createElement("a");
    link.href = "admin.html";
    link.className = "admin-nav-link" + (location.pathname.endsWith("admin.html") ? " active" : "");
    link.innerHTML = '<span class="icon">🛡️</span>Admin';
    nav.appendChild(link);
  }
}

// Whisper-alapú hangvezérlés
let mediaRecorder = null;
let audioChunks = [];
let isRecording = false;

function initVoiceControl(commandHandler) {
  const micBtn = document.getElementById("micBtn");
  if (!micBtn) return;

  async function startRecording() {
    if (isRecording) return;
    try {
      const stream = await navigator.mediaDevices.getUserMedia({ audio: true });
      audioChunks = [];
      mediaRecorder = new MediaRecorder(stream);
      mediaRecorder.ondataavailable = e => { if (e.data.size > 0) audioChunks.push(e.data); };
      mediaRecorder.onstop = async () => {
        stream.getTracks().forEach(t => t.stop());
        const blob = new Blob(audioChunks, { type: "audio/webm" });
        await sendToWhisper(blob, commandHandler);
      };
      mediaRecorder.start();
      isRecording = true;
      micBtn.classList.add("listening");
      document.getElementById("micLabel").textContent = "Figyelek...";
    } catch {
      showVoiceFeedback("Mikrofon hozzáférés megtagadva.");
    }
  }

  function stopRecording() {
    if (!isRecording || !mediaRecorder) return;
    mediaRecorder.stop();
    isRecording = false;
    micBtn.classList.remove("listening");
    document.getElementById("micLabel").textContent = "Feldolgozás...";
  }

  micBtn.addEventListener("mousedown", e => { e.preventDefault(); startRecording(); });
  micBtn.addEventListener("mouseup", e => { e.preventDefault(); stopRecording(); });
  micBtn.addEventListener("mouseleave", () => { if (isRecording) stopRecording(); });
  micBtn.addEventListener("touchstart", e => { e.preventDefault(); startRecording(); }, { passive: false });
  micBtn.addEventListener("touchend", e => { e.preventDefault(); stopRecording(); }, { passive: false });
}

async function sendToWhisper(blob, commandHandler) {
  try {
    const formData = new FormData();
    formData.append("audio", blob, "recording.webm");
    const token = getToken();
    const headers = token ? { "Authorization": `Bearer ${token}` } : {};
    const res = await fetch("/api/transcribe", { method: "POST", body: formData, headers });
    if (!res.ok) throw new Error("Szerver hiba");
    const data = await res.json();
    if (data.text && data.text.trim()) {
      showVoiceFeedback(`"${data.text}"`, 3000);
      commandHandler(data.text.toLowerCase().trim());
    } else {
      showVoiceFeedback("Nem hallottam semmit.");
    }
  } catch (e) {
    showVoiceFeedback("Hiba: " + e.message);
  } finally {
    const lbl = document.getElementById("micLabel");
    if (lbl) lbl.textContent = "Nyomva tartva";
  }
}

function showVoiceFeedback(msg, duration = 2500) {
  const el = document.getElementById("voiceFeedback");
  if (!el) return;
  el.textContent = msg;
  el.classList.add("show");
  clearTimeout(el._timer);
  el._timer = setTimeout(() => el.classList.remove("show"), duration);
}

function setActiveNav() {
  const path = location.pathname.split("/").pop() || "index.html";
  document.querySelectorAll("nav a").forEach(a => {
    const href = a.getAttribute("href");
    a.classList.toggle("active", href === path || (path === "" && href === "index.html"));
  });
}

document.addEventListener("DOMContentLoaded", () => { setActiveNav(); renderAdminNav(); });

// Push notifications
async function initPush() {
  if (!("serviceWorker" in navigator) || !("PushManager" in window)) return;
  try {
    const reg = await navigator.serviceWorker.register("/sw.js");
    const keyRes = await fetch("/api/push/vapid-public-key");
    const { public_key } = await keyRes.json();
    if (!public_key) return;

    const existing = await reg.pushManager.getSubscription();
    if (existing) return; // már feliratkozott

    const permission = await Notification.requestPermission();
    if (permission !== "granted") return;

    const sub = await reg.pushManager.subscribe({
      userVisibleOnly: true,
      applicationServerKey: urlBase64ToUint8Array(public_key),
    });
    const json = sub.toJSON();
    await apiFetch("/api/push/subscribe", {
      method: "POST",
      body: JSON.stringify({ endpoint: json.endpoint, p256dh: json.keys.p256dh, auth: json.keys.auth }),
    });
  } catch {}
}

function urlBase64ToUint8Array(base64String) {
  const padding = "=".repeat((4 - base64String.length % 4) % 4);
  const base64 = (base64String + padding).replace(/-/g, "+").replace(/_/g, "/");
  const raw = atob(base64);
  return Uint8Array.from([...raw].map(c => c.charCodeAt(0)));
}
