// Rivalry Week storage: a small document store on Supabase (RPC functions), polled for changes.
// Exposes window.RW_STORE = { db, uid } with the same doc/collection API the game page uses.
(function () {
  const URL_ = "https://lepyxeykovdlkchtokbp.supabase.co/rest/v1/rpc/";
  const KEY = "sb_publishable_aoAzcmUj0tPL4D-DAGbRoQ_Zkl_zqA4";
  const POLL_MS = 3000;

  // A per-browser player id. Kept in localStorage; a new device is a new player.
  let uid = null;
  try {
    uid = localStorage.getItem("rw-uid");
    if (!uid) {
      uid = "u" + Array.from(crypto.getRandomValues(new Uint8Array(12)), b => b.toString(16).padStart(2, "0")).join("");
      localStorage.setItem("rw-uid", uid);
    }
  } catch (e) { uid = null; }

  async function rpc(fn, args) {
    const res = await fetch(URL_ + fn, {
      method: "POST",
      headers: { apikey: KEY, Authorization: "Bearer " + KEY, "Content-Type": "application/json" },
      body: JSON.stringify(args),
    });
    const text = await res.text();
    if (!res.ok) {
      let msg = text; try { msg = JSON.parse(text).message || text; } catch (e) {}
      const err = new Error(msg);
      err.code = /game_full/.test(msg) ? "game_full" : /invalid_argument/.test(msg) ? "invalid_argument" : /duplicate key/.test(msg) ? "already_exists" : "network_" + res.status;
      throw err;
    }
    return text ? JSON.parse(text) : null;
  }

  // Subscriptions: each runs its fetch on an interval and on any local write.
  const subs = new Set();
  function refreshAll() { subs.forEach(s => s.run()); }
  setInterval(() => { if (!document.hidden) refreshAll(); }, POLL_MS);
  document.addEventListener("visibilitychange", () => { if (!document.hidden) refreshAll(); });

  function subscribe(fetcher, next, error) {
    let last = undefined, alive = true, inflight = false;
    const s = {
      run: async () => {
        if (!alive || inflight) return; inflight = true;
        try {
          const val = await fetcher();
          const key = JSON.stringify(val);
          if (alive && key !== last) { last = key; next(val); }
        } catch (e) { if (alive && last === undefined && error) error(e); }
        inflight = false;
      },
    };
    subs.add(s); s.run();
    return () => { alive = false; subs.delete(s); };
  }

  const clone = v => v == null ? v : JSON.parse(JSON.stringify(v));
  const meta = { fromCache: false, hasPendingWrites: false };
  const docSnap = (path, data) => ({ id: path.split("/").pop(), exists: data != null, data: () => clone(data), metadata: meta });
  const querySnap = rows => {
    const docs = (rows || []).map(r => docSnap(r.path, r.data));
    return { docs, size: docs.length, empty: !docs.length, metadata: meta };
  };

  function docRef(path) {
    const getData = () => rpc("rw_get", { p_path: path });
    return {
      id: path.split("/").pop(), path,
      get: async () => docSnap(path, await getData()),
      set: async d => { await rpc("rw_set", { p_path: path, p_data: d }); refreshAll(); },
      update: async d => { await rpc("rw_update", { p_path: path, p_patch: d }); refreshAll(); },
      onSnapshot: (next, error) => subscribe(getData, d => next(docSnap(path, d)), error),
      collection: c => colRef(path + "/" + c),
    };
  }

  function colRef(path, filter) {
    const fetchRows = () => filter
      ? rpc("rw_my_games", { p_uid: filter.value })
      : rpc("rw_list", { p_parent: path });
    return {
      path,
      doc: id => docRef(path + "/" + id),
      where: (field, op, value) => {
        if (path !== "games" || field !== "players" || op !== "array-contains") throw new Error("unsupported query");
        return colRef(path, { value });
      },
      get: async () => querySnap(await fetchRows()),
      onSnapshot: (next, error) => subscribe(fetchRows, rows => next(querySnap(rows)), error),
    };
  }

  async function join(gid, name) { await rpc("rw_join", { p_gid: gid, p_uid: uid, p_name: name }); refreshAll(); }
  window.RW_STORE = { db: { doc: docRef, collection: p => colRef(p) }, uid, join };
})();
