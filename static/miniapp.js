(function () {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  const initData = tg ? tg.initData || "" : "";
  const urlParams = new URLSearchParams(window.location.search);
  const previewMode = urlParams.get("preview") === "1";

  const authStatus = document.getElementById("authStatus");
  const profileLink = document.getElementById("profileLink");
  const copyLink = document.getElementById("copyLink");
  const summaryBubble = document.getElementById("summaryBubble");
  const summaryNotes = document.getElementById("summaryNotes");
  const profileName = document.getElementById("profileName");
  const avatarImg = document.getElementById("avatarImg");
  const avatarFallback = document.getElementById("avatarFallback");
  const chipTone = document.getElementById("chipTone");
  const chipSpeed = document.getElementById("chipSpeed");
  const chipFormat = document.getElementById("chipFormat");
  const copyToast = document.getElementById("copyToast");

  const answerStatus = document.getElementById("answerStatus");
  const answerTargetTitle = document.getElementById("answerTargetTitle");
  const sendAnswerBtn = document.getElementById("sendAnswer");
  const targetInput = document.getElementById("target");
  const userSuggestions = document.getElementById("userSuggestions");
  const insightTarget = document.getElementById("insightTarget");
  const insightSuggestions = document.getElementById("insightSuggestions");
  const choiceButtons = document.querySelectorAll(".choice-btn");
  const choiceGroups = Array.from(document.querySelectorAll(".choice-group"));

  const profileBlock = document.getElementById("profileBlock");
  const answerBlock = document.getElementById("answerBlock");
  const insightBlock = document.getElementById("insightBlock");
  const tabProfile = document.getElementById("tabProfile");
  const tabAnswer = document.getElementById("tabAnswer");
  const tabInsight = document.getElementById("tabInsight");
  const selected = {
    tone: "",
    speed: "",
    contact_format: "",
    caution: "",
  };
  let showingForeignProfile = false;
  let ownProfileLink = "";
  let inviteLink = "";
  let foreignProfileIsAppUser = true;
  let foreignProfileUsername = "";
  let answerFlowTarget = "";
  let answerFlowStep = -1;
  const ANSWER_FIELDS = ["tone", "speed", "contact_format", "caution"];

  function setTab(name) {
    profileBlock.style.display = name === "profile" ? "block" : "none";
    answerBlock.style.display = name === "answer" ? "block" : "none";
    insightBlock.style.display = name === "insight" ? "block" : "none";
    tabProfile.classList.toggle("active", name === "profile");
    tabAnswer.classList.toggle("active", name === "answer");
    tabInsight.classList.toggle("active", name === "insight");
  }

  function setForeignProfileView() {
    profileBlock.style.display = "block";
    answerBlock.style.display = "none";
    insightBlock.style.display = "none";
    tabProfile.classList.remove("active");
    tabAnswer.classList.remove("active");
    tabInsight.classList.add("active");
    updateShareState();
  }

  function updateShareState() {
    if (!copyLink) return;
    if (showingForeignProfile && foreignProfileIsAppUser) {
      copyLink.disabled = true;
      copyLink.textContent = "Поделиться";
      copyLink.title = "Можно делиться только своим профилем";
      return;
    }
    if (showingForeignProfile && !foreignProfileIsAppUser) {
      copyLink.disabled = false;
      copyLink.textContent = "Пригласить";
      copyLink.title = "";
      return;
    } else {
      copyLink.disabled = false;
      copyLink.textContent = "Поделиться";
      copyLink.title = "";
    }
  }

  tabProfile.addEventListener("click", async () => {
    setTab("profile");
    if (showingForeignProfile) {
      authStatus.textContent = "Возвращаем твой профиль...";
      await loadProfile();
    }
  });
  tabAnswer.addEventListener("click", () => setTab("answer"));
  tabInsight.addEventListener("click", () => {
    setTab("insight");
    loadInsightSuggestions();
  });
  insightTarget.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      const value = insightTarget.value.trim();
      if (!value) return;
      addToLocalInsightHistory(value);
      loadTargetProfile(value);
      loadInsightSuggestions();
    }
  });
  choiceButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const field = btn.dataset.field;
      const value = btn.dataset.value;
      if (!field || !value) return;
      const group = btn.closest(".choice-group");
      const groupIndex = group ? Number(group.dataset.step || -1) : -1;
      // Allow changing already opened steps; block only steps not opened yet.
      if (groupIndex < 0 || groupIndex > answerFlowStep) return;
      selected[field] = value;
      document.querySelectorAll(`.choice-btn[data-field="${field}"]`).forEach((b) => {
        b.classList.toggle("active", b === btn);
      });
      if (groupIndex === answerFlowStep && field === ANSWER_FIELDS[answerFlowStep]) {
        revealNextAnswerStep();
      }
    });
  });

  async function api(path, options) {
    const res = await fetch(path, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        ...(previewMode ? {} : { "X-Telegram-Init-Data": initData }),
        ...(options && options.headers ? options.headers : {}),
      },
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.ok === false) {
      throw new Error(data.error || ("HTTP " + res.status));
    }
    return data;
  }

  function recommendationText(rec) {
    return {
      toneText: rec.tone === "easy" ? "с юмора" : "спокойно, по делу",
      speedText: rec.speed === "slow" ? "не торопясь" : "сразу",
      formatText: rec.format === "text" ? "через переписку" : "в живом общении",
      toneEmoji: rec.tone === "easy" ? "😄" : "🧠",
      speedEmoji: rec.speed === "slow" ? "🐢" : "🔥",
      formatEmoji: rec.format === "text" ? "💬" : "🎤",
    };
  }

  function buildSummaryText(d) {
    if (!d.enough) {
      return (
        "Похоже, кто-то уже отвечал.\n" +
        "Нужно ещё пару ответов,\nчтобы собрать понятную картину."
      );
    }

    const rec = recommendationText(d.recommendation);
    let text =
      "Как к тебе проще начать общение\n\n" +
      rec.toneEmoji +
      " " +
      rec.toneText +
      "\n" +
      rec.speedEmoji +
      " " +
      rec.speedText +
      "\n" +
      rec.formatEmoji +
      " " +
      rec.formatText;
    return text;
  }

  function setAvatar(user) {
    const name = (user && (user.first_name || user.username || user.last_name)) || "User";
    const firstName = user && user.first_name ? String(user.first_name).trim() : "";
    const lastName = user && user.last_name ? String(user.last_name).trim() : "";
    const usernameRaw = user && user.username ? String(user.username).replace(/^@/, "") : "";
    const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
    if (fullName) {
      profileName.textContent = fullName;
    } else if (usernameRaw) {
      profileName.textContent = "Пользователь @" + usernameRaw;
    } else {
      profileName.textContent = name;
    }
    avatarFallback.textContent = (name || "U").slice(0, 1).toUpperCase();
    const username = usernameRaw;
    const proxyPhoto = user && user.avatar_url ? String(user.avatar_url) : "";
    const directPhoto = user && user.photo_url ? String(user.photo_url) : "";
    const fallbackPhoto = username ? "https://t.me/i/userpic/320/" + username + ".jpg" : "";
    const candidates = [directPhoto, proxyPhoto, fallbackPhoto].filter(Boolean);

    if (candidates.length > 0) {
      let idx = 0;
      avatarImg.style.display = "block";
      avatarFallback.style.display = "none";
      avatarImg.onerror = function () {
        idx += 1;
        if (idx >= candidates.length) {
          avatarImg.style.display = "none";
          avatarFallback.style.display = "grid";
          return;
        }
        avatarImg.src = candidates[idx];
      };
      avatarImg.src = candidates[idx];
    } else {
      avatarImg.style.display = "none";
      avatarFallback.style.display = "grid";
    }
  }

  function renderProfile(d) {
    profileLink.textContent = d.link || "—";
    inviteLink = d.invite_link || inviteLink;

    setAvatar(d.user || {});
    summaryBubble.textContent = buildSummaryText(d);
    const notes = [];
    if (d.caution_block) notes.push("⚠️ иногда лучше не давить");
    if (d.uncertain_block) notes.push("ℹ️ по некоторым пунктам мнения расходятся");
    summaryNotes.textContent = notes.join("\n");

    if (d.enough && d.recommendation) {
      const rec = recommendationText(d.recommendation);
      chipTone.textContent = rec.toneEmoji;
      chipSpeed.textContent = rec.speedEmoji;
      chipFormat.textContent = rec.formatEmoji;
    } else {
      chipTone.textContent = "❓";
      chipSpeed.textContent = "❓";
      chipFormat.textContent = "❓";
    }
  }

  async function loadProfile() {
    try {
      const endpoint = previewMode ? "/api/miniapp/preview" : "/api/miniapp/me";
      const resp = await api(endpoint);
      renderProfile(resp.data);
      showingForeignProfile = false;
      foreignProfileIsAppUser = true;
      foreignProfileUsername = "";
      ownProfileLink = (resp.data && resp.data.link) || ownProfileLink;
      updateShareState();
      authStatus.textContent = "";
    } catch (e) {
      summaryBubble.textContent = "Ошибка: " + e.message;
      authStatus.textContent = "Ошибка авторизации";
    }
  }

  async function loadTargetProfile(target) {
    const normalized = normalizeName(target);
    if (!normalized) return;
    try {
      if (previewMode) {
        const resp = await api("/api/miniapp/preview");
        const data = { ...resp.data };
        data.user = {
          ...(data.user || {}),
          username: normalized.replace("@", ""),
          first_name: "",
          last_name: "",
          photo_url: "",
        };
        data.target = normalized;
        data.link = "https://t.me/getxposedbot?start=ref_" + normalized.replace("@", "");
        data.is_app_user = false;
        renderProfile(data);
        foreignProfileIsAppUser = false;
      } else {
        const resp = await api("/api/miniapp/profile?target=" + encodeURIComponent(normalized));
        renderProfile(resp.data);
        foreignProfileIsAppUser = !!resp.data.is_app_user;
      }
      showingForeignProfile = true;
      foreignProfileUsername = normalized;
      updateShareState();
      authStatus.textContent = "Профиль " + normalized;
      setForeignProfileView();
    } catch (e) {
      summaryBubble.textContent = "Ошибка: " + e.message;
      setForeignProfileView();
    }
  }

  function normalizeName(name) {
    let v = String(name || "").trim().toLowerCase();
    if (!v) return "";
    if (!v.startsWith("@")) v = "@" + v;
    return v;
  }

  function clearAnswerSelections() {
    ANSWER_FIELDS.forEach((field) => {
      selected[field] = "";
      document.querySelectorAll(`.choice-btn[data-field="${field}"]`).forEach((b) => {
        b.classList.remove("active");
      });
    });
  }

  function resetAnswerFlow() {
    answerFlowStep = -1;
    answerFlowTarget = "";
    clearAnswerSelections();
    choiceGroups.forEach((g) => g.classList.remove("visible"));
    sendAnswerBtn.style.display = "none";
    answerTargetTitle.textContent = "";
  }

  function revealStep(index) {
    if (index < 0 || index >= choiceGroups.length) return;
    choiceGroups[index].classList.add("visible");
  }

  function revealNextAnswerStep() {
    const next = answerFlowStep + 1;
    if (next < choiceGroups.length) {
      answerFlowStep = next;
      revealStep(next);
      return;
    }
    answerFlowStep = choiceGroups.length;
    sendAnswerBtn.style.display = "block";
  }

  async function resolveTargetDisplayName(target) {
    if (previewMode) return target;
    try {
      const resp = await api("/api/miniapp/profile?target=" + encodeURIComponent(target));
      const user = resp.data && resp.data.user ? resp.data.user : null;
      if (!user) return target;
      const fn = (user.first_name || "").trim();
      const ln = (user.last_name || "").trim();
      const full = [fn, ln].filter(Boolean).join(" ").trim();
      if (full) return full;
      if (user.username) return "@" + String(user.username).replace(/^@/, "");
      return target;
    } catch (e) {
      return target;
    }
  }

  async function startAnswerFlow(rawTarget) {
    const target = normalizeName(rawTarget);
    if (!target) {
      resetAnswerFlow();
      return;
    }
    if (answerFlowTarget === target && answerFlowStep >= 0) return;
    resetAnswerFlow();
    answerFlowTarget = target;
    answerStatus.textContent = "Подготовка...";
    const displayName = await resolveTargetDisplayName(target);
    answerTargetTitle.textContent = "Оставляем отзыв о " + displayName;
    answerStatus.textContent = "";
    revealNextAnswerStep();
  }

  function addToLocalInsightHistory(name) {
    const normalized = normalizeName(name);
    if (!normalized) return;
    const key = "miniapp_insight_recent";
    const raw = localStorage.getItem(key);
    const arr = raw ? JSON.parse(raw) : [];
    const next = [normalized, ...arr.filter((x) => x !== normalized)].slice(0, 20);
    localStorage.setItem(key, JSON.stringify(next));
  }

  function getLocalInsightHistory() {
    const key = "miniapp_insight_recent";
    const raw = localStorage.getItem(key);
    if (!raw) return [];
    try {
      const arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr.map(normalizeName).filter(Boolean) : [];
    } catch (e) {
      return [];
    }
  }

  function renderInsightSuggestions(items) {
    const unique = [];
    const seen = new Set();
    items.map(normalizeName).filter(Boolean).forEach((x) => {
      if (!seen.has(x)) {
        seen.add(x);
        unique.push(x);
      }
    });
    insightSuggestions.innerHTML = "";
    unique.slice(0, 20).forEach((u) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "item";
      btn.textContent = u;
      btn.addEventListener("click", () => {
        insightTarget.value = u;
        addToLocalInsightHistory(u);
        loadTargetProfile(u);
      });
      insightSuggestions.appendChild(btn);
    });
  }

  async function loadInsightSuggestions() {
    const localItems = getLocalInsightHistory();
    try {
      const endpoint = previewMode
        ? "/api/miniapp/preview-recent-targets"
        : "/api/miniapp/recent-targets";
      const resp = await api(endpoint);
      const remoteItems = Array.isArray(resp.items) ? resp.items : [];
      renderInsightSuggestions([...localItems, ...remoteItems]);
    } catch (e) {
      renderInsightSuggestions(localItems);
    }
  }

  let searchTimer = null;
  async function searchUsers(query) {
    try {
      const endpoint = previewMode
        ? "/api/miniapp/preview-users"
        : "/api/miniapp/search-users?q=" + encodeURIComponent(query);
      const resp = await api(endpoint);
      const items = Array.isArray(resp.items) ? resp.items : [];
      userSuggestions.innerHTML = "";
      items.slice(0, 20).forEach((username) => {
        const opt = document.createElement("option");
        opt.value = username;
        userSuggestions.appendChild(opt);
      });
    } catch (e) {
      userSuggestions.innerHTML = "";
    }
  }

  targetInput.addEventListener("input", function () {
    const q = (targetInput.value || "").trim();
    if (searchTimer) clearTimeout(searchTimer);
    if (q.length < 2) {
      userSuggestions.innerHTML = "";
      return;
    }
    searchTimer = setTimeout(() => {
      searchUsers(q);
    }, 180);
  });
  targetInput.addEventListener("change", () => {
    startAnswerFlow(targetInput.value);
  });
  targetInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      startAnswerFlow(targetInput.value);
    }
  });

  sendAnswerBtn.addEventListener("click", async function () {
    answerStatus.textContent = "Отправка...";
    if (!answerFlowTarget) {
      answerStatus.textContent = "Сначала укажи @username.";
      return;
    }
    if (answerFlowTarget.replace(/^@/, "").toLowerCase().endsWith("bot")) {
      answerStatus.textContent = "Нельзя оставлять отзывы о ботах.";
      return;
    }
    if (!selected.tone || !selected.speed || !selected.contact_format || !selected.caution) {
      answerStatus.textContent = "Выбери все варианты перед отправкой.";
      return;
    }
    const payload = {
      target: answerFlowTarget,
      tone: selected.tone,
      speed: selected.speed,
      contact_format: selected.contact_format,
      caution: selected.caution,
    };
    try {
      addToLocalInsightHistory(payload.target);
      const endpoint = previewMode ? "/api/miniapp/preview-feedback" : "/api/miniapp/feedback";
      const resp = await api(endpoint, { method: "POST", body: JSON.stringify(payload) });
      answerStatus.textContent = resp.message || "Отправлено";
      await loadProfile();
      targetInput.value = "";
      resetAnswerFlow();
    } catch (e) {
      answerStatus.textContent = e.message;
    }
  });

  copyLink.addEventListener("click", async function () {
    if (showingForeignProfile) {
      if (foreignProfileIsAppUser) {
        showCopyToast("Можно делиться только своей ссылкой.");
        return;
      }
    }
    const text = showingForeignProfile ? inviteLink : (ownProfileLink || profileLink.textContent || "");
    if (!text || text === "—") return;
    const shareText = showingForeignProfile
      ? ("Приглашаю @" + foreignProfileUsername.replace(/^@/, "") + " в приложение")
      : "Предложить оценить меня анонимно";
    const shareUrl = "https://t.me/share/url?url=" + encodeURIComponent(text) + "&text=" + encodeURIComponent(shareText);
    try {
      if (tg && typeof tg.openTelegramLink === "function") {
        tg.openTelegramLink(shareUrl);
      } else if (navigator.share) {
        await navigator.share({ text: shareText, url: text });
      } else {
        await navigator.clipboard.writeText(text);
      }
      copyLink.textContent = "Готово";
      showCopyToast(
        "Ссылка готова к отправке.\nЭффектнее всего делиться ею в общих чатах или в сторис."
      );
      setTimeout(() => (copyLink.textContent = "Поделиться"), 1200);
    } catch (e) {
      copyLink.textContent = "Ошибка";
      setTimeout(() => (copyLink.textContent = "Поделиться"), 1200);
    }
  });

  let toastTimer = null;
  function showCopyToast(text) {
    if (!copyToast) return;
    copyToast.textContent = text;
    copyToast.classList.add("show");
    if (toastTimer) clearTimeout(toastTimer);
    toastTimer = setTimeout(() => {
      copyToast.classList.remove("show");
    }, 3200);
  }

  if (tg) {
    tg.ready();
    tg.expand();
    if (tg.themeParams && tg.themeParams.button_color) {
      document.documentElement.style.setProperty("--blue", tg.themeParams.button_color);
    }
  }
  async function initApp() {
    loadInsightSuggestions();
    await loadProfile();
    resetAnswerFlow();
    const rateTarget = normalizeName(urlParams.get("rate") || "");
    if (rateTarget) {
      setTab("answer");
      targetInput.value = rateTarget;
      await startAnswerFlow(rateTarget);
    }
  }

  initApp();
})();
