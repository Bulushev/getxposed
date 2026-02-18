(function () {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  const initData = tg ? tg.initData || "" : "";
  const urlParams = new URLSearchParams(window.location.search);
  const previewMode = urlParams.get("preview") === "1";

  const authStatus = document.getElementById("authStatus");
  const profileLink = document.getElementById("profileLink");
  const copyLink = document.getElementById("copyLink");
  const editProfileBtn = document.getElementById("editProfileBtn");
  const profileEditor = document.getElementById("profileEditor");
  const profileNoteInput = document.getElementById("profileNoteInput");
  const saveProfileBtn = document.getElementById("saveProfileBtn");
  const cancelProfileBtn = document.getElementById("cancelProfileBtn");
  const summaryBubble = document.getElementById("summaryBubble");
  const summaryBubbleWrap = document.querySelector(".bubble-wrap");
  const metricVisitors = document.getElementById("metricVisitors");
  const metricAnswers = document.getElementById("metricAnswers");
  const summaryNotes = document.getElementById("summaryNotes");
  const answersBlock = document.getElementById("answersBlock");
  const answersList = document.getElementById("answersList");
  const answersTitle = document.getElementById("answersTitle");
  const profileNote = document.getElementById("profileNote");
  const profileName = document.getElementById("profileName");
  const profileHeadLink = document.querySelector(".profile-head");
  const profilePresence = document.getElementById("profilePresence");
  const avatarImg = document.getElementById("avatarImg");
  const avatarFallback = document.getElementById("avatarFallback");
  const chipTone = document.getElementById("chipTone");
  const chipSpeed = document.getElementById("chipSpeed");
  const chipFormat = document.getElementById("chipFormat");
  const chipsContainer = document.querySelector(".chips");
  const copyToast = document.getElementById("copyToast");

  const answerStatus = document.getElementById("answerStatus");
  const answerTargetHead = document.getElementById("answerTargetHead");
  const answerTargetTitle = document.getElementById("answerTargetTitle");
  const answerTargetAvatar = document.getElementById("answerTargetAvatar");
  const answerTargetAvatarImg = document.getElementById("answerTargetAvatarImg");
  const answerTargetAvatarFallback = document.getElementById("answerTargetAvatarFallback");
  const answerProgress = document.getElementById("answerProgress");
  const answerProgressBar = document.getElementById("answerProgressBar");
  const answerIntro = document.getElementById("answerIntro");
  const answerHint = document.getElementById("answerHint");
  const answerBackBtn = document.getElementById("answerBackBtn");
  const swipeCard = document.getElementById("swipeCard");
  const swipeTitle = document.getElementById("swipeTitle");
  const swipeLeft = document.getElementById("swipeLeft");
  const swipeRight = document.getElementById("swipeRight");
  const swipeBadgeLeft = document.getElementById("swipeBadgeLeft");
  const swipeBadgeRight = document.getElementById("swipeBadgeRight");
  const sendAnswerBtn = document.getElementById("sendAnswer");
  const targetInput = document.getElementById("target");
  const userSuggestions = document.getElementById("userSuggestions");
  const insightTarget = document.getElementById("insightTarget");
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
    initiative: "",
    start_context: "",
    attention_reaction: "",
    caution: "",
    frequency: "",
    comm_format: "",
    uncertainty: "",
  };
  let showingForeignProfile = false;
  let ownProfileLink = "";
  let inviteLink = "";
  let foreignProfileIsAppUser = true;
  let foreignProfileUsername = "";
  let currentProfileNoteText = "";
  let answerFlowTarget = "";
  let answerFlowStep = -1;
  let currentProfileUsername = "";
  const BASE_ANSWER_FIELDS = [
    "tone",
    "speed",
    "contact_format",
    "initiative",
    "start_context",
    "attention_reaction",
    "caution",
    "frequency",
  ];
  let flowAnswerFields = [...BASE_ANSWER_FIELDS];
  let adaptiveToneQuestion = false;
  let adaptiveStructureQuestion = false;
  const flowFieldSet = new Set(BASE_ANSWER_FIELDS);
  let swipeStartX = 0;
  let swipeStartY = 0;
  let swipeStarted = false;
  let swipeLocked = false;
  const groupByField = {};
  choiceGroups.forEach((group) => {
    const firstBtn = group.querySelector(".choice-btn[data-field]");
    const field = firstBtn ? firstBtn.dataset.field : "";
    if (field) groupByField[field] = group;
  });

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
    if (editProfileBtn) {
      editProfileBtn.style.display = showingForeignProfile ? "none" : "inline-flex";
    }
    if (profileEditor) profileEditor.style.display = "none";

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

  function updateAnswersTitle() {
    if (!answersTitle) return;
    answersTitle.textContent = showingForeignProfile
      ? "Каким его/её видят другие"
      : "Каким тебя видят другие";
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
  });
  insightTarget.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      const value = insightTarget.value.trim();
      if (!value) return;
      loadTargetProfile(value);
    }
  });
  choiceButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const field = btn.dataset.field;
      const value = btn.dataset.value;
      if (!field || !value) return;
      if (!flowFieldSet.has(field)) return;
      const groupIndex = flowAnswerFields.indexOf(field);
      // Allow changing already opened steps; block only steps not opened yet.
      if (groupIndex < 0 || groupIndex > answerFlowStep) return;
      selected[field] = value;
      updateAnswerProgress();
      document.querySelectorAll(`.choice-btn[data-field="${field}"]`).forEach((b) => {
        b.classList.toggle("active", b === btn);
      });
      if (groupIndex === answerFlowStep && field === flowAnswerFields[answerFlowStep]) {
        revealNextAnswerStep();
      }
    });
  });

  function getVisibleChoiceRow() {
    const active = choiceGroups.find((g) => g.classList.contains("visible"));
    if (!active) return null;
    return active.querySelector(".choice-row");
  }

  function setSwipeBadges(dx) {
    if (!swipeCard || !swipeBadgeLeft || !swipeBadgeRight) return;
    const strength = Math.min(1, Math.abs(dx) / 120);
    swipeCard.classList.toggle("swipe-left", dx < -24);
    swipeCard.classList.toggle("swipe-right", dx > 24);
    swipeBadgeLeft.style.opacity = dx < 0 ? String(strength) : "0";
    swipeBadgeRight.style.opacity = dx > 0 ? String(strength) : "0";
    swipeBadgeLeft.style.transform = dx < 0 ? "scale(1)" : "scale(.92)";
    swipeBadgeRight.style.transform = dx > 0 ? "scale(1)" : "scale(.92)";
  }

  function resetSwipeCardVisual(animated = true) {
    if (!swipeCard) return;
    swipeCard.style.transition = animated ? "transform .18s ease" : "none";
    swipeCard.style.transform = "translateX(0px) rotate(0deg)";
    swipeCard.classList.remove("swipe-left", "swipe-right");
    if (swipeBadgeLeft) swipeBadgeLeft.style.opacity = "0";
    if (swipeBadgeRight) swipeBadgeRight.style.opacity = "0";
    if (swipeBadgeLeft) swipeBadgeLeft.style.transform = "scale(.92)";
    if (swipeBadgeRight) swipeBadgeRight.style.transform = "scale(.92)";
  }

  function pickByDirection(direction) {
    const row = getVisibleChoiceRow();
    if (!row) return;
    const options = row.querySelectorAll(".choice-btn");
    if (!options || options.length < 2) return;
    if (direction < 0) {
      options[0].click();
    } else {
      options[1].click();
    }
  }

  function animateAndPick(direction) {
    if (!swipeCard || swipeLocked) return;
    swipeLocked = true;
    const outX = direction < 0 ? -Math.max(window.innerWidth, 420) : Math.max(window.innerWidth, 420);
    const rotate = direction < 0 ? -14 : 14;
    swipeCard.style.transition = "transform .22s cubic-bezier(.2,.8,.2,1)";
    swipeCard.style.transform = `translateX(${outX}px) rotate(${rotate}deg)`;
    setTimeout(() => {
      resetSwipeCardVisual(false);
      pickByDirection(direction);
      swipeLocked = false;
    }, 220);
  }

  function applyDrag(dx, dy) {
    if (!swipeCard || swipeLocked) return;
    if (Math.abs(dy) > Math.abs(dx)) return;
    const rotate = Math.max(-12, Math.min(12, dx / 14));
    swipeCard.style.transition = "none";
    swipeCard.style.transform = `translateX(${dx}px) rotate(${rotate}deg)`;
    setSwipeBadges(dx);
  }

  function finishDrag(dx, dy) {
    if (swipeLocked) return;
    if (Math.abs(dy) > Math.abs(dx)) {
      resetSwipeCardVisual(true);
      return;
    }
    if (Math.abs(dx) < 72) {
      resetSwipeCardVisual(true);
      return;
    }
    animateAndPick(dx < 0 ? -1 : 1);
  }

  if (swipeCard) {
    swipeCard.addEventListener("touchstart", (event) => {
      if (!event.touches || event.touches.length !== 1 || swipeLocked) return;
      swipeStarted = true;
      swipeStartX = event.touches[0].clientX;
      swipeStartY = event.touches[0].clientY;
    }, { passive: true });

    swipeCard.addEventListener("touchmove", (event) => {
      if (!swipeStarted || !event.touches || !event.touches.length) return;
      const dx = event.touches[0].clientX - swipeStartX;
      const dy = event.touches[0].clientY - swipeStartY;
      applyDrag(dx, dy);
    }, { passive: true });

    swipeCard.addEventListener("touchend", (event) => {
      if (!swipeStarted || !event.changedTouches || !event.changedTouches.length) return;
      swipeStarted = false;
      const dx = event.changedTouches[0].clientX - swipeStartX;
      const dy = event.changedTouches[0].clientY - swipeStartY;
      finishDrag(dx, dy);
    }, { passive: true });

    swipeCard.addEventListener("mousedown", (event) => {
      if (swipeLocked) return;
      swipeStarted = true;
      swipeStartX = event.clientX;
      swipeStartY = event.clientY;
    });

    window.addEventListener("mousemove", (event) => {
      if (!swipeStarted) return;
      const dx = event.clientX - swipeStartX;
      const dy = event.clientY - swipeStartY;
      applyDrag(dx, dy);
    });

    window.addEventListener("mouseup", (event) => {
      if (!swipeStarted) return;
      swipeStarted = false;
      const dx = event.clientX - swipeStartX;
      const dy = event.clientY - swipeStartY;
      finishDrag(dx, dy);
    });
  }

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

    return "Как к тебе проще начать общение\n\nСобрали полную картину по карточкам ниже.";
  }

  function pluralRu(n, one, two, many) {
    const v = Math.abs(Number(n) || 0) % 100;
    const n1 = v % 10;
    if (v > 10 && v < 20) return many;
    if (n1 > 1 && n1 < 5) return two;
    if (n1 === 1) return one;
    return many;
  }

  function setAvatar(user) {
    const name = (user && (user.first_name || user.username || user.last_name)) || "User";
    const firstName = user && user.first_name ? String(user.first_name).trim() : "";
    const lastName = user && user.last_name ? String(user.last_name).trim() : "";
    const usernameRaw = user && user.username ? String(user.username).replace(/^@/, "") : "";
    currentProfileUsername = usernameRaw ? usernameRaw.toLowerCase() : "";
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

  function openTelegramProfile(username) {
    const uname = String(username || "").replace(/^@/, "").trim();
    if (!uname) {
      showCopyToast("Не удалось открыть профиль: нет username.");
      return;
    }
    const url = "https://t.me/" + uname;
    if (tg && typeof tg.openTelegramLink === "function") {
      tg.openTelegramLink(url);
    } else {
      window.open(url, "_blank", "noopener,noreferrer");
    }
  }

  function renderProfile(d, isForeign = showingForeignProfile) {
    profileLink.textContent = d.link || "—";
    inviteLink = d.invite_link || inviteLink;
    if (profilePresence) {
      profilePresence.style.display = d.is_app_user ? "inline-flex" : "none";
    }

    setAvatar(d.user || {});
    const noteText = String(d.profile_note || "").trim();
    const visitors = Number(d.visitors || 0);
    const answers = Number(d.answers || 0);
    if (metricVisitors) {
      metricVisitors.textContent = `${visitors} ${pluralRu(visitors, "человек", "человека", "человек")}`;
    }
    if (metricAnswers) {
      metricAnswers.textContent = `${answers} ${pluralRu(answers, "ответ", "ответа", "ответов")}`;
    }
    currentProfileNoteText = noteText;
    const hideBubble = isForeign && d.enough && !noteText;
    if (summaryBubbleWrap) {
      summaryBubbleWrap.style.display = hideBubble ? "none" : "";
    }
    if (!isForeign) {
      const showOwnPlaceholder = !noteText;
      summaryBubble.textContent = noteText || "напиши что-нибудь о себе";
      summaryBubble.classList.toggle("placeholder", showOwnPlaceholder);
    } else if (!d.enough) {
      summaryBubble.textContent = buildSummaryText(d);
      summaryBubble.classList.remove("placeholder");
    } else {
      summaryBubble.textContent = noteText;
      summaryBubble.classList.remove("placeholder");
    }
    const extraHint = d.extra_hint ? String(d.extra_hint) : "";
    summaryNotes.textContent = extraHint;
    summaryNotes.classList.toggle("has-icon", Boolean(extraHint));
    if (profileNote) {
      profileNote.style.display = "none";
      profileNote.textContent = "";
    }

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

    if (answersBlock && answersList) {
      const rows = Array.isArray(d.result_rows) ? d.result_rows : [];
      if (rows.length) {
        answersList.innerHTML = "";
        rows.forEach((card) => {
          const row = document.createElement("div");
          row.className = "answers-item";
          const title = document.createElement("div");
          title.className = "answers-item-title";
          title.textContent = card.title || "";
          const value = document.createElement("div");
          value.className = "answers-item-value";
          value.textContent = card.value || "";
          row.appendChild(title);
          row.appendChild(value);
          answersList.appendChild(row);
        });
        answersBlock.style.display = "block";
      } else {
        answersList.innerHTML = "";
        answersBlock.style.display = "none";
      }
    }
  }

  async function loadProfile() {
    try {
      const endpoint = previewMode ? "/api/miniapp/preview" : "/api/miniapp/me";
      const resp = await api(endpoint);
      renderProfile(resp.data, false);
      showingForeignProfile = false;
      foreignProfileIsAppUser = true;
      foreignProfileUsername = "";
      ownProfileLink = (resp.data && resp.data.link) || ownProfileLink;
      updateShareState();
      updateAnswersTitle();
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
        renderProfile(data, true);
        foreignProfileIsAppUser = false;
      } else {
        const resp = await api("/api/miniapp/profile?target=" + encodeURIComponent(normalized));
        renderProfile(resp.data, true);
        foreignProfileIsAppUser = !!resp.data.is_app_user;
      }
      showingForeignProfile = true;
      foreignProfileUsername = normalized;
      updateShareState();
      updateAnswersTitle();
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

  function configureAdaptiveFlow(adaptive) {
    adaptiveToneQuestion = !!(adaptive && adaptive.ask_tone_question);
    adaptiveStructureQuestion = !!(adaptive && adaptive.ask_uncertainty_question);
    flowAnswerFields = [...BASE_ANSWER_FIELDS];
    if (adaptiveToneQuestion) flowAnswerFields.push("comm_format");
    if (adaptiveStructureQuestion) flowAnswerFields.push("uncertainty");
    flowFieldSet.clear();
    flowAnswerFields.forEach((f) => flowFieldSet.add(f));
  }

  function clearAnswerSelections() {
    Object.keys(selected).forEach((field) => {
      selected[field] = "";
      document.querySelectorAll(`.choice-btn[data-field="${field}"]`).forEach((b) => {
        b.classList.remove("active");
      });
    });
  }

  function updateAnswerProgress() {
    if (!answerProgress || !answerProgressBar) return;
    const total = flowAnswerFields.length;
    const done = flowAnswerFields.filter((field) => Boolean(selected[field])).length;
    const pct = total > 0 ? Math.round((done / total) * 100) : 0;
    answerProgressBar.style.width = pct + "%";
  }

  function getChoicePayload(stepIndex) {
    if (stepIndex < 0 || stepIndex >= flowAnswerFields.length) return null;
    const field = flowAnswerFields[stepIndex];
    const group = groupByField[field];
    if (!group) return null;
    const title = ((group.querySelector(".choice-title") || {}).textContent || "").trim();
    const buttons = group.querySelectorAll(".choice-btn");
    if (!buttons || buttons.length < 2) return null;
    const leftBtn = buttons[0];
    const rightBtn = buttons[1];
    const leftEm = ((leftBtn.querySelector(".em") || {}).textContent || "").trim();
    const rightEm = ((rightBtn.querySelector(".em") || {}).textContent || "").trim();
    const leftText = leftBtn.textContent.replace(leftEm, "").trim();
    const rightText = rightBtn.textContent.replace(rightEm, "").trim();
    return {
      title,
      left: { em: leftEm, text: leftText },
      right: { em: rightEm, text: rightText },
    };
  }

  function renderSwipeCard(stepIndex) {
    if (!swipeCard || !swipeTitle || !swipeLeft || !swipeRight) return;
    const payload = getChoicePayload(stepIndex);
    if (!payload) {
      swipeCard.style.display = "none";
      return;
    }
    swipeTitle.textContent = payload.title;
    swipeLeft.innerHTML = "<b>← влево</b>" + payload.left.em + " " + payload.left.text;
    swipeRight.innerHTML = "<b>вправо →</b>" + payload.right.em + " " + payload.right.text;
    swipeCard.style.display = "grid";
  }

  function resetAnswerFlow() {
    answerFlowStep = -1;
    answerFlowTarget = "";
    configureAdaptiveFlow({ ask_tone_question: false, ask_uncertainty_question: false });
    clearAnswerSelections();
    choiceGroups.forEach((g) => g.classList.remove("visible"));
    sendAnswerBtn.style.display = "none";
    answerTargetTitle.textContent = "";
    if (answerTargetHead) answerTargetHead.style.display = "none";
    if (answerTargetAvatar) answerTargetAvatar.style.display = "none";
    targetInput.style.display = "";
    userSuggestions.style.display = "";
    if (answerIntro) answerIntro.style.display = "";
    if (answerHint) answerHint.style.display = "";
    if (answerBackBtn) answerBackBtn.style.display = "none";
    if (swipeCard) swipeCard.style.display = "none";
    if (answerProgress) answerProgress.style.display = "none";
    updateAnswerProgress();
    resetSwipeCardVisual(false);
  }

  function revealStep(index) {
    if (index < 0 || index >= flowAnswerFields.length) return;
    const field = flowAnswerFields[index];
    const targetGroup = groupByField[field];
    if (!targetGroup) return;
    choiceGroups.forEach((g, i) => {
      if (g !== targetGroup) g.classList.remove("visible");
    });
    targetGroup.classList.add("visible");
    renderSwipeCard(index);
  }

  function revealNextAnswerStep() {
    const next = answerFlowStep + 1;
    if (next < flowAnswerFields.length) {
      answerFlowStep = next;
      revealStep(next);
      sendAnswerBtn.style.display = next === (flowAnswerFields.length - 1) ? "block" : "none";
      return;
    }
    answerFlowStep = flowAnswerFields.length;
    sendAnswerBtn.style.display = "block";
  }

  async function resolveTargetDisplay(target) {
    if (previewMode) {
      return {
        title: target,
        avatar_url: "",
        first_name: "",
        username: target.replace(/^@/, ""),
        adaptive: {
          ask_tone_question: false,
          ask_uncertainty_question: false,
        },
      };
    }
    try {
      const resp = await api("/api/miniapp/profile?target=" + encodeURIComponent(target));
      const user = resp.data && resp.data.user ? resp.data.user : null;
      if (!user) {
        return { title: target, avatar_url: "", first_name: "", username: target.replace(/^@/, "") };
      }
      const fn = (user.first_name || "").trim();
      const ln = (user.last_name || "").trim();
      const full = [fn, ln].filter(Boolean).join(" ").trim();
      return {
        title: full || (user.username ? "@" + String(user.username).replace(/^@/, "") : target),
        avatar_url: String(user.avatar_url || user.photo_url || ""),
        first_name: fn,
        username: String(user.username || target.replace(/^@/, "")),
        adaptive: resp.data && resp.data.adaptive_questions ? resp.data.adaptive_questions : {
          ask_tone_question: false,
          ask_uncertainty_question: false,
        },
      };
    } catch (e) {
      return {
        title: target,
        avatar_url: "",
        first_name: "",
        username: target.replace(/^@/, ""),
        adaptive: {
          ask_tone_question: false,
          ask_uncertainty_question: false,
        },
      };
    }
  }

  function setAnswerTargetAvatar(display) {
    if (!answerTargetAvatar || !answerTargetAvatarImg || !answerTargetAvatarFallback) return;
    const username = String(display.username || "").replace(/^@/, "");
    const fallbackLetter = (String(display.first_name || username || "?").trim().slice(0, 1) || "?").toUpperCase();
    answerTargetAvatarFallback.textContent = fallbackLetter;
    const fallbackPhoto = username ? "https://t.me/i/userpic/320/" + username + ".jpg" : "";
    const candidates = [String(display.avatar_url || ""), fallbackPhoto].filter(Boolean);
    if (!candidates.length) {
      answerTargetAvatar.style.display = "block";
      answerTargetAvatarImg.style.display = "none";
      answerTargetAvatarFallback.style.display = "grid";
      return;
    }
    let idx = 0;
    answerTargetAvatar.style.display = "block";
    answerTargetAvatarImg.style.display = "block";
    answerTargetAvatarFallback.style.display = "none";
    answerTargetAvatarImg.onerror = function () {
      idx += 1;
      if (idx >= candidates.length) {
        answerTargetAvatarImg.style.display = "none";
        answerTargetAvatarFallback.style.display = "grid";
        return;
      }
      answerTargetAvatarImg.src = candidates[idx];
    };
    answerTargetAvatarImg.src = candidates[idx];
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
    const display = await resolveTargetDisplay(target);
    configureAdaptiveFlow(display.adaptive || {});
    answerTargetTitle.textContent = "Оставляем ответ о " + display.title;
    if (answerTargetHead) answerTargetHead.style.display = "flex";
    setAnswerTargetAvatar(display);
    targetInput.style.display = "none";
    if (answerIntro) answerIntro.style.display = "none";
    if (answerHint) answerHint.style.display = "none";
    if (answerBackBtn) answerBackBtn.style.display = "none";
    if (swipeCard) swipeCard.style.display = "grid";
    if (answerProgress) answerProgress.style.display = "none";
    updateAnswerProgress();
    answerStatus.textContent = "";
    revealNextAnswerStep();
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
    if (flowAnswerFields.some((field) => !selected[field])) {
      answerStatus.textContent = "Выбери все варианты перед отправкой.";
      return;
    }
    const payload = {
      target: answerFlowTarget,
      tone: selected.tone,
      speed: selected.speed,
      contact_format: selected.contact_format,
      initiative: selected.initiative,
      start_context: selected.start_context,
      attention_reaction: selected.attention_reaction,
      caution: selected.caution,
      frequency: selected.frequency,
    };
    if (flowFieldSet.has("comm_format")) payload.comm_format = selected.comm_format;
    if (flowFieldSet.has("uncertainty")) payload.uncertainty = selected.uncertainty;
    try {
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

  if (answerBackBtn) {
    answerBackBtn.addEventListener("click", () => {
      targetInput.value = "";
      answerStatus.textContent = "";
      resetAnswerFlow();
    });
  }

  if (editProfileBtn && profileEditor && profileNoteInput) {
    editProfileBtn.addEventListener("click", (event) => {
      event.stopPropagation();
      if (showingForeignProfile) return;
      profileNoteInput.value = currentProfileNoteText || "";
      profileEditor.style.display = "grid";
      profileNoteInput.focus();
    });
  }

  if (cancelProfileBtn && profileEditor) {
    cancelProfileBtn.addEventListener("click", () => {
      profileEditor.style.display = "none";
    });
  }

  if (saveProfileBtn && profileEditor && profileNoteInput) {
    saveProfileBtn.addEventListener("click", async () => {
      const note = String(profileNoteInput.value || "").trim();
      const lowered = note.toLowerCase();
      if (
        lowered.includes("http://") ||
        lowered.includes("https://") ||
        lowered.includes("www.") ||
        lowered.includes("t.me/")
      ) {
        showCopyToast("Ссылки в описании запрещены.");
        return;
      }
      try {
        await api("/api/miniapp/profile-note", {
          method: "POST",
          body: JSON.stringify({ note }),
        });
        profileEditor.style.display = "none";
        await loadProfile();
      } catch (e) {
        showCopyToast("Не удалось сохранить: " + e.message);
      }
    });
  }

  if (profileNoteInput) {
    profileNoteInput.addEventListener("paste", (event) => {
      const text = (event.clipboardData && event.clipboardData.getData("text")) || "";
      const lowered = String(text).toLowerCase();
      if (
        lowered.includes("http://") ||
        lowered.includes("https://") ||
        lowered.includes("www.") ||
        lowered.includes("t.me/")
      ) {
        event.preventDefault();
        showCopyToast("Ссылки вставлять нельзя.");
      }
    });
  }

  if (profileHeadLink) {
    profileHeadLink.style.cursor = "pointer";
    profileHeadLink.addEventListener("click", () => {
      openTelegramProfile(currentProfileUsername);
    });
  }

  if (chipsContainer) {
    const stopOpenProfile = (event) => {
      event.preventDefault();
      event.stopPropagation();
    };
    chipsContainer.addEventListener("click", stopOpenProfile);
    chipsContainer.addEventListener("touchstart", stopOpenProfile, { passive: false });
    chipsContainer.addEventListener("mousedown", stopOpenProfile);
  }

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
  }
  async function initApp() {
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
