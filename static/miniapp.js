(function () {
  const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
  const initData = tg ? tg.initData || "" : "";

  const authStatus = document.getElementById("authStatus");
  const profileText = document.getElementById("profileText");
  const answerStatus = document.getElementById("answerStatus");
  const insightText = document.getElementById("insightText");

  const profileBlock = document.getElementById("profileBlock");
  const answerBlock = document.getElementById("answerBlock");
  const insightBlock = document.getElementById("insightBlock");
  const tabProfile = document.getElementById("tabProfile");
  const tabAnswer = document.getElementById("tabAnswer");
  const tabInsight = document.getElementById("tabInsight");

  function setTab(name) {
    profileBlock.style.display = name === "profile" ? "block" : "none";
    answerBlock.style.display = name === "answer" ? "block" : "none";
    insightBlock.style.display = name === "insight" ? "block" : "none";
    tabProfile.classList.toggle("accent", name === "profile");
    tabAnswer.classList.toggle("accent", name === "answer");
    tabInsight.classList.toggle("accent", name === "insight");
  }

  tabProfile.addEventListener("click", () => setTab("profile"));
  tabAnswer.addEventListener("click", () => setTab("answer"));
  tabInsight.addEventListener("click", () => setTab("insight"));

  async function api(path, options) {
    const res = await fetch(path, {
      ...options,
      headers: {
        "Content-Type": "application/json",
        "X-Telegram-Init-Data": initData,
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
    const tone = rec.tone === "easy" ? "с юмора" : "спокойно, по делу";
    const speed = rec.speed === "slow" ? "не торопясь" : "сразу";
    const format = rec.format === "text" ? "через переписку" : "в живом общении";
    return [
      "Как с тобой чаще всего",
      "начинают контакт:",
      "",
      "👉 " + tone,
      "👉 " + speed,
      "👉 " + format,
    ];
  }

  async function loadProfile() {
    try {
      const resp = await api("/api/miniapp/me");
      const d = resp.data;
      const lines = [
        "твоя ссылка 👇",
        d.link,
        "",
        "👀 посмотрели — " + d.viewed,
        d.answers === 0 ? "🔥 ответов — похоже, кто-то уже заходил" : "🔥 ответов — " + d.answers,
        "👁 молча заглянули — " + d.silent,
        "",
        "— — —",
        "",
      ];
      if (!d.enough) {
        lines.push("Похоже, кто-то уже отвечал.");
        lines.push("");
        lines.push("Нужно ещё пару ответов,");
        lines.push("чтобы собрать понятную картину.");
      } else {
        lines.push(...recommendationText(d.recommendation));
        if (d.caution_block) {
          lines.push("");
          lines.push("⚠️ Иногда люди чувствуют напряжение.");
          lines.push("Лучше не давить и дать время.");
        }
        if (d.uncertain_block) {
          lines.push("");
          lines.push("По этому пункту мнения разделились —");
          lines.push("лучше ориентироваться по ситуации.");
        }
      }
      profileText.textContent = lines.join("\n");
      authStatus.textContent = "Сессия активна";
    } catch (e) {
      profileText.textContent = "Ошибка: " + e.message;
      authStatus.textContent = "Ошибка авторизации";
    }
  }

  document.getElementById("sendAnswer").addEventListener("click", async function () {
    answerStatus.textContent = "Отправка...";
    const payload = {
      target: document.getElementById("target").value.trim(),
      tone: document.getElementById("tone").value,
      speed: document.getElementById("speed").value,
      contact_format: document.getElementById("contactFormat").value,
      caution: document.getElementById("caution").value,
    };
    try {
      const resp = await api("/api/miniapp/feedback", { method: "POST", body: JSON.stringify(payload) });
      answerStatus.textContent = resp.message || "Отправлено";
      await loadProfile();
    } catch (e) {
      answerStatus.textContent = e.message;
    }
  });

  document.getElementById("getInsight").addEventListener("click", async function () {
    insightText.textContent = "Загрузка...";
    const target = document.getElementById("insightTarget").value.trim();
    try {
      const resp = await api("/api/miniapp/insight?target=" + encodeURIComponent(target), { method: "GET" });
      insightText.textContent = resp.enough ? resp.text : "Пока недостаточно ответов по этому человеку.";
    } catch (e) {
      insightText.textContent = "Ошибка: " + e.message;
    }
  });

  if (tg) {
    tg.ready();
    tg.expand();
  }
  loadProfile();
})();
