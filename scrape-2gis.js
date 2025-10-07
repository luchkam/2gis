// scrape-2gis.js
// Пример запуска: node scrape-2gis.js spb "ритуальные услуги" 1000
// 1-й аргумент: слаг города (spb, moscow, nnov, ekb, kazan, almaty, astana ...)
// 2-й аргумент: запрос (строка)
// 3-й аргумент: лимит карточек к походу (опц.; по умолчанию без лимита)

const fs = require("fs");
const path = require("path");
const puppeteer = require("puppeteer");

// Puppeteer v22+ не имеет page.waitForTimeout → используем sleep
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

// ID первой карточки на текущей странице (чтобы ждать смену контента)
async function getFirstCardId(page) {
  return await page.evaluate(() => {
    const a = Array.from(document.querySelectorAll("a"))
      .map(x => x.href)
      .find(u => (/\/(firm|place|entity)\//.test(u)) && u.includes("2gis."));
    if (!a) return null;
    const m = a.match(/\/(firm|place|entity)\/([^\/?#]+)/i);
    return m ? `${m[1]}:${m[2]}` : null;
  });
}

// Нормализация URL и извлечение уникального ID карточки (firm/place/entity)
const normalizeUrl = (u) => {
  try {
    const a = new URL(u);
    a.search = "";
    a.hash = "";
    return a.toString().replace(/\/$/, "");
  } catch {
    return String(u || "");
  }
};

const extractId = (u) => {
  try {
    const m = normalizeUrl(u).match(/\/(firm|place|entity)\/([^\/?#]+)/i);
    return m ? `${m[1]}:${m[2]}` : null; // пример: "firm:70000001006745431"
  } catch {
    return null;
  }
};

function toCsvRow(obj) {
  const esc = v =>
    String(v ?? "")
      .replace(/"/g, '""')
      .replace(/\r?\n/g, " ");
  return [
    esc(obj.name),
    esc(obj.url),
    esc(obj.phones?.join(" | ")),
    esc(obj.website),
    esc(obj.email?.join(" | ")),
    esc(obj.telegram?.join(" | ")),
    esc(obj.address)
  ]
    .map(s => `"${s}"`)
    .join(",") + "\n";
}

async function autoScroll(page, maxMs = 45000) {
  const start = Date.now();
  let lastHeight = await page.evaluate(() => document.body.scrollHeight);
  while (Date.now() - start < maxMs) {
    await page.evaluate(() => window.scrollBy(0, window.innerHeight * 0.9));
    await sleep(600);
    const newHeight = await page.evaluate(() => document.body.scrollHeight);
    if (newHeight === lastHeight) break;
    lastHeight = newHeight;
  }
}

async function run() {
  const city = process.argv[2] || "spb";
  const query = process.argv[3] || "ритуальные услуги";
  const limit = parseInt(process.argv[4] || "0", 10) || Infinity;

  // Базовый URL поиска (для KZ обычно редиректит и так; оставим .ru, чтобы не усложнять)
  const searchUrl = `https://2gis.ru/${city}/search/${encodeURIComponent(query)}`;

  const out = fs.createWriteStream(path.join(process.cwd(), `out_${city}.csv`), {
    encoding: "utf8",
  });
  out.write(`"name","card_url","phones","website","email","telegram","address"\n`);

  const browser = await puppeteer.launch({
    headless: true,
    args: ["--no-sandbox","--disable-setuid-sandbox","--lang=ru-RU,ru"]
  });
  const page = await browser.newPage();
  await page.setViewport({ width: 1366, height: 900 });
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
  );

  console.log("Открываю выдачу:", searchUrl);
  await page.goto(searchUrl, { waitUntil: "domcontentloaded", timeout: 60000 });
  await sleep(1500);

  // Определяем реальное число страниц из пагинатора
  async function detectMaxPages() {
    await autoScroll(page);
    const last = await page.evaluate(() => {
      // ссылки вида /page/2, /page/3 ...
      const nums = Array.from(document.querySelectorAll('a[href*="/page/"]'))
        .map(a => (a.getAttribute('href') || '').match(/\/page\/(\d+)/))
        .filter(Boolean)
        .map(m => parseInt(m[1], 10))
        .filter(n => Number.isFinite(n));

      // запасной вариант: берём численные подписи кнопок пагинации
      const byText = Array.from(document.querySelectorAll('a,button'))
        .map(el => (el.textContent || "").trim())
        .map(t => parseInt(t, 10))
        .filter(n => Number.isFinite(n));

      const maxLink = nums.length ? Math.max(...nums) : 1;
      const maxText = byText.length ? Math.max(...byText) : 1;
      return Math.max(maxLink, maxText, 1);
    });
    return last;
  }

  const LAST_PAGE = await detectMaxPages();
  console.log(`Найдено страниц в выдаче: ${LAST_PAGE}`);

  // --- Пагинация: строго 1..LAST_PAGE, учитываем уникальные ID карточек ---
  const seenIds = new Set();
  const urlById = new Map();

  for (let p = 1; p <= LAST_PAGE; p++) {
    const urlp1 = p === 1 ? searchUrl : `${searchUrl}/page/${p}`;
    const urlp2 = `${searchUrl}?page=${p}`; // запасной формат

    let ok = false;
    try {
      await page.goto(urlp1, { waitUntil: "domcontentloaded", timeout: 60000 });
      ok = true;
    } catch {
      try {
        await page.goto(urlp2, { waitUntil: "domcontentloaded", timeout: 60000 });
        ok = true;
      } catch {}
    }
    if (!ok) {
      console.log(`Страница ${p}: не открылась, пропускаю`);
      continue;
    }

    await sleep(900);
    await autoScroll(page);

    const foundLinks = await page.evaluate(() => {
      const as = Array.from(document.querySelectorAll("a"));
      return Array.from(new Set(
        as.map(a => a.href)
          .filter(Boolean)
          .filter(u => (/\/(firm|place|entity)\//.test(u)) && u.includes("2gis."))
      ));
    });

    let addedHere = 0;
    for (const link of foundLinks) {
      const id = extractId(link);
      if (!id) continue;
      if (!seenIds.has(id)) {
        seenIds.add(id);
        if (!urlById.has(id)) urlById.set(id, normalizeUrl(link)); // первый нормализованный URL
        addedHere++;
      }
    }
    console.log(`Страница ${p}/${LAST_PAGE}: найдено ссылок=${foundLinks.length}, новых ID=${addedHere}`);
  }

  const cardLinks = Array.from(urlById.values());
  console.log(`Найдено уникальных карточек по ID: ${cardLinks.length}`);

  // --- Обход карточек ---
  let processed = 0;
  for (const url of cardLinks) {
    if (processed >= limit) break;
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
      await sleep(1000);

      // Иногда контакты скрыты за кнопкой «показать телефон»
      try {
        await page.$$eval("button", btns => {
          const b = btns.find(x => /показ(ать|ыть).+телефон/i.test(x.innerText));
          if (b) b.click();
        });
        await sleep(700);
      } catch {}

      const data = await page.evaluate(() => {
        const text = s => (s ? s.textContent.trim() : "");
        const name =
          text(document.querySelector("h1")) ||
          text(document.querySelector('[data-testid="header"]'));
        const address =
          text(document.querySelector('[data-testid="address"]')) ||
          text(document.querySelector('[itemprop="streetAddress"]')) ||
          "";

        const phones = Array.from(
          document.querySelectorAll('a[href^="tel:"]')
        )
          .map(a => a.getAttribute("href") || "")
          .map(h => h.replace(/^tel:/, ""))
          .filter(Boolean);

        const links = Array.from(document.querySelectorAll('a[href^="http"]'))
          .map(a => a.href);

        const website =
          links.find(u =>
            !/2gis\./i.test(u) &&
            !/yandex\./i.test(u) &&
            !/vk\.com\/share/i.test(u) &&
            !/wa\.me/i.test(u) &&
            !/t\.me/i.test(u)
          ) || "";

        const telegram = links.filter(u => /t\.me\//i.test(u));
        const email = Array.from(document.querySelectorAll('a[href^="mailto:"]'))
          .map(a => (a.getAttribute("href") || "").replace(/^mailto:/, ""))
          .filter(Boolean);

        return { name, address, phones, website, email, telegram };
      });

      // Если есть сайт — попробуем добрать e-mail/тг со страницы
      if (data.website) {
        try {
          const p2 = await browser.newPage();
          await p2.setUserAgent(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
          );
          await p2.goto(data.website, { waitUntil: "domcontentloaded", timeout: 20000 });
          await sleep(800);

          const extra = await p2.evaluate(() => {
            const hrefs = Array.from(document.querySelectorAll("a")).map(a => a.href);
            const emails = hrefs
              .filter(h => /^mailto:/i.test(h))
              .map(h => h.replace(/^mailto:/i, ""))
              .filter(Boolean);
            const tgs = hrefs.filter(h => /t\.me\//i.test(h));
            return { emails, tgs };
          });

          // Уникальная склейка
          const uniq = arr => Array.from(new Set((arr || []).filter(Boolean)));
          data.email = uniq([...(data.email || []), ...(extra.emails || [])]);
          data.telegram = uniq([...(data.telegram || []), ...(extra.tgs || [])]);

          await p2.close();
        } catch {}
      }

      out.write(toCsvRow({ ...data, url }));
      processed++;
      process.stdout.write(`\rСобрано: ${processed}/${Math.min(limit, cardLinks.length)}`);
    } catch (e) {
      console.error("\nОшибка на", url, e.message);
    }
  }

  console.log("\nГотово ->", path.join(process.cwd(), `out_${city}.csv`));
  await browser.close();
  out.end();
}

run().catch(e => {
  console.error(e);
  process.exit(1);
});