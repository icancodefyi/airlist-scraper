// scraper.js
import dotenv from "dotenv";
dotenv.config();

import axios from "axios";
import { MongoClient } from "mongodb";
import pLimit from "p-limit";

/**
 * Minimal deps required:
 * npm i axios mongodb p-limit dotenv
 *
 * Usage: node scraper.js
 */

/* ---------------- ENV ---------------- */
const {
  MONGO_URL,
  DB_NAME = "toppersjournal",
  COLLECTION = "toppers",
  GROQ_API_KEY,
  GROQ_API_URL,
  SERPER_API_KEY,
  CONCURRENCY = "1",
  TEST_LIMIT = "10",
  MODEL_NAME = "llama3-8b"
} = process.env;

if (!MONGO_URL) {
  console.error("❌ MONGO_URL missing in .env");
  process.exit(1);
}
if (!GROQ_API_KEY || !GROQ_API_URL) {
  console.error("❌ GROQ_API_KEY or GROQ_API_URL missing in .env");
  process.exit(1);
}
if (!SERPER_API_KEY) {
  console.error("❌ SERPER_API_KEY missing in .env");
  process.exit(1);
}

const client = new MongoClient(MONGO_URL, { maxPoolSize: 10 });
const limit = pLimit(parseInt(CONCURRENCY, 10) || 1);

/* ---------------- Helpers ---------------- */
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function fullName(doc = {}) {
  return `${doc.firstname || ""} ${doc.lastname || ""}`.trim();
}

function safeJSON(text) {
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch (err) {
    // try extracting first {...}
    const m = text.match(/{[\s\S]*}/);
    if (m) {
      try {
        return JSON.parse(m[0]);
      } catch {}
    }
    return null;
  }
}

/* ---------------- Serper Search ----------------
   We'll call serper.dev google endpoint that returns organic results
*/
async function serperSearch(query, maxResults = 8) {
  try {
    const res = await axios.post(
      "https://google.serper.dev/search",
      { q: query, num: maxResults, autocorrect: true },
      {
        headers: {
          "X-API-KEY": SERPER_API_KEY,
          "Content-Type": "application/json",
        },
        timeout: 20000,
      }
    );

    const organic = res.data?.organic || [];
    return organic.map((r) => ({
      source: "serper",
      title: r.title || "",
      snippet: (r.snippet || "").replace(/\s+/g, " ").trim(),
      url: r.link || r.link || "",
    }));
  } catch (err) {
    // show useful error
    const errMsg =
      err?.response?.data?.error || err?.response?.data || err?.message || String(err);
    console.log("Serper error:", errMsg);
    return [];
  }
}

/* ---------------- Build research ---------------- */
function buildQueries(doc) {
  const name = fullName(doc);
  const year = doc.year || "";
  return [
    `${name} UPSC topper interview ${year}`,
    `${name} UPSC preparation strategy`,
    `${name} AIR ${doc.rank} UPSC`,
    `${name} UPSC journey ${year}`,
    `${name} topper talk ${year}`,
  ];
}

function combineResearch(doc, snippets = []) {
  const header = `STRUCTURED DATA: ${JSON.stringify({
    firstname: doc.firstname || "",
    lastname: doc.lastname || "",
    fullname: fullName(doc),
    rank: doc.rank || null,
    year: doc.year || null,
    optional: doc.optionalSub || doc.optional || null,
    slug: doc.slug || null,
  })}\n\n`;

  const body = snippets
    .slice(0, 24)
    .map(
      (s, i) =>
        `${i + 1}. ${s.source.toUpperCase()} | ${s.title}\n${s.snippet}\n${s.url}`
    )
    .join("\n\n");

  return header + body;
}

/* ---------------- Groq LLM call ---------------- */
async function callGroq(prompt) {
  // We post to GROQ_API_URL (from env). This adapter expects openai-like chat completions
  try {
    const payload = {
      model: "llama-3.1-8b-instant",
      messages: [
        { role: "system", content: "You are a UPSC content generator." },
        { role: "user", content: prompt },
      ],
      max_tokens: 900,
      temperature: 0.4,
    };

    const res = await axios.post(GROQ_API_URL, payload, {
      headers: {
        Authorization: `Bearer ${GROQ_API_KEY}`,
        "Content-Type": "application/json",
      },
      timeout: 120000,
    });

    // multiple providers have slightly different shapes; try common paths
    const choices = res.data?.choices || res.data?.response?.choices || null;
    if (choices && choices.length > 0) {
      const msg = choices[0].message?.content ?? choices[0].text ?? null;
      return msg;
    }

    // fallback: try data.text
    if (res.data?.text) return res.data.text;

    // if nothing found, return entire body as string
    return JSON.stringify(res.data);
  } catch (err) {
    // bubble the full response for debugging
    let msg = err?.response?.data || err?.message || String(err);
    // try to stringify if object
    if (typeof msg === "object") msg = JSON.stringify(msg);
    throw new Error(`Groq failed: ${msg}`);
  }
}

/* ---------------- Build prompt ---------------- */
function buildPrompt(doc, research) {
  return `
You MUST follow all rules EXACTLY.

---

## RULES

1. Think step-by-step **internally**, but output ONLY valid JSON.
2. DO NOT write anything outside JSON. No preface, no explanation.
3. Inside JSON:
   - You may use simple Markdown (bold, italics, lists) BUT no headings.
4. Use ONLY the research provided. Do NOT invent facts.
5. Tone must be human, warm, and natural—not robotic.

---

## CONTENT REQUIREMENTS

### 1) "about"
- 40–60 words
- Very short, factual, crisp  
- Summarize who the topper is and their approach  
- No exaggeration, no generic sentences

### 2) "strategy"
- 450–650 words (long and SEO-rich)
- Multi-paragraph  
- High-detail explanation based ONLY on research  
- Include:
  - prelims approach  
  - mains strategy  
  - interview preparation  
  - resources used  
  - revision cycles  
  - note-making style  
  - optional subject approach  
  - test series methods  
- Should feel human-written, not AI-generated  
- Should be tailored to THIS topper (no generic IAS advice)

### 3) "insights"
- 5–8 bullet points  
- Short, specific, factual takeaways  

---

## RESEARCH

${research}

---

## FINAL OUTPUT FORMAT (MANDATORY)

Return ONLY this JSON object. NOTHING else.

{
  "about": "...",
  "strategy": "...",
  "insights": ["...", "..."]
}
`;
}




/* ---------------- Process single topper ---------------- */
async function processTopper(doc, coll) {
  console.log(`\n➡ Processing: ${fullName(doc)} (AIR ${doc.rank || "?"})`);

  // Build search queries
  const queries = buildQueries(doc);

  // Collect results from serper for each query with slight delay to be polite
  let combinedResults = [];
  for (const q of queries) {
    await sleep(250 + Math.random() * 250); // tiny jitter
    const res = await serperSearch(q, 8);
    combinedResults = combinedResults.concat(res);
  }

  // dedupe by url
  const seen = new Set();
  const deduped = [];
  for (const r of combinedResults) {
    const key = (r.url || r.title || r.snippet).slice(0, 200);
    if (!seen.has(key)) {
      seen.add(key);
      deduped.push(r);
    }
    if (deduped.length >= 20) break;
  }

  const researchText = combineResearch(doc, deduped);
  const prompt = buildPrompt(doc, researchText);

  // Call Groq LLM
  let raw;
  try {
    raw = await callGroq(prompt);
  } catch (err) {
    console.error("Groq failed:", err.message || err);
    // If Groq fails, mark document with error and move on
    await coll.updateOne(
      { _id: doc._id },
      {
        $set: {
          enriched: false,
          enrichedError: err.message ? String(err.message) : "Groq error",
          lastTriedAt: new Date(),
        },
      }
    );
    return;
  }

  // Try to parse JSON
  const parsed = safeJSON(raw);
  if (!parsed) {
    console.log("❌ LLM returned invalid JSON. Raw output saved to `enrichedRaw`");
    await coll.updateOne(
      { _id: doc._id },
      {
        $set: {
          enriched: false,
          enrichedRaw: raw,
          enrichedError: "invalid_json",
          lastTriedAt: new Date(),
        },
      }
    );
    return;
  }

  // Validate fields
  const bio = parsed.bio ? String(parsed.bio).trim() : "";
  const strategy = parsed.strategy ? String(parsed.strategy).trim() : "";
  const insights = Array.isArray(parsed.insights)
    ? parsed.insights.map((s) => String(s).trim()).filter(Boolean).slice(0, 6)
    : [];

  // Update Mongo
  await coll.updateOne(
    { _id: doc._id },
    {
      $set: {
        bio,
        strategy,
        insights,
        enriched: true,
        enrichedAt: new Date(),
        enrichedRaw: raw,
      },
      $unset: {
        enrichedError: "",
      },
    }
  );

  console.log(`✔ Updated: ${fullName(doc)}`);
}

/* ---------------- Main ---------------- */
async function main() {
  console.log("Connecting to MongoDB...");
  await client.connect();
  const db = client.db(DB_NAME);
  const coll = db.collection(COLLECTION);

  // Find toppers not enriched
  const cursor = coll
    .find({
      $or: [{ enriched: false }, { enriched: { $exists: false } }],
    })
    .limit(parseInt(TEST_LIMIT, 10) || 10);

  const docs = await cursor.toArray();
  console.log(`\nFound ${docs.length} toppers to process\n`);

  if (!docs.length) {
    console.log("No toppers found matching the query. Exiting.");
    await client.close();
    return;
  }

  // process sequentially but with concurrency limit wrapper
  const tasks = docs.map((doc) => {
    return limit(() => processTopper(doc, coll));
  });

  await Promise.all(tasks);

  console.log("\n🎉 DONE — All toppers processed.\n");
  await client.close();
}

main().catch((e) => {
  console.error("FATAL ERROR:", e);
  process.exit(1);
});
