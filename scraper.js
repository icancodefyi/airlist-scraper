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
  MODEL_NAME = "llama3-8b",
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

  // First try direct parse
  try {
    return JSON.parse(text);
  } catch (err) {
    // Try extracting JSON block
    const m = text.match(/{[\s\S]*}/);
    if (m) {
      let extracted = m[0];

      // Try parsing as-is first
      try {
        return JSON.parse(extracted);
      } catch {
        // If that fails, fix common JSON issues:
        // 1. Replace literal newlines inside strings with \n
        // 2. Replace unescaped quotes
        // 3. Remove control characters

        try {
          // Fix by properly escaping content within string values
          extracted = extracted.replace(
            /"(bio|strategy|insights)":\s*"([^"]*(?:"[^"]*)*?)"/gs,
            (match, key, value) => {
              // Escape control characters and newlines in the value
              const cleaned = value
                .replace(/\\/g, "\\\\") // Escape backslashes first
                .replace(/\n/g, "\\n") // Escape newlines
                .replace(/\r/g, "\\r") // Escape carriage returns
                .replace(/\t/g, "\\t") // Escape tabs
                .replace(/"/g, '\\"'); // Escape quotes
              return `"${key}":"${cleaned}"`;
            }
          );

          return JSON.parse(extracted);
        } catch (finalErr) {
          console.log("JSON parse error:", finalErr.message);
          console.log("First 200 chars:", extracted.substring(0, 200));
          return null;
        }
      }
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
      err?.response?.data?.error ||
      err?.response?.data ||
      err?.message ||
      String(err);
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
        `${i + 1}. ${s.source.toUpperCase()} | ${s.title}\n${s.snippet}\n${
          s.url
        }`
    )
    .join("\n\n");

  return header + body;
}

/* ---------------- Groq LLM call ---------------- */
/* ---------------- Groq LLM call with auto rate-limit retry ---------------- */
async function callGroq(prompt, attempt = 1) {
  const MAX_RETRIES = 3;

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

    const choices = res.data?.choices || res.data?.response?.choices;
    if (choices?.length) {
      return choices[0].message?.content ?? choices[0].text;
    }

    return JSON.stringify(res.data);

  } catch (err) {
    const data = err?.response?.data;

    // -------- Detect Groq rate limit --------
    const message = data?.error?.message || "";

    if (message.includes("Rate limit reached")) {
      // Extract the number of seconds
      const match = message.match(/try again in ([0-9.]+)s/i);
      const waitSeconds = match ? parseFloat(match[1]) : 6;

      console.log(
        `⏳ Groq rate limit hit. Waiting ${waitSeconds}s before retrying (Attempt ${attempt}/${MAX_RETRIES})`
      );

      await new Promise((res) => setTimeout(res, waitSeconds * 1000));

      if (attempt < MAX_RETRIES) {
        return await callGroq(prompt, attempt + 1);
      }

      throw new Error("Groq rate limit exceeded even after retrying.");
    }

    // -------- Other errors --------
    let msg = data || err.message || String(err);
    if (typeof msg === "object") msg = JSON.stringify(msg);
    throw new Error(`Groq failed: ${msg}`);
  }
}


/* ---------------- Build prompt ---------------- */
function buildPrompt(doc, research) {
  return `
You MUST return ONLY valid, properly formatted JSON. NO markdown formatting outside the JSON.
All newline characters INSIDE the JSON strings must be written as the literal characters backslash-n (\\n), NOT actual line breaks.
All quotes inside text must be escaped as \\".
All backslashes must be escaped as \\\\.

Use ONLY the research + structured data provided.

${research}

Write 3 fields:

1) "bio"
   - 50–70 words
   - simple, factual, research-based
   - single paragraph

2) "strategy"
   - 1200–1800 words (VERY LONG - 2-3x normal length)
   - Written in HUMAN CONVERSATIONAL TONE - like a friend telling their story
   - Use STORYTELLING format with narrative flow
   - Divide into clear sections with ### headers
   - Include personal anecdotes and experiences
   - Use **bold** for key concepts and turning points
   - Use \\n\\n for paragraph breaks (literal backslash-n backslash-n, not real newlines)
   - Use - for key points with \\n between them
   - Make it engaging, personal, and detailed
   - Include: Background → Study Plan → Challenges → Strategies → Interview Experience → Success
   - Sound natural and conversational, not robotic

3) "insights"
   - 6–10 short bullet-style sentences
   - each 8–15 words
   - actionable and practical

Return EXACTLY this JSON structure, ensuring all newlines inside strings are represented as \\n:

{
  "bio": "text...",
  "strategy": "long story-format text with markdown...",
  "insights": ["point1", "point2", "point3"]
}

DO NOT add any extra fields.
DO NOT add any text outside the JSON.
DO NOT add commentary.
Return ONLY the raw JSON object. Nothing else.
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
  console.log("Raw LLM response (first 500 chars):", raw.substring(0, 500));
  const parsed = safeJSON(raw);
  if (!parsed) {
    console.log(
      "❌ LLM returned invalid JSON. Raw output saved to `enrichedRaw`"
    );
    console.log("Full raw output:", raw);
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

  // Map "bio" from LLM (already has correct field name)
  const bio = parsed.bio ? String(parsed.bio).trim() : "";
  const strategy = parsed.strategy ? String(parsed.strategy).trim() : "";
  const insights = Array.isArray(parsed.insights)
    ? parsed.insights
        .map((s) => String(s).trim())
        .filter(Boolean)
        .slice(0, 10)
    : [];

  // Validate required fields
  if (!bio || bio.length < 20) {
    console.log(`⚠️ WARNING: bio is empty or too short for ${fullName(doc)}`);
    console.log(`  bio length: ${bio.length}`);
    await coll.updateOne(
      { _id: doc._id },
      {
        $set: {
          enriched: false,
          enrichedError: "bio_too_short",
          enrichedRaw: raw,
          lastTriedAt: new Date(),
        },
      }
    );
    return;
  }

  if (!strategy || strategy.length < 150) {
    console.log(
      `⚠️ WARNING: strategy is empty or too short for ${fullName(doc)}`
    );
    console.log(`  strategy length: ${strategy.length}`);
    await coll.updateOne(
      { _id: doc._id },
      {
        $set: {
          enriched: false,
          enrichedError: "strategy_too_short",
          enrichedRaw: raw,
          lastTriedAt: new Date(),
        },
      }
    );
    return;
  }

  if (!insights || insights.length < 3) {
    console.log(`⚠️ WARNING: insights too few for ${fullName(doc)}`);
    console.log(`  insights count: ${insights.length}`);
    await coll.updateOne(
      { _id: doc._id },
      {
        $set: {
          enriched: false,
          enrichedError: "insufficient_insights",
          enrichedRaw: raw,
          lastTriedAt: new Date(),
        },
      }
    );
    return;
  }

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
  console.log(`  - bio: ${bio.length} chars`);
  console.log(`  - strategy: ${strategy.length} chars`);
  console.log(`  - insights: ${insights.length} points`);
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
