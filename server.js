const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

// =============================
// CONFIG
// =============================
const MAX_SUBJECT_RETRIES = 3;
const PROCESS_INTERVAL_MS = 750;
const CONCURRENCY = 8;

const HUBSPOT_TOKEN = process.env.HUBSPOT_TOKEN;
const ANTHROPIC_API_KEY = process.env.ANTHROPIC_API_KEY;

// =============================
// PERSISTENT DOMAIN CACHE
// =============================
const CACHE_FILE = path.join('/tmp', 'domain_cache.json');
const COMPLETED_LOG = path.join('/tmp', 'completed.log');
const ERROR_LOG = path.join('/tmp', 'errors.log');
const CACHE_TTL_MS = 7 * 24 * 60 * 60 * 1000;

let domainCache = new Map();

function loadCacheFromDisk() {
  try {
    if (fs.existsSync(CACHE_FILE)) {
      const raw = fs.readFileSync(CACHE_FILE, 'utf8');
      const obj = JSON.parse(raw);
      domainCache = new Map(Object.entries(obj));
      let evicted = 0;
      for (const [key, val] of domainCache.entries()) {
        if (Date.now() - val.cachedAt > CACHE_TTL_MS) {
          domainCache.delete(key);
          evicted++;
        }
      }
      console.log(`💾 Domain cache loaded: ${domainCache.size} domains (${evicted} expired entries evicted)`);
    } else {
      console.log('💾 No domain cache file found — starting fresh');
    }
  } catch (err) {
    console.error('⚠️ Could not load domain cache:', err.message);
    domainCache = new Map();
  }
}

function saveCacheToDisk() {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(Object.fromEntries(domainCache)), 'utf8');
  } catch (err) {
    console.error('⚠️ Could not save domain cache:', err.message);
  }
}

function loadCompletedIds() {
  const ids = new Set();
  try {
    if (fs.existsSync(COMPLETED_LOG)) {
      const lines = fs.readFileSync(COMPLETED_LOG, 'utf8').trim().split('\n');
      for (const line of lines) {
        const [contactId] = line.split(',');
        if (contactId) ids.add(contactId.trim());
      }
      console.log(`📋 Completed log loaded: ${ids.size} previously processed contacts`);
    }
  } catch (err) {
    console.error('⚠️ Could not load completed log:', err.message);
  }
  return ids;
}

function logCompleted(contactId, step) {
  try {
    fs.appendFileSync(COMPLETED_LOG, `${contactId},${step},${new Date().toISOString()}\n`);
  } catch (err) {
    console.error('⚠️ Could not write to completed log:', err.message);
  }
}

function logError(contactId, step, message) {
  try {
    const line = `${contactId},${step},${message.replace(/,/g, ';')},${new Date().toISOString()}\n`;
    fs.appendFileSync(ERROR_LOG, line);
  } catch (err) {
    console.error('⚠️ Could not write to error log:', err.message);
  }
}

loadCacheFromDisk();
const completedIds = loadCompletedIds();

// =============================
// BLOCKED DOMAINS
// =============================
const BLOCKED_DOMAINS = new Set([
  'bankofamerica.com', 'wellsfargo.com', 'citigroup.com', 'citi.com',
  'chase.com', 'jpmorgan.com', 'goldmansachs.com', 'morganstanley.com',
  'herbalife.com', 'securityfinance.com', 'braze.com', 'salesforce.com',
  'microsoft.com', 'google.com', 'amazon.com', 'apple.com', 'meta.com',
  'linkedin.com', 'twitter.com', 'facebook.com', 'instagram.com'
]);

// =============================
// AEO RESOURCE LINK HELPERS
// Rotated across AEO-active steps:
//   aeoGuideLink()        -> steps 1, 2, 3  (concept introduction phase)
//   contentAnalyzerLink() -> steps 5, 7, 9  (solution / diagnostic phase)
// =============================
const LINK_STYLE = 'font-weight:bold;text-decoration:underline;color:#A2CF23;';
const AEO_GUIDE_HREF = 'https://www.pedowitzgroup.com/the-complete-guide-to-answer-engine-optimization-aeo';
const CONTENT_ANALYZER_HREF = 'https://www.pedowitzgroup.com/content-analyzer';

function aeoGuideLink(anchor) {
  return `<a href="${AEO_GUIDE_HREF}" style="${LINK_STYLE}">${anchor}</a>`;
}
function contentAnalyzerLink(anchor) {
  return `<a href="${CONTENT_ANALYZER_HREF}" style="${LINK_STYLE}">${anchor}</a>`;
}

// =============================
// TPG 10-STEP SEQUENCE MAP
// =============================
// IMPROVEMENTS APPLIED:
//   - Steps 1-4: talkingPoints rewritten to problem-first framing
//   - Step 1: ctaType changed from 'meeting' to 'reply' (cold open; earn the ask)
//   - Steps 1-2: word count tightened to 60-85 (forces sharper writing)
//   - Steps 3,5,7,9: aeoContext added (buyer visibility / AEO angle)
//   - Steps 1,2: aeoContext added (opening hook and pain framing)
//   - All other steps: aeoContext = null (suppresses AEO in prompt)
//   - avoidPhrases added per step (enforced in prompt, prevents clichés)
//   - AEO resource links rotated in:
//       Steps 1, 2, 3 -> AEO Guide (introducing the concept)
//       Steps 5, 7, 9 -> Content Analyzer (solution/diagnostic phase)
// =============================
const SEQUENCE_MAP = [
  {
    step: 1,
    pillar: "AI SYSTEMS & AUTOMATION",
    service: "AI Agents and Automation",
    serviceUrl: "https://www.pedowitzgroup.com/ai-agents-and-automation",
    offer: "AI Agent Discovery Tool",
    offerUrl: "https://www.pedowitzgroup.com/agentic-ai",
    angle: "pipeline_predictability",
    talkingPoint: "Open with the buyer visibility gap (see AEO context below) — that's the hook. Then pivot: the reason pipeline feels unpredictable right now isn't just bad leads or misaligned teams. It's that a growing share of the buying process is happening in a channel most marketing teams have never had to compete in. Your sales team spending 40% of their time on non-selling work is a symptom of a system that was built for a different buyer journey. The teams pulling ahead have fixed both: they're visible where buyers research, and they've removed the overhead that slows revenue down.",
    ctaType: "reply",
    wordCount: "60 to 85",
    openingStyle: "observation",
    reEngagementNote: null,
    aeoContext: `AEO / BUYER VISIBILITY — OPENING HOOK (this is the first thing the prospect reads after the salutation):

THE EMAIL MUST BE STRUCTURED IN THIS EXACT ORDER:

1. OPEN with the buyer visibility hook — one to two sentences naming the gap directly. This is non-negotiable. Use one of these patterns, adapted for the prospect's industry and tone:
   - "Your buyers are getting their vendor shortlist from ChatGPT right now — and most [industry] companies have no idea whether they're showing up in those answers."
   - "A growing share of your market is asking an AI tool to compare [category] options before they ever hit a website."
   - "The companies winning right now aren't just ranking on Google — they're showing up when a buyer asks an AI tool to recommend vendors in their space."

2. IF company blog or news signals are available — use them to personalize the hook in one sentence immediately after it. The pattern is: "I noticed [company]'s [blog/content] is talking about [specific topic]. That's exactly the kind of positioning that should be surfacing when a buyer asks an AI tool to compare options in your space — but only if the content is structured for it." The company signal personalizes the hook. It does NOT replace the hook.

3. PIVOT to the pipeline consequence in one sentence: the pipeline unpredictability they feel and the buyer visibility gap are the same problem. Their sales team spending time on non-selling work is a symptom of a buying process that has moved to a channel they have never had to compete in.

4. CLOSE with the reply CTA question as instructed in the CALL TO ACTION section.

CRITICAL: If company blog/news signals are available, do NOT open with "I saw/I noticed" as the very first line. The visibility hook comes first. The "I noticed" line is the personalization bridge between the hook and the pipeline pivot.

Do NOT use the terms "AEO", "AXO", or "answer engine optimization". Plain language only. Name the problem vividly. Do not pitch a solution.

RESOURCE LINK — OPTIONAL: If a value-add fits naturally before the reply question, include one hyperlink to the AEO guide as a plain reading resource — no pitch attached. Anchor text should be a short natural phrase like "worth a read" or "quick take on it":
${aeoGuideLink('[short phrase]')}
Only include if it fits naturally. Do not force it.`,
    avoidPhrases: ["execution to strategy", "unlock", "revenue engine", "scale your", "transform your", "holistic", "leverage", "robust", "cutting-edge", "impactful", "synergy", "utilize"]
  },
  {
    step: 2,
    pillar: "AI STRATEGY & INNOVATION",
    service: "AI Roadmap Accelerator",
    serviceUrl: "https://www.pedowitzgroup.com/solutions/ai",
    offer: "AI Readiness Assessment",
    offerUrl: "https://www.pedowitzgroup.com/ai-assessment",
    angle: "martech_roi",
    talkingPoint: "Most companies are running 15 to 20 MarTech tools and getting real value from maybe 8. The rest were bought with a business case that made sense at the time. Now they're just contract renewals nobody wants to own. The answer isn't more tools. It's a prioritized roadmap that starts with what you already have and connects every investment to a revenue outcome — not a 90-day experiment that fades when the champion leaves.",
    ctaType: "meeting",
    wordCount: "60 to 85",
    openingStyle: "observation",
    reEngagementNote: null,
    aeoContext: `AEO / BUYER VISIBILITY — PAIN FRAMING (one to two sentences, woven naturally into the body — not the opening, not a standalone paragraph):
The roadmap problem has a new wrinkle most AI investment plans don't account for: buyer discovery is shifting to AI-generated answers. ChatGPT, Perplexity, and Google's AI Overview are now summarizing vendor categories for buyers before they search, visit a site, or fill out a form. Most AI roadmaps being built today are optimizing for channels buyers are already leaving.
Weave this as a sharpening observation inside the body — it makes the "bad roadmap" pain more specific and more urgent without changing the email's thesis. Do NOT use the terms "AEO", "AXO", or "answer engine optimization". Plain language only. Do not develop this into a separate pitch — one observation, then move on.

RESOURCE LINK — INCLUDE ONCE: After the buyer visibility observation, add a single hyperlink to the AEO guide as a value-add. Frame it as something worth reading, not a pitch. Short anchor phrase — "here's a quick read on it", "we put together a guide", or similar:
${aeoGuideLink('[short phrase]')}
Then move directly to the meeting CTA.`,
    avoidPhrases: ["execution to strategy", "unlock", "transform", "holistic", "leverage", "robust", "cutting-edge", "impactful", "synergy", "utilize", "journey", "revenue engine"]
  },
  {
    step: 3,
    pillar: "AI INTELLIGENCE & PERSONALIZATION",
    service: "AI-Driven Personalization",
    serviceUrl: "https://www.pedowitzgroup.com/ai-driven-personalization",
    offer: "Revenue Marketing Maturity Assessment",
    offerUrl: "https://www.pedowitzgroup.com/revenue-marketing-maturity-assessment",
    angle: "lead_quality",
    talkingPoint: "Your best prospects are doing their research right now and your marketing team has no idea. They're reading your competitors' case studies, comparing positioning, and forming opinions — and they will never fill out a form. By the time they talk to sales, 70 to 80% of the decision is already made. The companies winning that invisible research phase are the ones whose content shows up with the right answer at the right moment.",
    ctaType: "content",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: null,
    aeoContext: `AEO / BUYER VISIBILITY — PRIMARY (this is the email where the concept gets fully named and developed):
By now the prospect has seen a reference to AI-generated buyer research in emails 1 and 2. This email deepens it into a full argument: personalization only works if you can reach the buyer — but a growing share of buyers are forming opinions in AI-generated answers before they ever click a result, visit a site, or raise their hand. If your brand, your point of view, and your content aren't showing up in those answers, you're invisible for the part of the journey that matters most.

This is the primary AEO email. The concept should be developed across two to three sentences, positioned as the explanation for why "invisible buyers" is getting worse, not better. Lead with the phenomenon, connect it to the personalization gap, and let the content offer feel like the logical next step for a team that wants to understand where they stand.

Do NOT use the terms "AEO", "AXO", or "answer engine optimization". Describe the mechanism in plain language — what buyers are doing, what marketing teams are missing, and what it costs them.

RESOURCE LINK — MANDATORY FOR THIS STEP: This is the primary AEO email and the AEO Guide is the ideal content offer here. Include the AEO Guide link alongside or instead of the Revenue Marketing Maturity Assessment — whichever fits more naturally. Present it as the resource that helps them understand the buyer visibility gap you just described:
${aeoGuideLink('[short phrase]')}
Make it feel like the logical next step for a reader who just got the concept for the first time.`,
    avoidPhrases: ["70 to 80 percent", "meet them in the moment", "right message right channel", "unlock", "transform", "leverage", "journey", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize"]
  },
  {
    step: 4,
    pillar: "AI SYSTEMS & AUTOMATION",
    service: "Marketing Operations Automation",
    serviceUrl: "https://www.pedowitzgroup.com/marketing-operations-automation",
    offer: "AI Project Prioritization Tool",
    offerUrl: "https://www.pedowitzgroup.com/tpg-ai-project-prioritization",
    angle: "team_capacity",
    talkingPoint: "This email is about the hidden labor cost inside marketing ops — not pipeline signals (covered in email 1), but the manual work that happens before any campaign touches a prospect: lead routing, data hygiene, segmentation, campaign QA. Most marketing ops teams are running at 120% capacity doing work that should not require a human. The question is how much of that overhead is actually scheduled to go away — and what your team could do with that time instead.",
    ctaType: "meeting",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: null,
    aeoContext: null,
    avoidPhrases: ["execution to strategy", "from cost center to", "unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "revenue engine", "scale your", "journey"]
  },
  {
    step: 5,
    pillar: "STRATEGY",
    service: "Revenue Marketing Transformation",
    serviceUrl: "https://www.pedowitzgroup.com/solutions/revenue-marketing-transformation",
    offer: "Revenue Marketing Index 2025",
    offerUrl: "https://www.pedowitzgroup.com/revenue-marketing-index-start",
    angle: "pipeline_predictability",
    talkingPoint: "TPG's RM6 framework refined across 14 years and 1,300 plus clients is the blueprint for turning marketing from a cost center into a predictable scalable revenue engine connecting strategy, technology, creative, and operations into one accountable motion.",
    ctaType: "content",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: "This is email 5 of 10. Acknowledge in one confident sentence — not apologetically — that you have sent a few notes and have not heard back. Then move on without dwelling on it. Do not apologize. Do not beg.",
    aeoContext: `AEO / BUYER VISIBILITY — COMPETITIVE ESCALATION (one sentence, placed as a sharp aside in the body — not the opening, not a paragraph):
The Revenue Marketing leaders pulling ahead right now aren't just connecting strategy, technology, and operations — they're also making sure their brand shows up when a buyer asks an AI tool to recommend vendors in their space. The companies building RM6 maturity are, almost without exception, the same ones whose content surfaces in AI-generated answers. One sentence. Make the prospect feel like their competitors are already there. Do not develop further.
Do NOT use the terms "AEO", "AXO", or "answer engine optimization". Plain language only.

RESOURCE LINK — OPTIONAL: After the competitive escalation sentence, add the Content Analyzer as a light secondary mention — frame it as a quick way to see where their content stands right now:
${contentAnalyzerLink('[short phrase]')}
Only include if it fits without slowing the email down. The primary CTA is the Revenue Marketing Index.`,
    avoidPhrases: ["unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "journey", "revenue engine", "cost center"]
  },
  {
    step: 6,
    pillar: "OPERATIONS",
    service: "Revenue Operations",
    serviceUrl: "https://www.pedowitzgroup.com/solutions/revenue-operations",
    offer: "Revenue Marketing Maturity Assessment",
    offerUrl: "https://www.pedowitzgroup.com/revenue-marketing-maturity-assessment-survey",
    angle: "attribution",
    talkingPoint: "When marketing, sales, and customer success operate off different data and different definitions attribution breaks down and the CFO questions your budget. RevOps alignment closes that gap and makes every dollar accountable.",
    ctaType: "reply",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: null,
    aeoContext: null,
    avoidPhrases: ["unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "journey", "AI search", "AEO", "AXO", "answer engine", "buyer visibility"]
  },
  {
    step: 7,
    pillar: "TECHNOLOGY CONSULTING",
    service: "HubSpot Services",
    serviceUrl: "https://www.pedowitzgroup.com/hubspot-main",
    offer: "HubSpot ROI Calculator",
    offerUrl: "https://www.pedowitzgroup.com/roi",
    angle: "martech_roi",
    talkingPoint: "Most organizations use fewer than 60% of their MarTech capabilities. Whether you're on HubSpot or evaluating a migration TPG's technology consulting practice gets your stack generating revenue instead of generating tickets.",
    ctaType: "content",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: null,
    aeoContext: `AEO / BUYER VISIBILITY — DIAGNOSTIC OFFER (this is the payoff for the arc started at step 1):
The prospect has now seen the buyer visibility problem framed as pain (emails 1 and 2), developed as thesis (email 3), and escalated competitively (email 5). This is the first email where the solution can be named.

HubSpot architecture, content structure, and CRM signals all feed into whether a brand surfaces in AI-generated answers. Most HubSpot configurations were never built with this in mind — they were built for search and form fills, not for the AI-summarized buyer journey.

CONDITIONAL OFFER:
- If intent signals are ACTIVE or the prospect has visited the TPG website 5 or more times: name the AXO Diagnostic explicitly. "TPG's AXO diagnostic scores how visible your brand is across ChatGPT, Perplexity, Claude, and Gemini — and what it would take to improve it." Link as a natural anchor: <a href="https://www.pedowitzgroup.com/axo" style="${LINK_STYLE}">AXO diagnostic</a>
- If no strong intent signals: describe the visibility gap and connect it to HubSpot configuration without naming AXO. Let the concept land; the diagnostic can surface at the meeting.

Do NOT use the term "AEO" or "answer engine optimization" in the email body. "AXO" is permitted only under the high-intent condition above.

RESOURCE LINK — INCLUDE ONCE: Whether or not you name the AXO diagnostic, include the Content Analyzer as a secondary resource — frame it as a quick tool to see how their current content would perform in AI-generated summaries:
${contentAnalyzerLink('[short phrase]')}
Place it naturally before or after the HubSpot ROI Calculator CTA. One sentence framing, then the link — not a standalone paragraph.`,
    avoidPhrases: ["unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "revenue engine", "journey"]
  },
  {
    step: 8,
    pillar: "AI INTELLIGENCE",
    service: "Data and Decision Intelligence",
    serviceUrl: "https://www.pedowitzgroup.com/data-and-decision-intelligence",
    offer: "Marketing Automation Migration ROI Calculator",
    offerUrl: "https://www.pedowitzgroup.com/marketing-automation-roi-calculator",
    angle: "attribution",
    talkingPoint: "Disconnected data is the silent killer of marketing ROI. When your CRM, MAP, and product analytics don't talk to each other your team makes decisions on incomplete signals and misses the revenue leaks hiding in plain sight.",
    ctaType: "reply",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: "This is email 8 of 10. In one sentence, name the silence directly and confidently — something like 'I have sent a few notes without hearing back' — then pivot immediately to a single sharp question that invites a reply. No apology. No guilt.",
    aeoContext: null,
    avoidPhrases: ["unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "journey", "AI search", "AEO", "AXO", "answer engine", "buyer visibility"]
  },
  {
    step: 9,
    pillar: "MANAGED SERVICES & CREATIVE",
    service: "Demand Generation",
    serviceUrl: "https://www.pedowitzgroup.com/hubspot-demand-generation",
    offer: "Content Analyzer Assessment",
    offerUrl: "https://www.pedowitzgroup.com/content-analyzer",
    angle: "content_performance",
    talkingPoint: "Demand gen only scales when content, campaigns, and technology run as one system. TPG's managed services teams run the full motion: strategy, content, email, SEO, and paid so your team focuses on revenue not maintenance.",
    ctaType: "meeting",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: null,
    aeoContext: `AEO / BUYER VISIBILITY — CHANNEL DISRUPTION (one sentence maximum, woven naturally into the body):
Demand gen is built on search and paid channels. But a growing share of buyers are asking AI tools to summarize the vendor landscape before they search anything — which means paid and organic investment is reaching buyers later in their decision, not earlier. One sentence that makes the reader feel like the channel model they're optimizing is already behind where buyers actually are.
Do NOT use the terms "AEO", "AXO", or "answer engine optimization". Plain language only. Do not develop further — this is a sharpening aside, not the thesis.

RESOURCE LINK — TIE TO PRIMARY OFFER: The Content Analyzer is already the featured offer for this step. Let the AEO channel disruption sentence flow directly into it as the natural payoff — the buyer visibility observation makes the content audit feel urgent. The CTA should feel like the answer to the disruption you just named:
${contentAnalyzerLink('[short phrase]')}`,
    avoidPhrases: ["unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "revenue engine", "journey"]
  },
  {
    step: 10,
    pillar: "REVENUE MARKETING TRANSFORMATION",
    service: "Revenue Marketing Breakthrough Zone",
    serviceUrl: "https://www.pedowitzgroup.com/revenue-marketing-ai-model-breakthrough-transformation",
    offer: "Revenue Marketing eGuide",
    offerUrl: "https://www.pedowitzgroup.com/revenue-marketing-eguide",
    angle: "pipeline_predictability",
    talkingPoint: "This is the direct close. Make a warm specific ask for 20 minutes and tell the prospect exactly what they will get from the conversation: a candid assessment of where their marketing motion has gaps and what it would take to fix them.",
    ctaType: "meeting",
    wordCount: "40 to 65",
    openingStyle: "question",
    reEngagementNote: "This is the final email — email 10 of 10. Name that directly. Tell them this is your last note. Make the ask warm and specific: 20 minutes, and tell them exactly what they will walk away with. No guilt, no pressure. Just a clean, confident close.",
    aeoContext: null,
    avoidPhrases: ["unlock", "transform", "leverage", "holistic", "robust", "cutting-edge", "impactful", "synergy", "utilize", "journey", "AI search", "AEO", "AXO", "answer engine", "buyer visibility"]
  }
];

// =============================
// CTA INSTRUCTIONS BY TYPE
// =============================
function buildCtaInstructions(ctaType, stepConfig) {
  // AEO resource links are specified inside aeoContext — flag here to avoid duplication
  const aeoResourceNote = stepConfig.aeoContext
    ? `\nNOTE: An AEO resource link (AEO Guide or Content Analyzer) is specified in the AEO CONTEXT section above. Do not duplicate it here if it already appears in the body.`
    : '';

  if (ctaType === 'meeting') {
    return `CALL TO ACTION — MEETING:
Ask for 20 minutes on the calendar. Tell the prospect exactly what they will get from that conversation. Make the ask feel specific and earned, not generic.
Calendar link (use a single word or short phrase as the anchor):
<a href="https://meetings.hubspot.com/scott-benedetti" style="${LINK_STYLE}">[word or phrase]</a>

Also include ONE single-word hyperlink to the featured service:
<a href="${stepConfig.serviceUrl}" style="${LINK_STYLE}">[word]</a>${aeoResourceNote}`;
  }

  if (ctaType === 'content') {
    return `CALL TO ACTION — CONTENT (NO MEETING ASK):
Do NOT ask for a meeting in this email. Instead, drive to the featured offer below as a pure value drop. One sentence framing why it is useful, then the link. No pitch attached to it.
Offer: ${stepConfig.offer}
Offer link (use a short natural phrase as the anchor):
<a href="${stepConfig.offerUrl}" style="${LINK_STYLE}">[short phrase]</a>

Also include ONE single-word hyperlink to the featured service:
<a href="${stepConfig.serviceUrl}" style="${LINK_STYLE}">[word]</a>${aeoResourceNote}`;
  }

  if (ctaType === 'reply') {
    return `CALL TO ACTION — REPLY REQUEST:
Do NOT ask for a meeting. Do NOT link to an offer. Instead, end the email with ONE single direct question that invites a one-sentence reply. The question should be specific to their industry or situation, not generic. Make it easy to answer. Examples: "Is attribution the biggest gap right now or is it something else?" or "What does your current RevOps setup look like?" Pick the question that fits this specific prospect.

Also include ONE single-word hyperlink to the featured service somewhere naturally in the body:
<a href="${stepConfig.serviceUrl}" style="${LINK_STYLE}">[word]</a>${aeoResourceNote}`;
  }

  return '';
}

// =============================
// INDUSTRY PERSONA LIBRARY
// =============================
const INDUSTRY_PERSONAS = {

  "TECHNOLOGY": {
    label: "Technology / SaaS",
    pains: [
      "Your pipeline numbers look fine until you dig in and realize half of it is deals your sales team already knows will never close. Marketing is generating volume but not the right kind of volume and the two teams are measuring completely different things.",
      "You're probably running 15 to 20 MarTech tools and fully utilizing maybe 8 of them. The rest were bought with a business case that made sense at the time but now they're just contract renewals nobody wants to own.",
      "AI is already inside your product. But your marketing team is still building campaigns in the same way they did three years ago. That gap is going to show up in competitive loss rates before it shows up in a board deck.",
      "Your best sales reps are spending 40% of their time on activity that has nothing to do with selling: updating the CRM, chasing down content, sitting in pipeline reviews where no one agrees on the numbers.",
      "The CFO gave you a budget cut and asked for better attribution. Those two things don't go together unless you change how marketing is measured, not just what it reports on."
    ],
    aiOpportunity: "Tech and SaaS companies have the most mature AI infrastructure but the biggest gap is between AI that runs the product and AI that runs the revenue engine. Marketing ops automation, AI-driven lead scoring, and journey orchestration are where the next efficiency and pipeline gains live.",
    stats: "45% of Technology companies have reached Revenue Marketing maturity, the highest of any industry, yet even in tech fewer than 25% use AI for forecasting, journey orchestration, or revenue analytics. Organizations implementing AI across all RM6 pillars see 25 to 40% improvement in revenue per employee.",
    toneNote: "Peer-to-peer. These buyers have seen every pitch. Skip the framework names. Lead with a specific observation about their company or competitive situation and get to the point in one sentence.",
    openingHooks: [
      "If they recently launched a product or entered a new market: acknowledge the launch and connect it to the pressure on the marketing team to generate qualified pipeline fast.",
      "If they have recent funding: tie the funding round to the accountability that comes with it and what that means for the CMO's ability to show pipeline contribution.",
      "If their blog or content shows a focus on a specific use case or persona: reflect that focus back and ask whether their demand gen motion is built around it or still running generic campaigns.",
      "If their tech stack suggests heavy MarTech investment: observe that the tools are in place but ask whether they're working as a system or as a collection of disconnected point solutions.",
      "If no specific news: reference the broader tech market pressure where budgets are flat but growth targets are not and connect that to what it means for their marketing team specifically."
    ]
  },

  "COMPUTER_SOFTWARE": {
    label: "Computer Software",
    pains: [
      "Enterprise software deals take 6 to 18 months to close and your marketing team is being measured on MQLs that have nothing to do with that reality. The metrics don't match the motion and sales knows it.",
      "Your product does something genuinely complex and the content you're producing to explain it is either too technical for the buyer or too shallow for the evaluator. That gap is costing you late-stage deals.",
      "You're spending real budget on demand gen but your highest-value prospects, the ones who are actually evaluating solutions, never fill out a form. They read everything and stay invisible until they're ready.",
      "Sales says marketing leads are bad. Marketing says sales doesn't follow up. Both are partially right but neither team has the data to prove it because attribution is broken at the handoff point.",
      "You're competing against well-funded point solutions that do one thing and market it brilliantly. Your platform does ten things better but your messaging tries to explain all ten and lands none of them."
    ],
    aiOpportunity: "Software companies have the most to gain from AI-driven buyer intent modeling and account-based personalization. When your ICP is doing 70% of their research before talking to sales, AI is what tells you who is in that research phase and what they care about before they raise their hand.",
    stats: "Only 22% of Technology and Software companies have reached full Revenue Marketing maturity. B2B buyers complete 70 to 80% of their journey before contacting sales, which means the companies winning are the ones creating personalized digital experiences for buyers who will never fill out a form.",
    toneNote: "Direct and a little skeptical. Software CMOs have heard every consulting pitch. Open with something specific about their company or product and resist the urge to explain TPG before earning the right to.",
    openingHooks: [
      "If they have a recent product release or G2 review momentum: reference the market traction and connect it to whether their pipeline motion is keeping pace.",
      "If their blog or content shows thought leadership in a specific category: note that the content is strong but ask whether it's generating pipeline or just awareness.",
      "If they appear to be in a competitive category: acknowledge the category pressure and what that means for how buyers evaluate them versus better-marketed alternatives.",
      "If their description mentions a platform or suite: acknowledge the breadth-versus-depth tension that platform companies always face in marketing.",
      "If no specific news: open with the observation that the best software companies build product roadmaps with obsessive precision but often treat marketing as an afterthought until growth stalls."
    ]
  },

  "INTERNET": {
    label: "Internet / Digital",
    pains: [
      "Your organic reach is down and paid acquisition costs are up. You're essentially paying more to reach the same audience you used to reach for free and the unit economics are quietly breaking.",
      "You have first-party data that most companies would pay to have but it's sitting in five different systems that don't talk to each other. The personalization you could be running is years ahead of what you're actually running.",
      "Every channel has its own attribution story. Google says it drove the conversion. Meta says it drove the conversion. Your CRM says something different. Nobody trusts the numbers and budget planning becomes a political exercise.",
      "You're A/B testing subject lines and button colors while your competitors are using AI to personalize the entire buyer journey. The gap between optimization and transformation is getting wider.",
      "Your content team is producing more than ever and engagement is down. Volume is not the problem. Relevance is the problem and you can't solve a relevance problem by publishing more."
    ],
    aiOpportunity: "Digital-native companies have the data and the infrastructure but most are using AI for content production when the real leverage is in using AI to orchestrate the entire buyer journey. Connecting behavioral signals to personalized experiences at every touchpoint is where revenue lift actually comes from.",
    stats: "70% of CMOs have adopted generative AI for content but fewer than 25% use it for journey orchestration or revenue analytics. Organizations that connect AI to their full revenue motion see 19% faster revenue growth than peers still running siloed channel strategies.",
    toneNote: "Fast and data-driven. Digital buyers move quickly and respect brevity. Open with a specific observation about their traffic strategy, content performance, or conversion gap and skip the preamble.",
    openingHooks: [
      "If they have a strong content or SEO presence: acknowledge it and ask whether the traffic is converting into pipeline or just numbers on a dashboard.",
      "If they appear to be in e-commerce or marketplace: connect rising CAC to the need for retention and lifetime value programs that go beyond email blasts.",
      "If their blog or press shows recent growth or expansion: tie the growth to the operational question of whether their marketing infrastructure is built to scale with it.",
      "If they show intent signals around pricing or demo pages: reference it directly as a sign that someone on their team is already evaluating solutions.",
      "If no specific news: open with the observation that the digital companies pulling ahead right now are not the ones spending more on ads but the ones building personalization engines that make every interaction feel one-to-one."
    ]
  },

  "FINANCIAL_SERVICES": {
    label: "Financial Services",
    pains: [
      "Your compliance team and your marketing team are in a constant standoff. By the time a campaign gets through legal review the moment has passed. You need a way to move fast without creating risk and right now you have neither speed nor a clear process.",
      "You have customers with 10 products available to them and they're using 2. Cross-sell and upsell programs exist on paper but in practice they rely on the banker or advisor remembering to bring it up. That is not a scalable revenue strategy.",
      "Digital acquisition costs are rising and branch traffic is declining. You're spending more to acquire customers who are worth less at first touch because the high-value relationships still close in person and marketing can't claim credit for them.",
      "Your CRM and your core banking system don't talk to each other in any meaningful way. Marketing is making segmentation decisions based on demographic data when the behavioral and balance data that would actually predict intent is locked in a system no one in marketing can access.",
      "Your marketing team measures leads. Your product teams measure accounts opened. Your finance team measures deposits and AUM. Nobody is working from the same definition of what success looks like and budget conversations become impossible."
    ],
    aiOpportunity: "Financial services has the richest behavioral and transactional data of any industry and it's almost entirely untapped for marketing. AI agents can score next-best-product intent, trigger compliant personalized outreach, and guide bankers and advisors with real-time recommendations, all within your existing governance framework.",
    stats: "Financial services has a bifurcated maturity model: traditional institutions lag in demand generation while fintech challengers lead. Banks integrating AI into their marketing motion see 25% faster campaign cycles and improved customer retention. Only 18% of financial services organizations have reached Revenue Marketing maturity.",
    toneNote: "Compliance-aware and commercially sharp. These buyers are sophisticated and skeptical. Never sound like a tech vendor. Sound like someone who understands the regulatory environment and the internal politics of a financial institution.",
    openingHooks: [
      "If they have recent press around a new product launch, acquisition, or expansion: acknowledge the initiative and connect it to the marketing infrastructure question of whether they can support it at scale.",
      "If they are a bank or credit union: reference the specific challenge of connecting digital marketing to funded accounts and deposit growth, which is the metric leadership actually cares about.",
      "If they are in insurance: open with the policy renewal and cross-sell problem, which is where the highest-value marketing leverage sits.",
      "If they appear to be a fintech: acknowledge their advantage in digital experience and ask whether their marketing motion is as advanced as their product.",
      "If no specific news: open with the observation that the financial institutions pulling ahead in customer acquisition and retention are the ones that figured out how to make compliance a design constraint rather than a roadblock."
    ]
  },

  "BANKING": {
    label: "Banking",
    pains: [
      "You open an account and then what? The onboarding experience is the same generic drip sequence it was five years ago. The first 90 days are when you win or lose the lifetime value of that customer and most banks treat it like a checkbox.",
      "Your highest-value customers are invisible to marketing. They manage their money through a private banker or financial advisor and those relationships live in someone's head or a spreadsheet, not your CRM.",
      "You're running the same batch-and-blast email to your entire database because building segments from core banking data requires an IT ticket and a six-week queue. By the time you can act on the insight the moment is gone.",
      "Branch traffic is down 30% in five years and digital traffic is up but converting at a fraction of what in-person conversations used to close. You have a digital presence but not a digital sales motion.",
      "Your competitor rolled out a genuinely good mobile experience and you're watching the deposit outflows in real time. Marketing can't move faster than the product but it also can't afford to wait for the product to catch up."
    ],
    aiOpportunity: "AI gives banks what they have always had in theory but never in practice: the ability to treat every customer as an audience of one, predict their next financial need before they search for it, and guide frontline staff with the right recommendation at the right moment, all within regulatory guardrails.",
    stats: "Banks integrating AI into marketing see 25% faster campaign cycles and measurable improvement in cross-sell conversion. Only 18% of financial services organizations operate at Revenue Marketing maturity, meaning the gap between laggards and leaders has never been wider or more expensive.",
    toneNote: "Sound like someone who has sat in a bank marketing meeting. Reference deposits, funded accounts, onboarding, and advisor enablement. Never use startup language or generic B2B framing.",
    openingHooks: [
      "If they have news about a branch opening, merger, or new product: connect it to the marketing question of whether their infrastructure is ready to support the growth.",
      "If they are a community bank or credit union: acknowledge the relationship advantage they have over national banks and ask whether their marketing motion is built to scale that advantage digitally.",
      "If they have a recent award or recognition: reference it and connect it to the question of whether their marketing is helping them compete for the next generation of customers who will never walk into a branch.",
      "If they appear to be investing in digital channels based on their website or blog: acknowledge the investment and ask whether it is generating measurable deposit and account growth.",
      "If no specific news: open with the observation that the banks winning the next decade are not the ones with the best branch network but the ones that can make a digital interaction feel as trusted as an in-person conversation."
    ]
  },

  "INSURANCE": {
    label: "Insurance",
    pains: [
      "You write the policy and then you disappear until renewal. The 11 months in between are a missed opportunity to cross-sell, deepen the relationship, and make it structurally difficult for the customer to leave.",
      "Your agents are your best salespeople and they have zero marketing support. They're sending the same email template from five years ago and wondering why conversion is down.",
      "Every quote request is treated the same. A 35-year-old homeowner shopping for the first time and a 55-year-old commercial buyer expanding a policy are getting the same nurture sequence.",
      "Your data is trapped in the policy admin system. Marketing can see email opens and form fills but cannot see coverage gaps, renewal probability, or lifetime value.",
      "Churn at renewal is the number one revenue leak in insurance marketing and most teams are treating it with a single renewal reminder email sent 30 days out. That is not a retention strategy."
    ],
    aiOpportunity: "Insurance marketers have more behavioral and transactional data than almost any other industry and are using almost none of it for proactive marketing. AI can predict renewal risk, identify cross-sell moments based on life events, and personalize every agent interaction so relationships scale without adding headcount.",
    stats: "Insurance firms aligning marketing with compliance and growth agendas are setting new benchmarks for revenue accountability in financial services. Organizations implementing AI-driven renewal and cross-sell programs see measurable reductions in churn and meaningful lift in policies per customer.",
    toneNote: "Speak in the language of policy retention, agent productivity, and lifetime customer value. These buyers are commercially sharp and do not respond to technology-first pitches. Lead with the revenue problem.",
    openingHooks: [
      "If they have news about a new product line or market expansion: connect it to the question of whether their agent enablement and marketing infrastructure can support the growth.",
      "If they have a strong digital presence or blog: reference a specific topic or theme and connect it to whether that content is actually generating quote requests.",
      "If they appear to be a regional carrier: acknowledge the competitive pressure from direct-to-consumer players and what that means for agent-channel marketing strategy.",
      "If their website suggests they are investing in digital: ask whether the digital experience is converting as well as an agent conversation would have ten years ago.",
      "If no specific news: open with the observation that the insurance carriers winning market share right now are not the ones with the lowest premiums but the ones that made the 11 months between renewal feel like a relationship."
    ]
  },

  "HOSPITAL_AND_HEALTH_CARE": {
    label: "Healthcare",
    pains: [
      "You have patient data, encounter data, and claims data that would make any consumer marketer envious. And you're sending a monthly newsletter. The gap between what you could be doing with that data and what you are doing is one of the biggest missed opportunities in healthcare marketing.",
      "Your marketing team is fighting a two-front war: proving ROI to a CFO who still thinks marketing is a cost center while navigating a compliance team that treats every campaign like a HIPAA violation waiting to happen.",
      "You spent real money on a CRM and a marketing automation platform and neither one is connected to your EHR in any meaningful way. Your clinicians don't trust the data, your marketing team can't act on it, and your patients are getting generic communications.",
      "Patient acquisition costs are up. Appointment no-show rates are stuck. And your referral pipeline depends on relationships that three people in your organization hold personally and would walk out the door with tomorrow.",
      "You're marketing to patients, referring physicians, and payers simultaneously, often with the same team and the same tools. These are completely different audiences with different motivations and different compliance requirements."
    ],
    aiOpportunity: "Healthcare organizations that connect AI to their patient engagement, HCP outreach, and referral marketing motion unlock the kind of personalization at scale that used to require ten times the staff. The key is doing it within HIPAA guardrails, which is not a barrier if it is built in from the start.",
    stats: "Only 8% of Healthcare organizations have reached Revenue Marketing maturity, the lowest of any major industry. Organizations that connect marketing technology to clinical and operational data see measurable improvements in patient acquisition cost, appointment conversion, and referral pipeline.",
    toneNote: "Compliance-first but commercially focused. These buyers are under CFO pressure to justify marketing spend. Reference patient acquisition, referral pipeline, and appointment conversion. Never make clinical claims.",
    openingHooks: [
      "If they have news about a new service line, facility opening, or expansion: tie it to the marketing infrastructure question of whether they can drive the patient volume to support the investment.",
      "If they have a blog or content focused on patient education: acknowledge it and ask whether it is generating actual appointment requests or just organic traffic.",
      "If they appear to be a health system with multiple service lines: reference the challenge of marketing across patients, HCPs, and payers with the same team and tools.",
      "If their description mentions a specific specialty or population: reflect that specificity back and ask whether their marketing is as specific as their clinical focus.",
      "If no specific news: open with the observation that the health systems pulling ahead in patient acquisition are the ones that figured out how to make a digital touchpoint feel as trusted as a conversation with a care coordinator."
    ]
  },

  "BIOTECHNOLOGY": {
    label: "Biotechnology",
    pains: [
      "You're pre-commercial or early commercial and your marketing team is trying to build demand generation infrastructure while also supporting clinical trials, managing KOL relationships, and preparing for a launch that keeps moving.",
      "Your scientific story is genuinely differentiated. But it's written for a scientific audience and your buyers, the ones who approve formulary decisions and write the checks, need a completely different story.",
      "HCP awareness of your therapy is high among the KOLs you've cultivated and almost nonexistent beyond that circle. Scaling beyond relationships that three people on your team hold personally is the hardest marketing problem in biotech.",
      "You're measuring marketing success by conference attendance, symposium turnout, and share of voice. None of those metrics connect to prescriptions written or accounts activated.",
      "Your competitive landscape shifts every six months as new data readouts, approvals, and label updates change what you can say. Your content and messaging architecture is not built to move that fast."
    ],
    aiOpportunity: "AI-driven HCP targeting, content personalization, and account-based marketing let biotech commercial teams reach the right prescribers and payers with the right message at the right stage of their awareness journey, compressing the time from awareness to prescription and from prescription to habit.",
    stats: "Life sciences organizations implementing AI-assisted commercial orchestration see meaningfully faster campaign cycles and improved stakeholder engagement without expanding field headcount. The window between launch and peak sales is getting shorter and the companies that automate their commercial motion are widening that window.",
    toneNote: "Speak to commercial and marketing leaders, not scientists. Lead with pipeline velocity, HCP reach, and revenue accountability. Show you understand the regulatory environment without being paralyzed by it.",
    openingHooks: [
      "If they have a recent data readout, FDA action, or approval: connect the clinical milestone to the commercial question of whether the marketing infrastructure is ready to capitalize on it.",
      "If they are in a crowded indication: acknowledge the competitive environment and ask whether their HCP awareness and differentiation strategy is built for a market where every competitor is running a similar playbook.",
      "If their pipeline or blog suggests they are building toward a launch: reference the pre-launch window and what it means to get the commercial foundation right before the clock starts.",
      "If they appear to be partnering with a larger pharma organization: acknowledge the dynamic and ask whether their marketing motion is positioned to drive the metrics that the partner cares about.",
      "If no specific news: open with the observation that the biotech companies that convert clinical momentum into commercial momentum the fastest are the ones that built their marketing infrastructure before they needed it."
    ]
  },

  "PHARMACEUTICALS": {
    label: "Pharmaceuticals",
    pains: [
      "You're running omnichannel campaigns but your channels are not actually connected. The rep visit, the digital ad, and the email are three separate programs with three separate owners and zero shared data.",
      "MLR review is killing your campaign velocity. By the time content is approved the clinical conversation has moved on. You need a content operating model that builds compliance in from the start.",
      "You have more data about HCP behavior than you've ever had and less clarity about what it means. Digital engagement scores, rep call data, speaker program attendance: all of it sits in different systems and none of it is informing next best action.",
      "Your blockbuster is facing generic competition and the growth story for the portfolio depends on brands that are earlier in the lifecycle and harder to market.",
      "Patient support programs, HCP education, and direct-to-consumer advertising are being run by three different teams with three different agency relationships and no shared customer data."
    ],
    aiOpportunity: "AI enables pharma marketing teams to personalize HCP communications at scale within MLR guardrails, predict which physicians are most likely to respond to which messages at which stage of their prescribing journey, and orchestrate omnichannel programs that feel coordinated because they actually are.",
    stats: "Pharmaceutical organizations implementing AI-assisted omnichannel orchestration see faster campaign cycles and improved HCP engagement without expanding field headcount. The shift from share of voice to share of attention is the defining commercial marketing challenge of this decade.",
    toneNote: "MLR and compliance awareness is essential. Lead with HCP reach, prescribing behavior, and market share impact. These buyers have deep agency relationships and are skeptical of consultants who do not understand the regulatory and commercial complexity of their business.",
    openingHooks: [
      "If they have a recent approval, label update, or data readout: connect the clinical news to the commercial question of whether the marketing motion is positioned to capitalize on it.",
      "If they have a launch coming: reference the pre-launch window and the importance of getting omnichannel infrastructure right before the field force is deployed.",
      "If their blog or press shows focus on patient centricity or digital health: acknowledge the focus and ask whether it is reflected in how they measure marketing's contribution to actual prescribing.",
      "If they appear to be facing patent cliff pressure: reference the portfolio transition challenge and the marketing implication of shifting investment from a mature asset to an emerging one.",
      "If no specific news: open with the observation that the pharma companies winning the next decade are not the ones with the biggest field force but the ones that figured out how to make every digital HCP interaction as valuable as a rep visit."
    ]
  },

  "INDUSTRIAL_AUTOMATION": {
    label: "Industrial Automation / Manufacturing",
    pains: [
      "Your buyers are 80% through their decision before they ever talk to your sales team. They've read your specs, compared you to three competitors, and formed an opinion. And your marketing had zero visibility into any of that.",
      "Your sales team is world-class at relationships and terrible at pipeline reporting. You have no reliable visibility into where opportunities actually stand, which means forecasting is a fiction.",
      "You've invested in a marketing automation platform and it's running the same nurture sequence it was running three years ago because nobody has the bandwidth to rebuild it.",
      "Trade shows were your demand gen engine. That model is broken. The ROI conversation is a nightmare, the leads are unqualified, and nobody wants to be the one to propose cutting the events calendar.",
      "Your content is technical documentation dressed up as thought leadership. It speaks to engineers, who are not the economic buyer. The CFO and VP of Operations want to understand business impact and your content cannot have that conversation."
    ],
    aiOpportunity: "AI gives manufacturing marketers what they have never had: visibility into buyer intent before the RFQ arrives, the ability to personalize outreach across a long and complex sales cycle, and a demand generation motion that does not depend on the trade show calendar or the sales team's relationship network.",
    stats: "Manufacturing has 30% of companies still in the Traditional Marketing stage, the highest of any industry measured by TPG. Yet manufacturing buyers complete 80% of their research before engaging sales, which means the gap between where marketing investment is going and where buyers are doing their research is enormous.",
    toneNote: "Practical and commercially direct. Manufacturing buyers have zero tolerance for marketing-speak. Lead with pipeline, quota, and ROI. Show you understand that in manufacturing, marketing exists to serve sales, not the other way around.",
    openingHooks: [
      "If they have news about a new product line, facility, or market expansion: connect the operational milestone to the commercial question of whether their demand gen infrastructure is built to create pipeline for it.",
      "If their blog or content shows investment in thought leadership: acknowledge it and ask whether it is generating qualified leads or just organic traffic from people who will never buy.",
      "If they appear to be a channel-dependent business: reference the dealer and distributor enablement problem and how marketing can systematically support channel partners.",
      "If their description mentions global sales or international markets: acknowledge the complexity and ask whether their marketing infrastructure is built to support a global sales motion.",
      "If no specific news: open with the observation that the manufacturing companies pulling ahead in demand generation are not the ones spending more on trade shows but the ones that figured out how to create pipeline from buyers who will never fill out a contact form."
    ]
  },

  "MACHINERY": {
    label: "Machinery / Industrial Equipment",
    pains: [
      "Your sales cycle is 9 to 18 months. Your marketing is optimized for the first 30 days. There is almost nothing happening in the middle of that journey where the deal is actually won or lost.",
      "You have a dealer network that is supposed to be your sales force and a marketing team that produces materials the dealers rarely use, cannot customize, and have no way to track.",
      "Your best-performing content is the content your engineers write for each other. The problem is that engineers are not the economic buyers.",
      "You went to 12 trade shows last year. You know three of them generated real pipeline. You cannot tell your CMO which three.",
      "Your CRM data is a disaster. Opportunities are logged inconsistently, contact records are out of date, and nobody can run a report that both teams agree reflects reality."
    ],
    aiOpportunity: "AI-driven demand generation and content personalization let machinery and industrial equipment marketers create pipeline from buyers who are deep in research mode but invisible to the sales team. Intent signals, behavioral data, and automated nurture replace the trade show calendar as the engine of new business.",
    stats: "Manufacturing organizations with true sales and marketing alignment see 19% faster revenue growth than unaligned peers. 30% of manufacturing companies are still in the Traditional Marketing stage, meaning the majority of competitors still rely on relationships and trade shows as their primary demand generation strategy.",
    toneNote: "Practical and direct. These buyers are operators. They want to know what problem you solve, what it costs, and how fast it works. Skip the framework introduction.",
    openingHooks: [
      "If they have a new product or equipment line: reference the launch and connect it to the question of whether their marketing can generate enough qualified pipeline to support a ramp.",
      "If they have news about a plant, facility, or capacity expansion: connect the operational investment to the commercial question of whether demand gen is keeping pace.",
      "If their blog suggests investment in digital content: acknowledge it and ask whether it is generating dealer engagement or just website traffic.",
      "If they appear to sell through a channel or dealer network: lead with the channel enablement problem because that is almost always the highest-leverage marketing opportunity in this segment.",
      "If no specific news: open with the reality that the machinery companies winning new business from competitors are not the ones with the biggest booths at trade shows but the ones that figured out how to reach buyers before the RFQ hits their inbox."
    ]
  },

  "MANAGEMENT_CONSULTING": {
    label: "Management Consulting",
    pains: [
      "Your firm's revenue depends on three partners who are brilliant at relationships and terrible at documentation. When they're busy you have no pipeline problem. When they're between engagements you have no pipeline at all. That is not a business model. That is a dependency.",
      "You produce excellent thought leadership. Whitepapers, conference talks, articles in the right publications. None of it is systematically converting into meetings.",
      "Your prospects are already talking to McKinsey, Deloitte, and BCG. The way you win is not on credentials. It's on specificity, speed, and the sense that you understand their exact problem better than anyone else.",
      "Every engagement starts from scratch. You have 15 years of client data, case studies, and institutional knowledge that could make your pitches sharper but most of it lives in someone's head or a folder no one can find.",
      "Your firm's marketing budget goes to a website refresh and a conference sponsorship every year. Neither generates measurable pipeline and nobody wants to have the conversation about doing something different."
    ],
    aiOpportunity: "AI-powered content distribution, account-based marketing, and lead intelligence let consulting firms scale the reach of their best thinking without scaling headcount. When a partner publishes an insight, AI ensures it reaches the 200 prospects most likely to be in a relevant conversation right now.",
    stats: "Only 13% of Professional Services organizations have reached Revenue Marketing maturity, despite being among the fastest movers toward revenue accountability. The firms pulling ahead are treating content as a pipeline asset, connecting every piece to a specific buyer journey and measuring it against qualified conversations generated.",
    toneNote: "Peer-to-peer and a little provocative. Management consultants are skeptical of consultants. Open with a specific observation about their firm's growth model or marketing approach and do not apologize for the implied critique.",
    openingHooks: [
      "If they have a recent publication, report, or public engagement: acknowledge the quality of the work and ask whether it is generating the pipeline it deserves.",
      "If they have recently expanded to a new market or practice area: connect the expansion to the marketing question of whether they have the demand generation infrastructure to build pipeline in a new area.",
      "If their website or blog shows strong thought leadership: note the depth and ask whether the distribution and demand capture motion behind it matches the quality of the content.",
      "If they appear to be growing through hiring: reference the growth trajectory and connect it to the challenge of building marketing infrastructure that scales with the firm.",
      "If no specific news: open with the observation that the consulting firms outgrowing their peer group are not the ones with the best credentials on their website but the ones that figured out how to turn institutional knowledge into systematic pipeline."
    ]
  },

  "ACCOUNTING": {
    label: "Accounting / Professional Services",
    pains: [
      "Referrals are your primary source of new business and you have almost no visibility into them. You don't know which clients refer the most, what triggers a referral, or how to systematically create more of them.",
      "Tax season is your Super Bowl. Every other quarter is an afterthought from a marketing standpoint. That means your pipeline is cyclical in a business that should be able to generate recurring revenue conversations year-round.",
      "Your advisory services are higher margin and more strategic than your compliance work but they represent a fraction of your revenue. You have the relationships to sell them but not the marketing motion to create consistent demand.",
      "Every partner has a different opinion about what the firm should be known for. That disagreement lives inside every piece of content, every pitch deck, and every website headline.",
      "You're competing with the Big Four on sophistication and with boutique firms on relationships. The firms winning in the middle market are the ones who figured out how to tell a specific story about a specific problem for a specific client type."
    ],
    aiOpportunity: "AI-driven content strategy and targeted digital marketing let accounting firms build a systematic pipeline alongside their referral network, turning practice area expertise into demand generation programs that generate qualified conversations without relying on who the partners happen to know.",
    stats: "Only 13% of Professional Services organizations operate at Revenue Marketing maturity. The firms that have made the transition grow advisory revenue 2x faster than those still relying primarily on referral and reputation.",
    toneNote: "Credibility-first. Accountants and accounting firm leaders are analytically rigorous and will push back on anything that sounds like marketing for marketing's sake. Lead with a specific business problem and a measurable outcome.",
    openingHooks: [
      "If they have a recent award, ranking, or industry recognition: acknowledge it and connect it to whether their marketing is helping them build on that credibility with the right buyer.",
      "If they have expanded to a new service line or market: reference the expansion and ask whether their marketing infrastructure supports the new direction.",
      "If their blog shows investment in thought leadership: acknowledge the quality and ask whether it is generating meetings or just demonstrating expertise to people who are already clients.",
      "If they have news about a merger or acquisition: connect the growth event to the marketing integration question of whether the combined firm has a coherent story.",
      "If no specific news: open with the observation that the accounting firms growing advisory revenue fastest are not the ones with the most impressive client list but the ones that figured out how to systematically create demand for work that does not require them to be present to sell it."
    ]
  },

  "RETAIL": {
    label: "Retail",
    pains: [
      "You know everything about what customers bought and almost nothing about why they stopped buying. Churn is invisible until it shows up in revenue and by then the relationship is already gone.",
      "Your loyalty program has millions of members and single-digit redemption rates. You built a database when you needed to build a relationship program.",
      "You're running the same email cadence to your entire customer base. The person who bought once three years ago and the person who bought twice last month are getting the same message.",
      "Physical traffic is declining and digital conversion is not picking up the difference. You're investing in both channels without a clear model for how they support each other.",
      "You have first-party data that is genuinely valuable and a growing urgency to use it before privacy regulation makes it even harder to act on."
    ],
    aiOpportunity: "AI enables retailers to personalize every touchpoint at scale, from the homepage to the post-purchase email, connect first-party behavioral data to actual purchase outcomes, and build the kind of loyalty that survives a price comparison. The retailers winning right now are not competing on price. They are competing on relevance.",
    stats: "Only 17% of Retail organizations have reached Revenue Marketing maturity. Retailers implementing AI-driven personalization and lifecycle marketing programs see meaningful improvements in repeat purchase rate and customer lifetime value within the first two quarters.",
    toneNote: "Connect everything to lifetime value, repeat purchase rate, and revenue per customer. Retail CMOs are under constant pressure from CFOs who can see revenue in real time. Lead with outcomes that show up on a dashboard.",
    openingHooks: [
      "If they have news about a new store opening, market expansion, or brand launch: connect the growth initiative to the marketing question of whether customer acquisition and retention programs are built to support it.",
      "If their blog or content shows investment in customer experience: acknowledge it and ask whether it is generating measurable improvement in repeat purchase rates.",
      "If they appear to be investing heavily in e-commerce or digital: reference the channel investment and ask whether digital conversion is keeping pace with digital traffic investment.",
      "If their description mentions a loyalty program: reference the program and ask whether it is generating lifetime value or just storing data that is not being activated.",
      "If no specific news: open with the observation that the retailers growing revenue per customer fastest are not the ones running the most promotions but the ones that figured out how to make every interaction feel like it was designed for that specific customer."
    ]
  },

  "MEDIA_PRODUCTION": {
    label: "Media & Entertainment",
    pains: [
      "Your content is excellent. Your distribution strategy is not. You're publishing into the same channels at the same cadence and wondering why reach is declining. The algorithm changed. Your strategy did not.",
      "Advertising revenue is structurally declining. The subscription model everyone pivoted to is proving harder to sustain than the projections suggested.",
      "You have a large audience and very little actionable data about them. You know what they consume but not who they are or what else they might pay for.",
      "Churn on your subscription product is higher than it should be and the win-back programs are not working because they're offering a discount to someone who left because of relevance, not price.",
      "Your editorial team and your marketing team operate as completely separate functions with almost no shared data and no shared strategy."
    ],
    aiOpportunity: "AI gives media companies the ability to personalize content recommendations, predict subscriber churn before it happens, and build audience intelligence that turns anonymous reach into addressable marketing segments.",
    stats: "Media companies face dual disruption: declining ad revenues and the urgency to monetize new digital experiences. Organizations using AI for audience personalization and churn prediction see measurable improvements in subscription retention and revenue per subscriber.",
    toneNote: "Editorially fluent and commercially direct. Media buyers respect creative instincts but are under real financial pressure. Lead with the business model problem. Show you understand the tension between editorial integrity and revenue accountability.",
    openingHooks: [
      "If they have a recent content launch, show, or publication: acknowledge it and connect it to the audience acquisition and retention question behind every content investment.",
      "If they have news about a platform expansion or new distribution channel: reference it and ask whether their marketing infrastructure is built to convert that reach into subscribers.",
      "If their blog or content shows investment in audience development: acknowledge the strategy and ask whether it is generating measurable subscriber growth.",
      "If they appear to be running both advertising and subscription models: acknowledge the dual-revenue complexity and connect it to the audience intelligence problem.",
      "If no specific news: open with the observation that the media companies successfully growing subscription revenue are not the ones producing the most content but the ones that figured out how to make every reader feel like the publication was made specifically for them."
    ]
  },

  "HIGHER_EDUCATION": {
    label: "Higher Education",
    pains: [
      "The 18-to-22 demographic is shrinking. Everyone knows it. Most institutions are responding by trying harder at the same things they have always done. The schools that will thrive are the ones that figured out how to market to adult learners, career changers, and international students.",
      "Your inquiry-to-enrollment funnel is broken somewhere in the middle. Applications are up. Enrollment is flat. Yield is the problem and most enrollment marketing teams are treating it with a generic email sequence.",
      "You have a CRM and a marketing automation platform and neither one is connected to the student information system in any meaningful way.",
      "Your competitor down the road just launched a new online program and is spending aggressively on search. Your digital marketing budget has not changed in three years.",
      "Your alumni relations and your enrollment marketing are two completely separate operations with no shared data and no shared strategy. Your most credible marketers are your alumni and they have no systematic way to advocate for your institution."
    ],
    aiOpportunity: "AI-driven personalization and journey orchestration let higher education marketers meet prospective students at every decision point with messaging that reflects where they are in their process, what program they are considering, and what their specific concerns are.",
    stats: "Only 12% of Higher Education organizations have reached Revenue Marketing maturity. Institutions that treat enrollment as a revenue marketing problem and invest in personalized digital journeys see measurable improvement in yield rate and net tuition revenue without increasing the size of their admissions team.",
    toneNote: "Speak to enrollment marketing leaders, CMOs, and VPs of Marketing. Connect everything to enrollment yield, net tuition revenue, and the institution's financial sustainability.",
    openingHooks: [
      "If they have a new program launch or curriculum expansion: connect the academic investment to the enrollment marketing question of whether they have the demand generation motion to fill it.",
      "If they have recent news about rankings, research, or institutional recognition: acknowledge the achievement and ask whether their enrollment marketing is capitalizing on it.",
      "If their website or blog shows investment in online or hybrid learning: reference the strategic shift and ask whether their marketing infrastructure is optimized for the online learner journey.",
      "If they appear to be focused on a specific student population: reflect that focus back and ask whether their personalization and outreach infrastructure matches the specificity of their academic mission.",
      "If no specific news: open with the observation that the institutions improving enrollment yield fastest are not the ones with the most impressive rankings but the ones that figured out how to make a prospective student feel seen from the first click."
    ]
  },

  "STAFFING_AND_RECRUITING": {
    label: "Staffing & Recruiting",
    pains: [
      "You have two audiences that need marketing simultaneously: candidates and clients. Most staffing firms are mediocre at marketing to both because they're trying to use the same team, the same tools, and the same strategy for two completely different buyer journeys.",
      "Your best clients came from someone's existing relationship. Your growth depends on building new relationships at scale and your marketing team has no systematic way to do that.",
      "The job board model is broken. You're paying for candidate volume you cannot place and ignoring the passive candidates in your own database who were qualified enough to work with you once.",
      "You place someone. They start. The relationship with the hiring manager goes quiet until they have another open req. Twelve months of silence is not a retention strategy.",
      "Your niche is your value. But your marketing does not communicate your niche. Your website, your emails, and your LinkedIn presence look like every other staffing firm."
    ],
    aiOpportunity: "AI agents can automate candidate matching, personalize client outreach, and surface intent signals across both audiences simultaneously, giving staffing firms the ability to run a marketing motion that is genuinely different from the industry standard of job board spend and cold outreach.",
    stats: "Staffing organizations implementing AI-driven candidate nurture and client engagement programs see meaningful improvements in placement velocity and client retention within the first two quarters. The firms growing market share fastest are treating business development as a marketing problem, not just a sales activity.",
    toneNote: "Commercial and specific. Staffing buyers are pragmatic. Lead with placement velocity, client retention, and the economics of building pipeline beyond cold outreach and job boards.",
    openingHooks: [
      "If they have news about a new practice area, market expansion, or acquisition: connect the growth to the marketing question of whether their brand and demand generation infrastructure supports the new direction.",
      "If their blog or content shows a focus on a specific industry or role type: acknowledge the niche focus and ask whether their marketing is as specific as their practice.",
      "If they appear to be investing in technology or AI for candidate matching: reference the technology investment and connect it to the business development question of whether client-facing marketing is keeping pace.",
      "If they have recent awards or recognition: acknowledge it and connect it to the question of whether their marketing is amplifying that credibility.",
      "If no specific news: open with the observation that the staffing firms outgrowing their competitors are not the ones with the most recruiters but the ones that figured out how to make every client interaction feel designed specifically for their hiring challenge."
    ]
  },

  "REAL_ESTATE": {
    label: "Real Estate",
    pains: [
      "Your pipeline is entirely dependent on market conditions. In a hot market everyone looks like a great operator. In a slow market you find out who actually has a marketing motion and who was just riding the cycle.",
      "You have thousands of contacts in your CRM who reached out at some point, were not ready, and went cold. Most of them are still in the market and some of them are actively working with someone else right now.",
      "Your marketing is almost entirely top-of-funnel. You generate inquiries but the conversion happens through relationships that your best agents hold personally. When they leave, the pipeline goes with them.",
      "Every listing gets a standard marketing package. The $10M waterfront property and the $400K condo are getting the same digital strategy.",
      "Your brand exists on the strength of individual agents rather than the strength of the company. That is a liability in a market where buyers and sellers increasingly start their search online."
    ],
    aiOpportunity: "AI-driven lead nurturing, behavioral intent scoring, and personalized follow-up sequences let real estate firms stay relevant to prospects across a buying cycle that might last 18 months without requiring an agent to manually track every conversation.",
    stats: "Real estate organizations implementing AI-driven lead prioritization and automated nurture see sales response time improve by 92% and meaningful pipeline lift within the first quarter. The gap between agents who use AI-assisted marketing and those who do not is widening every quarter.",
    toneNote: "Connect everything to transaction velocity and lead conversion. Real estate buyers are relationship-oriented but commercially driven. Show you understand the cyclical nature of the market.",
    openingHooks: [
      "If they have news about a new market entry, development project, or acquisition: connect the strategic expansion to the marketing question of whether they have the brand presence and demand generation motion to support it.",
      "If their blog or content shows investment in market education: acknowledge the strategy and ask whether it is generating qualified inquiries.",
      "If they appear to be a commercial real estate firm: reference the longer sales cycle and the challenge of staying relevant to a prospect across an 18-month decision process.",
      "If they appear to be primarily residential: acknowledge the market cycle pressure and connect it to the question of what their marketing motion looks like when the market slows.",
      "If no specific news: open with the observation that the real estate firms maintaining pipeline in a difficult market are not the ones spending more on listings but the ones that built a marketing infrastructure that keeps them relevant across the entire buying journey."
    ]
  },

  "TELECOMMUNICATIONS": {
    label: "Telecommunications",
    pains: [
      "Churn is your biggest revenue problem and your retention marketing is almost entirely reactive: a discount offer sent 30 days before the contract end date to someone who made up their mind three months ago.",
      "You have the most detailed behavioral data of any industry and it sits in systems that marketing cannot access in real time. You're marketing at a population level when you have the data to market to an audience of one.",
      "Your cross-sell programs exist in the contact center. Marketing has nothing to do with them and because marketing has nothing to do with them they're inconsistent and generating a fraction of the revenue they should.",
      "You're competing against companies that are willing to lose money to take your best customers. Price competition is a strategy you can only win for so long.",
      "Your brand marketing and your performance marketing are run by different teams with different agencies and different attribution models optimizing for different things."
    ],
    aiOpportunity: "AI-driven churn prediction, next-best-action recommendations, and personalized retention programs let telecom marketers shift from reactive win-back campaigns to proactive lifecycle management that protects revenue and reduces acquisition costs at the same time.",
    stats: "Telecommunications companies implementing AI across customer lifecycle programs see measurable reductions in churn rate and meaningful improvement in revenue per subscriber within the first year. The shift from mass marketing to behavioral personalization is the defining competitive challenge of the decade in telecom.",
    toneNote: "Lead with churn economics and revenue per subscriber. Telecom marketing leaders are under constant pressure from the CFO and the board. Connect AI and personalization directly to the metrics that appear in the quarterly earnings call.",
    openingHooks: [
      "If they have news about a network expansion, new product tier, or market entry: connect the network investment to the marketing question of whether customer acquisition and retention programs are built to convert coverage into revenue.",
      "If they have a recent brand or campaign launch: acknowledge it and ask whether the brand investment is translating into measurable improvement in customer acquisition cost.",
      "If their blog or content shows focus on customer experience: reference the focus and connect it to the churn economics question.",
      "If they appear to be in a highly competitive market: acknowledge the pricing pressure and ask whether their retention marketing is built to compete on something other than price.",
      "If no specific news: open with the observation that the telecom companies growing revenue per customer fastest are not the ones spending the most on acquisition but the ones that figured out how to make churn feel like the wrong decision six months before the contract comes up."
    ]
  },

  "NON_PROFIT_ORGANIZATION_MANAGEMENT": {
    label: "Nonprofit",
    pains: [
      "Your major donor relationships live in the ED's head and two board members' address books. That is not a development program. That is a dependency. The day any of those people move on you lose institutional knowledge that took decades to build.",
      "Your digital fundraising is producing a lot of small gifts from a lot of new donors who give once and disappear. First-year retention is below 30% for most organizations and you're filling a leaky bucket.",
      "Your program team and your development team have completely different priorities and almost no shared data. Program is measuring impact. Development is measuring dollars raised.",
      "You're producing content that demonstrates impact and distributing it to the people who already care about your mission. The people who don't know you yet are not seeing it.",
      "Your board thinks marketing is a cost you should minimize. You're running a sophisticated organization with a third of the marketing infrastructure you actually need and trying not to show it."
    ],
    aiOpportunity: "AI-driven donor segmentation, personalized cultivation sequences, and behavioral intent signals let nonprofit development teams scale their major gift pipeline without scaling headcount. When AI tells you which donor is showing the signals of a major gift conversation, your development team can have the right conversation at the right moment.",
    stats: "Nonprofit organizations connecting AI to donor engagement and cultivation see faster renewal rates and lower acquisition costs. The organizations growing their major gift pipeline fastest are the ones treating development as a revenue marketing challenge and building the infrastructure to support it.",
    toneNote: "Mission-aware and commercially grounded. Nonprofit buyers are under real financial pressure but resistant to anything that sounds like corporate sales language. Connect marketing investment to mission impact and organizational sustainability.",
    openingHooks: [
      "If they have a recent campaign, event, or program milestone: acknowledge the achievement and connect it to the development question of whether their donor marketing infrastructure is built to turn that momentum into long-term giving.",
      "If their blog or content shows strong programmatic storytelling: acknowledge the quality and ask whether the distribution and donor cultivation motion behind it matches the strength of the narrative.",
      "If they have news about a new initiative or expanded program: connect the mission investment to the development question of whether their major gift pipeline is positioned to fund it.",
      "If they appear to be running a capital campaign or major initiative: reference the campaign and ask whether their donor communications and cultivation sequences are as organized as the campaign itself.",
      "If no specific news: open with the observation that the nonprofits growing major gift revenue fastest are not the ones with the most compelling mission but the ones that figured out how to make every donor feel like the organization's success depends on them specifically."
    ]
  },

  "DEFAULT": {
    label: "B2B",
    pains: [
      "Your pipeline looks fine on paper until you ask sales to walk through it deal by deal. What you thought was a $4M pipeline is actually $1.5M of real opportunity and the rest is wishful thinking that nobody wants to remove from the CRM.",
      "You're investing in marketing programs that generate activity metrics but not revenue metrics. Impressions, opens, MQLs: your CMO can defend these in a budget review but your CFO is not impressed.",
      "Your MarTech stack has grown to 15 or 20 tools and you're getting maybe 40% of the value you're paying for. Every tool solved a specific problem when it was purchased. Together they create integration overhead and data inconsistency.",
      "AI is everywhere in your industry and your team is experimenting with it in isolated pockets. Nobody has a roadmap that connects AI investment to revenue outcomes.",
      "Sales and marketing are not aligned. That sentence has appeared in every B2B marketing survey for 20 years and it is still true. The cost is 67% of marketing leads being rejected by sales and a pipeline that neither team trusts."
    ],
    aiOpportunity: "AI connects the dots across marketing, sales, and customer success: automating the workflows that slow your team down, personalizing buyer journeys that are too complex to manage manually, and surfacing the revenue signals that manual processes consistently miss until it is too late to act on them.",
    stats: "88% of B2B companies still operate below Revenue Marketing maturity. Organizations implementing AI across all six RM6 pillars see 25 to 40% improvement in revenue per employee. The gap between companies that have built a Revenue Marketing operating model and those that have not is widening every quarter.",
    toneNote: "Direct and commercially focused. Lead with a specific observation about this company's situation. Avoid generic B2B framing. If you do not have enough company data to open with something specific, lead with the most provocative industry insight you have.",
    openingHooks: [
      "If they have recent news, a product launch, or a funding event: open with that and connect it to the marketing question it raises.",
      "If they have a blog or content that reveals their strategic priorities: reflect those priorities back and connect them to the gap between where they are and where they need to be.",
      "If their company description suggests a specific go-to-market motion: reference that motion and ask whether the marketing infrastructure is built to support it at scale.",
      "If they have visited specific pages on the TPG website: reference what they were researching and connect it to the broader challenge it suggests they are working on.",
      "If no specific data is available: open with the observation that most B2B marketing leaders are under more pressure to prove revenue impact than at any point in the last decade, and that the companies pulling ahead are the ones that stopped treating marketing as a service center and started treating it as a revenue engine."
    ]
  }
};

// =============================
// INDUSTRY RESOLVER
// =============================
function resolveIndustryPersona(industry) {
  const normalized = (industry || '')
    .toUpperCase()
    .replace(/\s+/g, '_')
    .replace(/[^A-Z_]/g, '');

  if (INDUSTRY_PERSONAS[normalized]) {
    console.log(`🏭 Industry match (exact): ${normalized}`);
    return INDUSTRY_PERSONAS[normalized];
  }
  for (const key of Object.keys(INDUSTRY_PERSONAS)) {
    if (key !== 'DEFAULT' && normalized.includes(key)) {
      console.log(`🏭 Industry match (partial): ${normalized} → ${key}`);
      return INDUSTRY_PERSONAS[key];
    }
  }
  for (const key of Object.keys(INDUSTRY_PERSONAS)) {
    if (key !== 'DEFAULT' && key.includes(normalized) && normalized.length > 3) {
      console.log(`🏭 Industry match (reverse partial): ${normalized} → ${key}`);
      return INDUSTRY_PERSONAS[key];
    }
  }
  console.log(`⚠️ No industry match for: "${industry}". Using DEFAULT.`);
  return INDUSTRY_PERSONAS['DEFAULT'];
}

// =============================
// QUEUE, CONCURRENCY & TRACKING
// =============================
let queue = [];
let inFlight = 0;
let errorCount = 0;
const processingIds = new Set();

// =============================
// HUBSPOT PATCH WITH RETRY
// =============================
async function hubspotPatch(url, data, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      await axios.patch(url, data, {
        headers: {
          Authorization: `Bearer ${HUBSPOT_TOKEN}`,
          'Content-Type': 'application/json'
        },
        timeout: 10000
      });
      return;
    } catch (err) {
      if (i === retries - 1) {
        if (err.response?.data) {
          console.error(`HubSpot error detail:`, JSON.stringify(err.response.data));
        }
        throw err;
      }
      const wait = (i + 1) * 2000;
      console.log(`⚠️ HubSpot PATCH failed (attempt ${i + 1}), retrying in ${wait}ms — ${err.message}`);
      await new Promise(r => setTimeout(r, wait));
    }
  }
}

// =============================
// ERROR LOG VIEWER
// =============================
app.get("/errors", (req, res) => {
  try {
    if (!fs.existsSync(ERROR_LOG)) {
      return res.send(`
        <!DOCTYPE html><html>
        <head><title>Error Log — Industry AI Nurture</title>
        <style>body{font-family:monospace;background:#0f0f0f;color:#a2cf23;padding:40px;}
        h1{color:#fff;font-size:18px;}</style></head>
        <body><h1>Error Log — Industry AI Nurture</h1><p>No errors logged yet.</p></body></html>
      `);
    }

    const lines = fs.readFileSync(ERROR_LOG, 'utf8').trim().split('\n').filter(Boolean);
    const rows = lines.map(line => {
      const parts = line.split(',');
      const contactId = parts[0] || '';
      const step     = parts[1] || '';
      const message  = parts.slice(2, -1).join(',') || '';
      const ts       = parts[parts.length - 1] || '';
      return { contactId, step, message, ts };
    });

    const tableRows = rows.map(r => `
      <tr>
        <td>${r.ts}</td>
        <td>${r.contactId}</td>
        <td>${r.step}</td>
        <td style="color:#e05252;">${r.message}</td>
      </tr>`).join('');

    res.send(`
      <!DOCTYPE html><html>
      <head>
        <title>Error Log — Industry AI Nurture</title>
        <style>
          body{font-family:monospace;background:#0f0f0f;color:#a2cf23;padding:40px;}
          h1{color:#fff;font-size:18px;margin-bottom:8px;}
          .summary{color:#555;font-size:13px;margin-bottom:30px;}
          table{border-collapse:collapse;width:100%;}
          th{text-align:left;color:#555;font-size:12px;padding:8px 12px;border-bottom:1px solid #1a1a1a;}
          td{padding:8px 12px;font-size:13px;border-bottom:1px solid #111;vertical-align:top;}
          .footer{font-size:12px;color:#333;margin-top:40px;border-top:1px solid #1a1a1a;padding-top:20px;}
        </style>
      </head>
      <body>
        <h1>Error Log — Industry AI Nurture</h1>
        <div class="summary">${rows.length} permanent failure${rows.length !== 1 ? 's' : ''} recorded</div>
        <table>
          <thead><tr>
            <th>Timestamp</th>
            <th>Contact ID</th>
            <th>Step</th>
            <th>Error</th>
          </tr></thead>
          <tbody>${tableRows}</tbody>
        </table>
        <div class="footer">Generated: ${new Date().toLocaleString()}</div>
      </body></html>
    `);
  } catch (err) {
    res.status(500).send(`Error reading log: ${err.message}`);
  }
});

// =============================
// HEALTH CHECK
// =============================
app.get("/", (req, res) => {
  res.json({
    status: "ok",
    queueLength: queue.length,
    inFlight,
    concurrency: CONCURRENCY,
    errorCount,
    domainsCached: domainCache.size,
    contactsCompleted: completedIds.size
  });
});

// =============================
// LIVE DASHBOARD
// =============================
app.get("/dashboard", (req, res) => {
  res.send(`
    <!DOCTYPE html><html>
    <head>
      <title>Industry AI Nurture — Scott Benedetti</title>
      <meta http-equiv="refresh" content="5">
      <style>
        body { font-family: monospace; background: #0f0f0f; color: #a2cf23; padding: 40px; }
        h1 { font-size: 18px; margin-bottom: 30px; color: #fff; }
        .grid { display: flex; gap: 40px; margin-bottom: 40px; flex-wrap: wrap; }
        .stat { font-size: 56px; font-weight: bold; margin: 0; line-height: 1; }
        .label { font-size: 12px; color: #555; margin-top: 8px; }
        .green{color:#a2cf23;}.orange{color:#f0a500;}.red{color:#e05252;}.grey{color:#333;}
        .footer { font-size: 12px; color: #333; margin-top: 40px; border-top: 1px solid #1a1a1a; padding-top: 20px; }
      </style>
    </head>
    <body>
      <h1>Industry AI Nurture &mdash; Scott Benedetti</h1>
      <div class="grid">
        <div><div class="stat ${queue.length > 0 ? 'orange' : 'grey'}">${queue.length}</div><div class="label">waiting in queue</div></div>
        <div><div class="stat green">${inFlight}</div><div class="label">in-flight (of ${CONCURRENCY} max)</div></div>
        <div><div class="stat green">${CONCURRENCY - inFlight}</div><div class="label">open slots</div></div>
        <div><div class="stat ${errorCount > 0 ? 'red' : 'grey'}">${errorCount}</div><div class="label">errors since restart</div></div>
        <div><div class="stat green">${domainCache.size}</div><div class="label">domains cached</div></div>
        <div><div class="stat green">${completedIds.size}</div><div class="label">contacts completed</div></div>
      </div>
      <div class="footer">Refreshed: ${new Date().toLocaleTimeString()} &nbsp;·&nbsp; Auto-refreshes every 5s</div>
    </body></html>
  `);
});

// =============================
// ENQUEUE
// =============================
app.post("/enqueue", (req, res) => {
  const job = { ...req.body, retries: 0 };
  const key = `${job.contactId}_${job.sequenceStep}`;

  if (completedIds.has(key)) {
    console.log(`⏭️ Already completed, skipping: ${key}`);
    return res.status(200).json({ status: "skipped", reason: "already_completed" });
  }

  queue.push(job);
  res.status(200).json({ status: "queued", queuePosition: queue.length });
});

// =============================
// PROCESS JOB
// OPTIMIZED: status merged into writeResults — saves 2 HubSpot PATCH calls per job
//            (previously: IN_PROGRESS + writeResults + SENT = 3 calls; now: 1 call)
// OPTIMIZED: 529 Anthropic overload handler added alongside existing 429 handler
//            (prevents burning retry counter on transient overload errors)
// =============================
async function processJob(job) {
  const key = `${job.contactId}_${job.sequenceStep}`;

  if (processingIds.has(key)) {
    console.log(`⚠️ Duplicate in-flight, skipping: ${key}`);
    inFlight--;
    return;
  }
  processingIds.add(key);

  try {
    const result = await runClaude(job);
    // Status SENT is written in the same PATCH as the email content
    await writeResults(job.contactId, result, job.sequenceStep || 1, 'SENT');

    completedIds.add(key);
    logCompleted(job.contactId, job.sequenceStep);
    console.log(`✅ Completed: ${job.contactId} - Step ${job.sequenceStep}`);
  } catch (err) {
    console.error(`❌ Error for ${job.contactId}:`, err.message);

    if (err.response?.status === 429) {
      // HubSpot rate limit — requeue at front, respect retry-after header
      const retryAfter = (parseInt(err.response.headers['retry-after']) || 60) * 1000;
      console.log(`⏳ HubSpot rate limited (429), requeuing ${job.contactId} in ${retryAfter}ms`);
      setTimeout(() => queue.unshift(job), retryAfter);
    } else if (err.response?.status === 529) {
      // Anthropic overloaded — requeue at front after 30s without burning retry counter
      console.log(`⏳ Anthropic overloaded (529), requeuing ${job.contactId} in 30s`);
      setTimeout(() => queue.unshift(job), 30000);
    } else {
      job.retries = (job.retries || 0) + 1;
      if (job.retries <= 2) {
        queue.push(job);
      } else {
        errorCount++;
        logError(job.contactId, job.sequenceStep, err.message);
        // updateStatus only called on permanent failure — all other status writes
        // are handled inside writeResults to minimize HubSpot API calls
        await updateStatus(job.contactId, 'FAILED');
      }
    }
  } finally {
    processingIds.delete(key);
    inFlight--;
  }
}

// =============================
// WORKER LOOP
// =============================
setInterval(() => {
  while (inFlight < CONCURRENCY && queue.length > 0) {
    const job = queue.shift();
    inFlight++;
    processJob(job);
  }
  if (queue.length > 0 || inFlight > 0) {
    console.log(`📊 Queue: ${queue.length} | In-flight: ${inFlight} | Completed: ${completedIds.size} | Cached domains: ${domainCache.size}`);
  }
}, PROCESS_INTERVAL_MS);

// =============================
// URL NORMALIZER
// =============================
function normalizeUrl(rawUrl) {
  if (!rawUrl) return null;
  let url = rawUrl.trim().replace(/\/+$/, '');
  if (!/^https?:\/\//i.test(url)) url = 'https://' + url;
  try { new URL(url); return url; } catch { return null; }
}

// =============================
// HTML STRIPPER
// =============================
function stripHtml(html) {
  return html
    .replace(/<script[\s\S]*?<\/script>/gi, '')
    .replace(/<style[\s\S]*?<\/style>/gi, '')
    .replace(/<[^>]+>/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

// =============================
// TITLE EXTRACTION
// =============================
async function extractTitles(url) {
  const normalized = normalizeUrl(url);
  if (!normalized) return [];
  try {
    const res = await axios.get(normalized, {
      timeout: 4000,
      maxContentLength: 300000,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-US,en;q=0.9'
      }
    });
    const html = res.data || '';
    const headlineMatches = [];
    const headingRegex = /<h[12][^>]*>([\s\S]*?)<\/h[12]>/gi;
    let match;
    while ((match = headingRegex.exec(html)) !== null) {
      const text = match[1].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim();
      if (text.length >= 20 && text.length <= 160) headlineMatches.push(text);
    }
    if (headlineMatches.length >= 2) return headlineMatches.slice(0, 3);
    const text = stripHtml(html);
    const fallbackMatches = [...text.matchAll(/(.{25,120})\s+(20\d{2})/g)];
    return fallbackMatches.map(m => m[1].trim()).slice(0, 3);
  } catch (err) {
    const reason = err.code === 'ECONNABORTED' ? 'timeout' : err.response ? `HTTP ${err.response.status}` : err.message;
    console.log(`⚠️ Could not fetch ${normalized}: ${reason}`);
    return [];
  }
}

// =============================
// COMPANY RESEARCH — PERSISTENT DOMAIN CACHE
// =============================
async function getCompanyContent(website) {
  const baseUrl = normalizeUrl(website);
  if (!baseUrl) return { newsBlock: null, blogBlock: null };
  let domain = '';
  try { domain = new URL(baseUrl).hostname.replace(/^www\./, ''); } catch { return { newsBlock: null, blogBlock: null }; }
  if (BLOCKED_DOMAINS.has(domain)) { console.log(`🚫 Skipping: ${domain}`); return { newsBlock: null, blogBlock: null }; }

  if (domainCache.has(domain)) {
    const cached = domainCache.get(domain);
    if (Date.now() - cached.cachedAt < CACHE_TTL_MS) {
      console.log(`💾 Cache hit: ${domain}`);
      return { newsBlock: cached.newsBlock, blogBlock: cached.blogBlock };
    }
    console.log(`♻️ Cache expired for ${domain} — re-scraping`);
  }

  const newsPaths = ['/news', '/press', '/newsroom', '/press-releases', '/company-news', '/about/news', '/awards', '/recognition'];
  const blogPaths = ['/blog', '/insights', '/resources', '/thought-leadership', '/articles'];
  let newsBlock = null, blogBlock = null;

  for (const p of newsPaths) {
    const titles = await extractTitles(`${baseUrl}${p}`);
    if (titles.length >= 1) {
      newsBlock = `COMPANY NEWS & AWARDS (scraped from ${baseUrl}${p}):\n` + titles.map(t => `- ${t}`).join('\n');
      break;
    }
  }
  for (const p of blogPaths) {
    const titles = await extractTitles(`${baseUrl}${p}`);
    if (titles.length >= 1) {
      blogBlock = `COMPANY BLOG & THOUGHT LEADERSHIP (scraped from ${baseUrl}${p}):\n` + titles.map(t => `- ${t}`).join('\n');
      break;
    }
  }

  domainCache.set(domain, { newsBlock, blogBlock, cachedAt: Date.now() });
  saveCacheToDisk();
  console.log(`💾 Cached + saved: ${domain} (${domainCache.size} domains total)`);

  return { newsBlock, blogBlock };
}

// =============================
// POST-PROCESSING
// =============================
function removeDashes(text) {
  return text.replace(/\s*—\s*/g, ', ').replace(/\s*–\s*/g, ', ').replace(/  +/g, ' ').trim();
}
function removeMarkdown(text) {
  return text
    .replace(/\*\*(.+?)\*\*/g, '$1')
    .replace(/\*(.+?)\*/g, '$1')
    .replace(/__([^_\n]+?)__/g, '$1')
    .replace(/(?<![a-zA-Z0-9])_([^_\n]+?)_(?![a-zA-Z0-9])/g, '$1')
    .replace(/`(.+?)`/g, '$1')
    .trim();
}
function removeSignature(text) {
  return text
    .replace(/\n+\s*(Scott|Pedowitz)\s*$/i, '')
    .replace(/\n+\s*(Best|Best regards|Thanks|Thank you|Regards|Sincerely|Cheers|Warm regards)[^\n]*/gi, '')
    .trim();
}
function sanitizeUrls(text) {
  return text.replace(/href="([^"]+?)%22/gi, 'href="$1');
}

// =============================
// CLAUDE LOGIC
// OPTIMIZED: prior emails trimmed to 300-char excerpts
//   - Subject lines are the primary dedup signal (already passed separately)
//   - 300-char excerpt catches angle/framing repeats without full body overhead
//   - Saves ~300-350 tokens per prior step; steps 6-10 see the biggest benefit
// OPTIMIZED: max_tokens reduced 1500 -> 1000
//   - Target output is 40-85 words (~200-350 tokens including subject + HTML)
//   - 1000 tokens is still 3x headroom; 1500 was unnecessary
// =============================
async function runClaude(job) {
  const SEQUENCE_STEP = job.sequenceStep || 1;
  const stepConfig = SEQUENCE_MAP[(SEQUENCE_STEP - 1) % SEQUENCE_MAP.length];

  const {
    firstname = '',
    company = '',
    jobtitle = '',
    industry = '',
    numemployees = '',
    annualrevenue = '',
    hs_linkedin_url = '',
    website = '',
    hs_intent_signals_enabled = '',
    web_technologies = '',
    description = '',
    hs_analytics_last_url = '',
    hs_analytics_num_page_views = ''
  } = job;

  const salutation = firstname.trim() || 'Hi there';
  const persona = resolveIndustryPersona(industry);

  const IntentContext = hs_intent_signals_enabled === "true"
    ? "Buyer intent signals are ACTIVE for this account."
    : "No active intent signals.";

  let behavioralContext = '';
  const pageViews = parseInt(hs_analytics_num_page_views) || 0;
  const lastUrl = (hs_analytics_last_url || '').trim();
  if (pageViews >= 10) behavioralContext += `HIGH engagement: ${pageViews} pages viewed. `;
  else if (pageViews >= 5) behavioralContext += `MODERATE engagement: ${pageViews} pages viewed. `;
  else if (pageViews >= 1) behavioralContext += `INITIAL visit: ${pageViews} page(s). `;
  else behavioralContext += 'No website visits recorded. ';
  if (lastUrl) {
    const url = lastUrl.toLowerCase();
    if (url.includes('/pricing')) behavioralContext += 'Last page: PRICING — high intent.';
    else if (url.includes('/demo') || url.includes('/get-started')) behavioralContext += 'Last page: DEMO — very high intent.';
    else if (url.includes('/case-stud') || url.includes('/customer')) behavioralContext += 'Last page: CASE STUDIES — seeking proof.';
    else if (url.includes('/ai-assessment') || url.includes('/ai-agent')) behavioralContext += 'Last page: AI SERVICES — evaluating AI readiness.';
    else if (url.includes('/revenue-marketing')) behavioralContext += 'Last page: REVENUE MARKETING — considering transformation.';
    else if (url.includes('/hubspot')) behavioralContext += 'Last page: HUBSPOT SERVICES.';
    else {
      const pageName = lastUrl.split('/').filter(p => p).pop()?.replace(/-/g, ' ') || 'homepage';
      behavioralContext += `Last page: ${pageName}.`;
    }
  }

  // OPTIMIZED: 300-char excerpts instead of full prior email bodies
  // Rationale: subject lines already carry the primary dedup signal.
  // Excerpts (opening sentence + angle) are sufficient to prevent framing repeats.
  // Full bodies at step 9 added ~1,400-1,600 tokens with minimal dedup benefit.
  const priorEmailsText = [];
  const priorSubjects = [];
  for (let i = 1; i < SEQUENCE_STEP; i++) {
    const bodyField = job[`industry_ai_nurture_claude_text_em${i}`];
    const subjectField = job[`industry_ai_nurture_subject_line_em${i}`];
    if (bodyField) {
      const excerpt = bodyField.slice(0, 300).replace(/\s+/g, ' ').trim();
      priorEmailsText.push(`EMAIL ${i} [excerpt]: ${excerpt}…`);
    }
    if (subjectField) priorSubjects.push(`Email ${i}: "${subjectField}"`);
  }
  const priorEmailsBlock = priorEmailsText.length ? priorEmailsText.join("\n\n") : "N/A";
  const priorSubjectsBlock = priorSubjects.length
    ? `PRIOR SUBJECT LINES (your new subject MUST NOT start with the same first word as any of these, and must be structurally and semantically distinct from all of them):\n${priorSubjects.join('\n')}`
    : "No prior subject lines yet.";

  let companyNewsBlock = null;
  let companyContentBlock = null;
  if (website) {
    try {
      const { newsBlock, blogBlock } = await getCompanyContent(website);
      companyNewsBlock = newsBlock;
      companyContentBlock = blogBlock;
    } catch (err) {
      console.log(`⚠️ Content extraction failed for ${company}: ${err.message}`);
    }
  }

  const painIndex = (SEQUENCE_STEP - 1) % persona.pains.length;
  const primaryPain = persona.pains[painIndex];

  const openingStyleInstruction = stepConfig.openingStyle === 'question'
    ? `OPENING STYLE: The very first line after "${salutation}," MUST be a direct question. Not an observation. A question. Make it specific to ${company} or their industry situation. Questions create engagement. Examples: "Quick question for you." or "How is [specific challenge] showing up for your team right now?" Then follow with your context and value.`
    : `OPENING STYLE: The very first line after "${salutation}," MUST be a specific observation about ${company} or their situation. Not the industry generally. This company specifically.`;

  const openingIntelligence = `
OPENING INTELLIGENCE — USE THIS TO WRITE THE FIRST LINE:

${SEQUENCE_STEP === 1
  ? `STEP 1 EXCEPTION: This email leads with the AEO / buyer visibility hook (see AEO CONTEXT section below) — not company news, not a generic observation. The visibility gap IS the opening. Company news and blog signals should be used to make the hook feel specific to this prospect, but the AEO concept is the first sentence. After the hook (one to two sentences), use company signals to deepen the pain before the CTA.`
  : `Priority order (use the highest-quality signal available):`
}

1. COMPANY NEWS / AWARDS (strongest hook${SEQUENCE_STEP === 1 ? ' — use to personalize the AEO hook, not replace it' : ''}):
${companyNewsBlock || "None found."}

2. COMPANY BLOG / THOUGHT LEADERSHIP:
${companyContentBlock || "None found."}

3. COMPANY DESCRIPTION (use specific details if no news or blog):
${description || "Not provided."}

4. BEHAVIORAL SIGNALS:
${behavioralContext.trim()}

5. INDUSTRY OPENING HOOKS (last resort — if no company-specific signal exists, open with a counterintuitive or second-level observation about this industry, not the most obvious pain):
${persona.openingHooks.map((h, i) => `${i + 1}. ${h}`).join('\n')}

${openingStyleInstruction}
`.trim();

  const ctaInstructions = buildCtaInstructions(stepConfig.ctaType, stepConfig);

  const reEngagementSection = stepConfig.reEngagementNote
    ? `RE-ENGAGEMENT INSTRUCTION:\n${stepConfig.reEngagementNote}`
    : '';

  const aeoRequired = !!stepConfig.aeoContext;

  const AEO_CHECK_PHRASES = [
    'chatgpt', 'perplexity', 'ai overview', 'ai-generated', 'ai generated',
    'answer', 'shortlist', 'asking an ai', 'asking a tool', 'invisible to',
    'showing up', 'show up', 'visible in', 'visibility gap', 'buyer research',
    'ai tools', 'research in ai', 'summarize', 'summarizing'
  ];

  function aeoPresent(text) {
    const lower = text.toLowerCase();
    return AEO_CHECK_PHRASES.some(p => lower.includes(p));
  }

  const avoidPhrasesBlock = stepConfig.avoidPhrases && stepConfig.avoidPhrases.length
    ? `BANNED PHRASES — never use any of these in this email:\n${stepConfig.avoidPhrases.map(p => `- "${p}"`).join('\n')}`
    : '';

  const aeoMandatoryBlock = aeoRequired
    ? `⚠️ MANDATORY REQUIREMENT — READ THIS BEFORE WRITING ANYTHING ELSE ⚠️
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
AEO / BUYER VISIBILITY — THIS MUST APPEAR IN THE EMAIL BODY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
${stepConfig.aeoContext.replace(/\$\{company\}/g, company).replace(/\$\{pageViews\}/g, String(pageViews))}

ENFORCEMENT: The finished email body MUST reference the fact that buyers are now forming vendor opinions inside AI-generated answers (ChatGPT, Perplexity, Google AI Overview, or similar). If this concept is absent from your output, the email has failed and must be rewritten. Do not use the terms "AEO", "AXO", or "answer engine optimization" — describe the phenomenon in plain language.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━`
    : `AEO SUPPRESSED FOR THIS STEP: Do not mention AI search, AEO, AXO, answer engines, or buyer visibility in AI tools anywhere in this email. These topics appear in other steps; repeating them here makes the sequence feel repetitive.`;

  const userContent = `You are Scott Benedetti, Partner and Executive Vice President of The Pedowitz Group (TPG), writing EMAIL ${SEQUENCE_STEP} of 10 in a personalized outbound nurture sequence to ${salutation} at ${company}.

${aeoMandatoryBlock}

The Pedowitz Group (TPG) is THE Revenue Marketing™ consulting firm. 1,300+ client engagements. $25B+ in marketing-sourced revenue generated. TPG's five service pillars:
1. AI SERVICES: AI Roadmap Accelerator, AI Agents and Automation, Marketing Operations Automation, AI-Driven Personalization, Data and Decision Intelligence
2. MARKETING & REVENUE OPERATIONS: Marketing Ops, Revenue Ops, Lead Management
3. STRATEGY: Revenue Marketing Transformation (RM6 Framework), ABM, Customer Experience, Campaign Strategy
4. TECHNOLOGY CONSULTING: HubSpot, Salesforce CRM, Salesforce Marketing Cloud, Oracle Eloqua, Pardot, Adobe Experience Manager
5. MANAGED SERVICES: MarTech Management, Demand Generation, Email Marketing, SEO, AEO

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THIS EMAIL'S SERVICE FOCUS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Pillar: ${stepConfig.pillar}
Service: ${stepConfig.service}
Service URL: ${stepConfig.serviceUrl}
Offer: ${stepConfig.offer}
Offer URL: ${stepConfig.offerUrl}
Revenue Angle: ${stepConfig.angle.replace(/_/g, ' ')}
Core Message: ${stepConfig.talkingPoint}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PROSPECT
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Name: ${firstname}
Title: ${jobtitle}
Company: ${company}
Industry: ${industry}
Employees: ${numemployees || "Unknown"}
Revenue: ${annualrevenue || "Unknown"}
Website: ${website || "Not provided"}
Web Tech: ${web_technologies || "Not listed"}
Intent: ${IntentContext}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
${openingIntelligence}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
INDUSTRY INTELLIGENCE FOR ${persona.label.toUpperCase()}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Primary Pain Point for This Email:
"${primaryPain}"

AI Opportunity for This Vertical:
${persona.aiOpportunity}

Industry Benchmarks (weave one naturally into the prose — never list them):
${persona.stats}

Tone Guidance:
${persona.toneNote}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
PRIOR EMAILS — DO NOT REPEAT ANYTHING FROM THESE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
${priorEmailsBlock}

${priorSubjectsBlock}
${reEngagementSection ? `\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n${reEngagementSection}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━` : ''}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
WRITE THE EMAIL
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FORMAT:
- Subject: 8 words or fewer. No dashes or hyphens.
- Salutation on its own line: "${salutation},"
- One blank line after salutation.
- Body: ${stepConfig.wordCount} words. Short paragraphs separated by ONE blank line.
- No bullets. No signature. No sign-off.
- HTML-safe plain text. <a> tags only for links.
- Zero dashes (— or – or -). Zero underscores. Zero markdown.

VOICE:
- Scott did a little research before writing this. It should feel like it.
- If there is company news, a blog post, a leadership change, or any specific signal available — open with "I saw", "I noticed", or "I came across" to make it feel like Scott actually looked them up.
- After the opening, write like a smart peer talking to another smart peer. Short sentences. Contractions. Real.
- It's fine to start a sentence with "And" or "But" for rhythm.
- Never use the word "agents" or "AI agents" as a standalone term. Instead describe what the technology actually does in plain language: "automated systems that watch for buying signals and route the right follow-up", "workflows that score intent and surface the right contacts before your team has to go looking."
- One industry benchmark stat woven naturally into the prose. Drop it casually. Not prefaced with "According to."
- The CTA should feel like a genuine ask. "Worth 20 minutes?" beats "I'd love to schedule time to discuss."

${avoidPhrasesBlock ? avoidPhrasesBlock + '\n' : ''}
HARD RULES:
- NEVER reuse any idea, framing, or angle from the prior emails above.
- Subject line MUST be structurally and semantically different from all prior subjects. Must not start with the same first word as any prior subject.
- SPECIFICITY TEST: If this email could go to a different company in a different industry with just a name swap, it has FAILED.
- No fabricated company news. No clinical claims for healthcare. No investment advice for financial services.
- Never use the word "AI" in the subject line.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CALL TO ACTION
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
${ctaInstructions}

OUTPUT (exactly this format, nothing else):
Subject: <subject line>

Body:
<email body>`;

  let subject = "";
  let bodyText = "";
  let attempt = 0;

  while (attempt < MAX_SUBJECT_RETRIES && (!subject || !bodyText || bodyText.length < 50 || (aeoRequired && !aeoPresent(bodyText)))) {
    attempt++;
    if (attempt > 1 && aeoRequired && bodyText && !aeoPresent(bodyText)) {
      console.log(`🔁 AEO missing from step ${SEQUENCE_STEP} output (attempt ${attempt}) — retrying with reinforcement`);
    }
    const res = await axios.post(
      "https://api.anthropic.com/v1/messages",
      {
        model: "claude-sonnet-4-20250514",
        max_tokens: 1000,
        temperature: 0.75,
        system: `You are Scott Benedetti, Partner and Executive Vice President of The Pedowitz Group. You did a little homework on this person before writing. You checked their company news, their blog, maybe their LinkedIn. Now you're writing them a short note — the way a smart, confident peer would, not a polished executive. Contractions are fine. Incomplete sentences are fine. Starting with "And" or "But" is fine. When you have a company signal to work from, open with "I saw", "I noticed", or "I came across" to make it feel researched and specific. After the opening, write like you're talking to someone you already know a little. Direct. Warm but not soft. No corporate polish. No sign-off. Never use the word AI in the subject line. When the prompt says a requirement is MANDATORY, treat it as non-negotiable — the email is incomplete without it.`,
        messages: [{ role: "user", content: userContent }]
      },
      {
        headers: {
          "x-api-key": ANTHROPIC_API_KEY,
          "anthropic-version": "2023-06-01",
          "content-type": "application/json"
        },
        timeout: 30000
      }
    );

    const text = res.data?.content?.find(p => p.type === "text")?.text || "";
    const subjectMatch = text.match(/^\s*Subject:\s*(.+)\s*$/mi);
    const bodyMatch = text.match(/^\s*Body:\s*([\s\S]+)$/mi) || text.match(/^\s*Subject:[\s\S]*?\n\n([\s\S]+)$/mi);
    subject = subjectMatch ? subjectMatch[1].trim().replace(/<[^>]+>/g, '') : "";
    bodyText = bodyMatch ? bodyMatch[1].trim() : "";
  }

  if (aeoRequired && !aeoPresent(bodyText)) {
    console.warn(`⚠️ AEO still absent from step ${SEQUENCE_STEP} after ${MAX_SUBJECT_RETRIES} attempts — proceeding anyway`);
  }

  if (!subject || !bodyText || bodyText.length < 50) {
    throw new Error(`Incomplete response after ${MAX_SUBJECT_RETRIES} attempts — subject: "${subject}", body length: ${bodyText.length}`);
  }

  return {
    subject: removeDashes(subject),
    bodyText: sanitizeUrls(removeMarkdown(removeSignature(removeDashes(bodyText))))
  };
}

// =============================
// HUBSPOT WRITE-BACK
// OPTIMIZED: status parameter merged in — eliminates the separate IN_PROGRESS
// and SENT calls, reducing HubSpot PATCH calls per job from 3 to 1.
//
// Properties (confirmed names):
//   industry_ai_nurture_subject_line_em1  through em10  (single-line text)
//   industry_ai_nurture_em1               through em10  (rich text / HTML)
//   industry_ai_nurture_claude_text_em1   through em9   (multi-line text)
//   ai_email_step_status                               (dropdown)
// =============================
async function writeResults(contactId, { subject, bodyText }, sequenceStep = 1, status = 'SENT') {
  const bodyHtml = bodyText
    .replace(/\r\n/g, "\n")
    .split(/\n{2,}/)
    .map(p => `<p style="margin:0 0 16px;">${p.replace(/\n/g, "<br>")}</p>`)
    .join("\n");

  await hubspotPatch(
    `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
    {
      properties: {
        [`industry_ai_nurture_subject_line_em${sequenceStep}`]: subject,
        [`industry_ai_nurture_em${sequenceStep}`]: bodyHtml,
        [`industry_ai_nurture_claude_text_em${sequenceStep}`]: bodyText,
        ai_email_step_status: status
      }
    }
  );
}

// =============================
// STATUS UPDATE
// Called only for permanent failures (FAILED status).
// All other status writes are handled inside writeResults.
// =============================
async function updateStatus(contactId, status) {
  try {
    await hubspotPatch(
      `https://api.hubapi.com/crm/v3/objects/contacts/${contactId}`,
      { properties: { ai_email_step_status: status } }
    );
  } catch (err) {
    console.log(`ℹ️ Status update skipped for ${contactId}: ${err.response?.data?.message || err.message}`);
  }
}

// =============================
// SERVER STARTUP
// =============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 TPG Industry Nurture worker on port ${PORT}`);
  console.log(`⚡ Concurrency: ${CONCURRENCY} | Interval: ${PROCESS_INTERVAL_MS}ms`);
  console.log(`🏭 Industry personas: ${Object.keys(INDUSTRY_PERSONAS).length - 1} verticals + DEFAULT`);
  console.log(`💾 Domain cache: ${domainCache.size} domains loaded`);
  console.log(`📋 Completed contacts: ${completedIds.size} loaded from log`);
});
