// Auto-moderation for text: spots slurs, sexual content, threats and abuse (English + common Hinglish),
// including simple disguises like "f.u.c.k", "sh1t" or "fuuuck".
// severe = block the message and flag the user; mild = mask the word and let the message through.

const SEVERE_WORDS = [
  // slurs
  "nigger", "nigga", "niggers", "niggas", "faggot", "faggots", "fag", "fags", "kike", "kikes", "chink", "chinks", "spic", "spics",
  "tranny", "trannies", "paki", "pakis", "raghead", "wetback", "coon", "coons", "dyke",
  // sexual
  "porn", "porno", "pornhub", "xvideos", "xxx", "pussy", "pussies", "blowjob", "handjob", "cumshot", "nudes",
  "horny", "boobs", "tits", "titties", "onlyfans", "sexting", "dickpic", "cunt", "cunts", "slut", "sluts", "whore", "whores",
  // Hinglish abuse
  "madarchod", "maderchod", "madharchod", "behenchod", "bhenchod", "behnchod", "bhosdike", "bhosdi", "bhosda", "bhosdiwale",
  "randi", "randwa", "chutmarike", "lund", "lauda", "lavda", "lodu", "chodu"
];
const SEVERE_PHRASES = [
  "kill yourself", "kill urself", "kys", "go die", "hope you die", "i will kill you", "ill kill you", "i will rape", "rape you",
  "child porn", "send nudes", "send nude", "nude pics", "show me your boobs", "show your boobs", "dick pic", "sex chat", "suck my dick",
  "suck my cock", "mc bc", "teri maa ki", "teri behen ki", "maa ki chut", "maa chuda", "behen ki chut"
];
const MILD_WORDS = [
  "fuck", "fucks", "fucked", "fucker", "fuckers", "fucking", "fuckin", "motherfucker", "motherfuckers", "fck", "fuk", "wtf", "stfu",
  "shit", "shits", "shitty", "bullshit", "bitch", "bitches", "bitchy", "bastard", "bastards", "asshole", "assholes",
  "dickhead", "prick", "wanker", "retard", "retarded", "douche", "douchebag", "jackass", "dumbass",
  "chutiya", "chutiye", "chutia", "gandu", "harami", "kamina", "kamine", "kutiya", "bsdk", "bkl"
];

const LEET = { "0": "o", "1": "i", "3": "e", "4": "a", "5": "s", "7": "t", "8": "b", "@": "a", "$": "s", "!": "i", "|": "i", "+": "t", "€": "e" };
// one token -> plain letters: lower case, no accents, leetspeak undone, 3+ repeats cut to 2 ("fuuuck" -> "fuuck")
const norm = s => String(s).toLowerCase().normalize("NFKD").replace(/[̀-ͯ]/g, "")
  .replace(/[0-9@$!|+€]/g, c => LEET[c] || c).replace(/[^a-z]/g, "").replace(/(.)\1\1+/g, "$1$1");
const single = s => s.replace(/(.)\1+/g, "$1");   // "fuuck" -> "fuck"
// A list word is matched exactly, or loosely (all repeats collapsed) only when it has no double letters itself.
// That way "fuuuck" and "shiit" are caught, but "Bob's" never matches "boobs" and "con" never matches "coon".
function makeSet(list){
  const exact = new Set(), loose = new Set();
  for (const w of list){ const n = norm(w); exact.add(n); if (single(n) === n) loose.add(n); }
  return t => exact.has(t) || loose.has(single(t));
}
const isSevere = makeSet(SEVERE_WORDS), isMild = makeSet(MILD_WORDS);
const PHRASES = SEVERE_PHRASES.map(p => " " + p.split(/\s+/).map(norm).join(" ") + " ");

// the whole text as normalized words, with spelled-out words ("f u c k", "f.u.c.k") joined back up
function wordsOf(text){
  const toks = String(text).split(/[\s.,;:_\-*~'"`/\\()[\]{}<>?]+/).filter(Boolean).map(norm).filter(Boolean);
  const out = [];
  for (let i = 0; i < toks.length; i++){
    if (toks[i].length === 1){ let j = i, w = ""; while (j < toks.length && toks[j].length === 1){ w += toks[j]; j++; } if (j - i >= 3){ out.push(w); i = j - 1; continue; } }
    out.push(toks[i]);
  }
  return out;
}

export function checkText(text){
  const s = String(text || "");
  if (!s.trim()) return { severe: false, mild: false, clean: s, hits: [] };
  const words = wordsOf(s), line = " " + words.join(" ") + " ", hits = [];
  for (const w of words) if (isSevere(w)) hits.push(w);
  for (const p of PHRASES) if (line.includes(p)) hits.push(p.trim());
  let mild = false;
  for (const w of words) if (isMild(w)) mild = true;
  // mask mild words in place, keeping the rest of the message exactly as written
  const clean = s.replace(/\S+/g, tok => {
    const core = tok.replace(/^[^\p{L}\p{N}@$!]+|[^\p{L}\p{N}@$!]+$/gu, ""), n = norm(core);
    if (n && isMild(n)) return tok.replace(core, core[0] + "*".repeat(Math.max(2, core.length - 1)));
    return tok;
  });
  return { severe: hits.length > 0, mild, clean, hits };
}
