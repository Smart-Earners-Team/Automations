import "dotenv/config";
import {
  Contract,
  Interface,
  JsonRpcProvider,
  parseUnits,
  toBigInt,
  ZeroAddress,
} from "ethers";
import fs from "fs";
import path from "path";
import RegistryAbi from "./abis/Registry.json";
import StakingAbi from "./abis/Staking.json";
import { REGISTRY_CONTRACT, STAKING_CONTRACT } from "./constants";
import { Call3, Multicall } from "@evmlord/multicall-sdk";

// --- SETUP ---
const provider = new JsonRpcProvider(process.env.VVA_RPC_URL);
const registry = new Contract(REGISTRY_CONTRACT, RegistryAbi, provider);
const staking = new Contract(STAKING_CONTRACT, StakingAbi, provider);

const mc = new Multicall({ provider, chainId: 56 });

const dataDir = path.join(__dirname, "data");
const weeksDir = path.join(dataDir, "weeks");
const checkpointFile = path.join(dataDir, "checkpoint.json");
const participantsFile = path.join(dataDir, "participants.json");
const sponsorsFile = path.join(dataDir, "sponsors.json");

const blockChunk = 10000;
const startBlockDefault = 64582380; // 0x59fda2d8207e1bf3e16991878daf739302021489bda215a5f0d0930c6f20c926 demo
const weekStartUnix = 1760313600; // October 13 demo
const MIN_STAKE = parseUnits("200", 9);

// --- TYPES ---
type WeekAggregate = {
  week: number;
  eligibility: string[];
  ineligible: string[];
  // we may have other leaderboard fields here; keep them optional
  [k: string]: any;
};

type Participant = {
  address: string;
  referee: string | null;
  checkInTime: number | null;
  stakedTotal: string;
  claimedTotal: string;
  forfeitedTotal: string;
  firstStakeAt?: number;
  firstStakeAmt?: string;
  firstClaimAt?: number;
  firstClaimAmt?: string;
  lastStakeAt?: number;
  lastClaimAt?: number;
  lastForfeitAt?: number;
  principalNow?: string;
  minPrincipalByWeek?: Record<string, string>;
  byWeek?: Record<
    string,
    {
      staked: string;
      claimed: string;
      forfeited: string;
      stakes: number;
      claims: number;
      forfeits: number;
    }
  >;
  // internal non-persisted marker to avoid double teamSize bumps
  _teamCounted?: boolean;
};
type Sponsor = {
  address: string;
  teamSize: number;
  byWeek: Record<string, { newMembers: number; firstClaimSum: string }>;
};
type Checkpoint = { lastProcessedBlock: number };

// --- HELPERS ---
function toBN(v: string | bigint | number) {
  return toBigInt(v);
}
function addStr(a: string | undefined, b: string) {
  const A = a ? toBN(a) : 0n;
  return (A + toBN(b)).toString();
}
function subFloor(a: string, b: string) {
  const A = toBN(a || "0");
  const B = toBN(b);
  return A > B ? (A - B).toString() : "0";
}
function getWeekIndex(ts: number) {
  if (!weekStartUnix || ts < weekStartUnix) return 0;
  return Math.floor((ts - weekStartUnix) / (7 * 24 * 60 * 60));
}

// --- FS HELPERS (native) ---
function ensureDirSync(dir: string) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}
function readJSON<T>(file: string, fallback: T): T {
  try {
    const raw = fs.readFileSync(file, "utf8");
    return JSON.parse(raw) as T;
  } catch {
    return fallback;
  }
}

// --- ATOMIC WRITE HELPERS (native fs) ---
function atomicWriteJSON(file: string, data: any) {
  ensureDirSync(path.dirname(file));
  const tmp = `${file}.tmp-${Date.now()}-${Math.random()
    .toString(16)
    .slice(2)}`;
  const buf = Buffer.from(JSON.stringify(data, null, 2), "utf8");
  const fd = fs.openSync(tmp, "w");
  try {
    fs.writeFileSync(fd, buf);
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(tmp, file); // atomic replace
}

function writeJSON(file: string, data: any) {
  atomicWriteJSON(file, data);
}

function ensureFileSync(file: string, defaultJson: any = {}) {
  ensureDirSync(path.dirname(file));
  if (!fs.existsSync(file))
    fs.writeFileSync(file, JSON.stringify(defaultJson, null, 2), "utf8");
}

// --- INITIALIZATION ---
function bumpParticipantWeek(
  p: Participant,
  week: number,
  kind: "staked" | "claimed" | "forfeited",
  amount: string
) {
  if (!p.byWeek) p.byWeek = {};
  const key = String(week);
  if (!p.byWeek[key])
    p.byWeek[key] = {
      staked: "0",
      claimed: "0",
      forfeited: "0",
      stakes: 0,
      claims: 0,
      forfeits: 0,
    };
  const rec = p.byWeek[key];
  rec[kind] = addStr(rec[kind], amount);
  if (kind === "staked") rec.stakes++;
  if (kind === "claimed") rec.claims++;
  if (kind === "forfeited") rec.forfeits++;
}
function initSponsorIfMissing(s: Record<string, Sponsor>, addr: string) {
  if (!s[addr]) s[addr] = { address: addr, teamSize: 0, byWeek: {} };
}
function bumpSponsorWeekFirstClaim(s: Sponsor, week: number, amount: string) {
  const key = String(week);
  if (!s.byWeek[key]) s.byWeek[key] = { newMembers: 0, firstClaimSum: "0" };
  s.byWeek[key].newMembers++;
  s.byWeek[key].firstClaimSum = addStr(s.byWeek[key].firstClaimSum, amount);
}

// --- DIFF LOGGER (lightweight) ---
function snapshotKeys(obj: Record<string, any>): Set<string> {
  return new Set(Object.keys(obj || {}));
}
function shallowClone<T extends object>(o: T): T {
  return JSON.parse(JSON.stringify(o));
}
function logDiff(
  beforeParticipants: Record<string, Participant>,
  afterParticipants: Record<string, Participant>,
  beforeSponsors: Record<string, Sponsor>,
  afterSponsors: Record<string, Sponsor>,
  weeklyEligibility: Record<
    string,
    { eligible: string[]; ineligible: string[] }
  >
) {
  const bpKeys = snapshotKeys(beforeParticipants);
  const apKeys = snapshotKeys(afterParticipants);
  const newParticipants = [...apKeys].filter((k) => !bpKeys.has(k)).length;

  const bsKeys = snapshotKeys(beforeSponsors);
  const asKeys = snapshotKeys(afterSponsors);
  const newSponsors = [...asKeys].filter((k) => !bsKeys.has(k)).length;

  // team size deltas (sum of absolute changes)
  let teamSizeDelta = 0;
  for (const k of asKeys) {
    const before = beforeSponsors[k]?.teamSize ?? 0;
    const after = afterSponsors[k]?.teamSize ?? 0;
    if (after !== before) teamSizeDelta += after - before;
  }

  // weekly eligibility counts
  const weeks = Object.keys(weeklyEligibility).sort(
    (a, b) => Number(a) - Number(b)
  );
  const summary = weeks.map((wk) => ({
    week: Number(wk),
    eligible: weeklyEligibility[wk].eligible.length,
    ineligible: weeklyEligibility[wk].ineligible.length,
  }));

  console.log(
    `[diff] +${newParticipants} participants, +${newSponsors} sponsors, teamSizeΔ=${teamSizeDelta}`
  );
  if (summary.length) {
    console.log(`[diff] weekly eligibility:`);
    for (const s of summary) {
      console.log(
        `  - week ${s.week}: eligible=${s.eligible}, ineligible=${s.ineligible}`
      );
    }
  }
}

/* -------------------- BLOCK TS CACHE -------------------- */
const tsCache = new Map<number, number>();
async function getTs(blockNumber: number) {
  const hit = tsCache.get(blockNumber);
  if (hit) return hit;
  const blk = await provider.getBlock(blockNumber);
  const ts = Number(blk!.timestamp);
  tsCache.set(blockNumber, ts);
  return ts;
}

/* -------------------- MULTICALL BATCH READS -------------------- */
// chunk helper to avoid oversized calldata
function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

async function mcBatchCheckInTime(
  users: string[]
): Promise<Record<string, number>> {
  if (!users.length) return {};
  const calls: Call3[] = users.map((u) => ({
    contract: registry,
    functionFragment: "checkInTime",
    args: [u],
    allowFailure: true,
  }));

  const out: Record<string, number> = {};
  for (const c of chunk(calls, 1000)) {
    const start = calls.indexOf(c[0]); // start index within full list
    const ret = await mc.aggregate3(c);

    ret.forEach((data, i) => {
      const [checkInOk, checkInData] = data;

      const v = checkInOk ? (checkInData as bigint) : 0n;
      const who = users[start + i];
      out[who.toLowerCase()] = Number(v);
    });
  }
  return out;
}

async function mcBatchGetReferee(
  users: string[]
): Promise<Record<string, string>> {
  if (!users.length) return {};
  const calls: Call3[] = users.map((u) => ({
    contract: registry,
    functionFragment: "getReferee",
    args: [u],
    allowFailure: true,
  }));
  const out: Record<string, string> = {};
  for (const c of chunk(calls, 1000)) {
    const start = calls.indexOf(c[0]);
    const ret = await mc.aggregate3(c);

    ret.forEach((data, i) => {
      const [refOk, refData] = data;
      const ref = refOk ? (refData as string) : ZeroAddress;
      const who = users[start + i];
      out[who.toLowerCase()] = ref.toLowerCase();
    });
  }
  return out;
}

function buildEligibilityForWeeks(
  weeks: number[],
  participants: Record<string, Participant>,
  minStake: bigint
): Record<string, { eligible: string[]; ineligible: string[] }> {
  const out: Record<string, { eligible: string[]; ineligible: string[] }> = {};
  for (const wk of weeks) {
    const key = String(wk);
    const rec = { eligible: [] as string[], ineligible: [] as string[] };
    for (const [addr, p] of Object.entries(participants)) {
      const mpbw = p.minPrincipalByWeek || {};
      const minStr = mpbw[key];
      if (!minStr) continue; // user had no activity for that week yet
      const minBN = toBN(minStr);
      if (minBN >= minStake) rec.eligible.push(addr);
      else rec.ineligible.push(addr);
    }
    out[key] = rec;
  }
  return out;
}

// --- MAIN INDEXER ---
async function main() {
  ensureDirSync(dataDir);
  ensureDirSync(weeksDir);
  ensureFileSync(checkpointFile, { lastProcessedBlock: startBlockDefault - 1 });
  ensureFileSync(participantsFile, {});
  ensureFileSync(sponsorsFile, {});

  const cp: Checkpoint = readJSON(checkpointFile, {
    lastProcessedBlock: startBlockDefault - 1,
  });
  const latestBlock = await provider.getBlockNumber();

  // Always advance one block past the last processed boundary
  let cursor = Math.max(
    startBlockDefault,
    (cp.lastProcessedBlock ?? startBlockDefault - 1) + 1
  );

  if (cursor >= latestBlock) {
    console.log(`[ok] nothing new`);
    return;
  }

  const participants = readJSON<Record<string, Participant>>(
    participantsFile,
    {}
  );
  const sponsors = readJSON<Record<string, Sponsor>>(sponsorsFile, {});

  // snapshots for diff logger
  const beforeParticipants = shallowClone(participants);
  const beforeSponsors = shallowClone(sponsors);

  const topicReferralAnchor = registry.interface.getEvent(
    "ReferralAnchorCreated"
  )!.topicHash;
  const topicStaked = staking.interface.getEvent("Staked")!.topicHash;
  const topicClaimed = staking.interface.getEvent("Claimed")!.topicHash;
  const topicForfeited = staking.interface.getEvent("Forfeited")!.topicHash;
  const topicUnstaked = staking.interface.getEvent("Unstaked")!.topicHash;

  while (cursor <= latestBlock) {
    const toBlock = Math.min(cursor + blockChunk - 1, latestBlock);
    console.log(`Processing ${cursor} → ${toBlock}`);

    const dirtyWeeks = new Set<number>();

    // ---------------- fetch logs in parallel ----------------
    const [refLogs, stakedLogs, claimLogs, forfeitLogs, unstakedLogs] =
      await Promise.all([
        provider.getLogs({
          address: REGISTRY_CONTRACT,
          fromBlock: cursor,
          toBlock,
          topics: [topicReferralAnchor],
        }),
        provider.getLogs({
          address: STAKING_CONTRACT,
          fromBlock: cursor,
          toBlock,
          topics: [topicStaked],
        }),
        provider.getLogs({
          address: STAKING_CONTRACT,
          fromBlock: cursor,
          toBlock,
          topics: [topicClaimed],
        }),
        provider.getLogs({
          address: STAKING_CONTRACT,
          fromBlock: cursor,
          toBlock,
          topics: [topicForfeited],
        }),
        provider.getLogs({
          address: STAKING_CONTRACT,
          fromBlock: cursor,
          toBlock,
          topics: [topicUnstaked],
        }),
      ]);

    console.log(
      `[logs] ref=${refLogs.length}, staked=${stakedLogs.length}, claimed=${claimLogs.length}, forfeited=${forfeitLogs.length}, unstaked=${unstakedLogs.length}`
    );

    // queues for multicall
    const needCheckIn: string[] = [];
    const needReferee: string[] = [];

    // ---------------- REGISTRY: ReferralAnchorCreated ----------------
    for (const log of refLogs) {
      const { args } = registry.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (args.user as string).toLowerCase();
      const referee = (args.referee as string).toLowerCase();

      const ts = await getTs(log.blockNumber); // we can derive check-in from block ts

      if (!participants[user])
        participants[user] = {
          address: user,
          referee: referee !== ZeroAddress ? referee : null,
          checkInTime: ts, // set from event block
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: "0",
          minPrincipalByWeek: {},
        };
      else {
        if (!participants[user].checkInTime)
          participants[user].checkInTime = ts;
        participants[user].referee =
          referee !== ZeroAddress ? referee : participants[user].referee;
      }

      if (referee && referee !== ZeroAddress) {
        initSponsorIfMissing(sponsors, referee);
        if (!participants[user]._teamCounted) {
          sponsors[referee].teamSize++;
          participants[user]._teamCounted = true;
        }
      }

      // if we still want to confirm on-chain time via multicall (optional):
      if (!participants[user].checkInTime) needCheckIn.push(user);
    }

    // ---------------- STAKED ----------------
    for (const log of stakedLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();
      const referrer = (parsed.args.referrer as string).toLowerCase();
      const claimed = Boolean(parsed.args.claimed);

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: referrer !== ZeroAddress ? referrer : null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: "0",
          minPrincipalByWeek: {},
        };
      } else if (!participants[user].referee && referrer !== ZeroAddress) {
        participants[user].referee = referrer;
      } else if (!participants[user].referee && referrer === ZeroAddress) {
        // we’ll try to backfill via multicall
        needReferee.push(user);
      }

      const ts = await getTs(log.blockNumber);
      const week = getWeekIndex(ts);

      dirtyWeeks.add(week);

      participants[user].stakedTotal = addStr(
        participants[user].stakedTotal,
        amount
      );
      participants[user].lastStakeAt = ts;
      if (!participants[user].firstStakeAt) {
        participants[user].firstStakeAt = ts;
        participants[user].firstStakeAmt = amount;
      }
      bumpParticipantWeek(participants[user], week, "staked", amount);

      if (claimed) {
        participants[user].principalNow = addStr(
          participants[user].principalNow,
          amount
        );
        const wk = String(week);
        const nowBN = toBN(participants[user].principalNow!);
        const prev = participants[user].minPrincipalByWeek![wk];
        participants[user].minPrincipalByWeek![wk] = !prev
          ? nowBN.toString()
          : (nowBN < toBN(prev) ? nowBN : toBN(prev)).toString();
      }
    }

    // ---------------- CLAIMED ----------------
    for (const log of claimLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();
      if (!participants[user])
        participants[user] = {
          address: user,
          referee: null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: "0",
          minPrincipalByWeek: {},
        };

      const ts = await getTs(log.blockNumber);
      const week = getWeekIndex(ts);

      dirtyWeeks.add(week);

      participants[user].claimedTotal = addStr(
        participants[user].claimedTotal,
        amount
      );
      participants[user].lastClaimAt = ts;
      if (!participants[user].firstClaimAt) {
        participants[user].firstClaimAt = ts;
        participants[user].firstClaimAmt = amount;

        const referee =
          participants[user].referee ||
          null; /* keep as-is; we might fill later via multicall */
        if (referee && referee !== ZeroAddress) {
          const ref = referee.toLowerCase();
          initSponsorIfMissing(sponsors, ref);
          bumpSponsorWeekFirstClaim(sponsors[ref], week, amount);
        } else {
          // we can also try to backfill later:
          needReferee.push(user);
        }
      }
      bumpParticipantWeek(participants[user], week, "claimed", amount);

      // principal increases on claim
      participants[user].principalNow = addStr(
        participants[user].principalNow,
        amount
      );
      const wk = String(week);
      const nowBN = toBN(participants[user].principalNow!);
      const prev = participants[user].minPrincipalByWeek![wk];
      participants[user].minPrincipalByWeek![wk] = !prev
        ? nowBN.toString()
        : (nowBN < toBN(prev) ? nowBN : toBN(prev)).toString();
    }

    // ---------------- FORFEITED ----------------
    for (const log of forfeitLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();
      if (!participants[user])
        participants[user] = {
          address: user,
          referee: null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: "0",
          minPrincipalByWeek: {},
        };

      const ts = await getTs(log.blockNumber);
      const week = getWeekIndex(ts);

      dirtyWeeks.add(week);

      participants[user].forfeitedTotal = addStr(
        participants[user].forfeitedTotal,
        amount
      );

      participants[user].lastForfeitAt = ts;
      bumpParticipantWeek(participants[user], week, "forfeited", amount);
    }

    // ---------------- UNSTAKED ----------------
    for (const log of unstakedLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();
      if (!participants[user])
        participants[user] = {
          address: user,
          referee: null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: "0",
          minPrincipalByWeek: {},
        };

      const ts = await getTs(log.blockNumber);
      const week = getWeekIndex(ts);

      dirtyWeeks.add(week);

      participants[user].principalNow = subFloor(
        participants[user].principalNow!,
        amount
      );
      const wk = String(week);
      const cur = toBN(
        participants[user].minPrincipalByWeek![wk] ||
          participants[user].principalNow!
      );
      const now = toBN(participants[user].principalNow!);
      participants[user].minPrincipalByWeek![wk] =
        cur === 0n || now < cur ? now.toString() : cur.toString();
    }

    // ---------------- MULTICALL BACKFILLS (optional but faster than per-call) ----------------

    // fill missing check-in times
    const uniqMissingCheckIn = [...new Set(needCheckIn)].filter(
      (u) => participants[u]?.checkInTime == null
    );
    if (uniqMissingCheckIn.length) {
      const map = await mcBatchCheckInTime(uniqMissingCheckIn);
      for (const [u, t] of Object.entries(map)) {
        if (participants[u] && !participants[u].checkInTime)
          participants[u].checkInTime = t || null;
      }
    }

    // 2) fill missing referees
    const uniqMissingRefs = [...new Set(needReferee)].filter(
      (u) => !participants[u]?.referee
    );
    if (uniqMissingRefs.length) {
      const map = await mcBatchGetReferee(uniqMissingRefs);
      for (const [u, r] of Object.entries(map)) {
        if (participants[u] && !participants[u].referee) {
          participants[u].referee =
            r && r !== ZeroAddress ? r.toLowerCase() : null;
          if (participants[u].referee) {
            initSponsorIfMissing(sponsors, participants[u].referee!);
            if (!participants[u]._teamCounted) {
              sponsors[participants[u].referee!].teamSize++;
              participants[u]._teamCounted = true;
            }
          }
        }
      }
    }

    // ---------------- FLUSH STATE FOR THIS CHUNK ----------------
    // Persist participants & sponsors first (atomic)
    writeJSON(participantsFile, participants);
    writeJSON(sponsorsFile, sponsors);

    // Persist affected week aggregates (atomic)
    if (dirtyWeeks.size > 0) {
      const dirty = Array.from(dirtyWeeks.values()).sort((a, b) => a - b);
      const wkElig = buildEligibilityForWeeks(dirty, participants, MIN_STAKE);
      for (const wk of dirty) {
        const file = path.join(weeksDir, `week_${wk}.json`);
        // merge with existing if needed (keep extra fields)
        const existing = readJSON<WeekAggregate>(file, {
          week: wk,
          eligibility: [],
          ineligible: [],
        });
        const merged: WeekAggregate = {
          ...existing,
          week: wk,
          eligibility: wkElig[String(wk)]?.eligible ?? [],
          ineligible: wkElig[String(wk)]?.ineligible ?? [],
        };
        writeJSON(file, merged);
      }
    }

    // checkpoint
    writeJSON(checkpointFile, { lastProcessedBlock: toBlock });
    console.log(`[ok] processed ${cursor}→${toBlock}`);
    cursor = toBlock + 1;
  }

  // ---------------- ELIGIBILITY ----------------
  const weeklyEligibility: Record<
    string,
    { eligible: string[]; ineligible: string[] }
  > = {};

  for (const [addr, p] of Object.entries(participants)) {
    const mpbw = p.minPrincipalByWeek || {};
    for (const [wk, minStr] of Object.entries(mpbw)) {
      const minBN = toBN(minStr || "0");
      const rec = weeklyEligibility[wk] || { eligible: [], ineligible: [] };
      if (minBN >= MIN_STAKE) rec.eligible.push(addr);
      else rec.ineligible.push(addr);
      weeklyEligibility[wk] = rec;
    }
  }

  for (const wk of Object.keys(weeklyEligibility)) {
    const file = path.join(weeksDir, `week_${wk}.json`);
    // initialize with typed default to fix TS errors
    const existing: WeekAggregate = readJSON<WeekAggregate>(file, {
      week: Number(wk),
      eligibility: [],
      ineligible: [],
    });
    const agg: WeekAggregate = {
      ...existing,
      week: Number(wk),
      eligibility: weeklyEligibility[wk].eligible,
      ineligible: weeklyEligibility[wk].ineligible,
    };
    writeJSON(file, agg);
  }

  writeJSON(participantsFile, participants);
  writeJSON(sponsorsFile, sponsors);

  // --- DIFF LOG ---
  logDiff(
    beforeParticipants,
    participants,
    beforeSponsors,
    sponsors,
    weeklyEligibility
  );

  console.log(
    `[done] participants=${Object.keys(participants).length}, sponsors=${
      Object.keys(sponsors).length
    }`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
