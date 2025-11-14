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
import { fileURLToPath } from "url";
import path from "path";
import RegistryAbi from "./abis/Registry.json";
import StakingAbi from "./abis/Staking.json";
import {
  REGISTRY_CONTRACT,
  STAKING_CONTRACT,
  sVVA_CONTRACT,
} from "./constants";
import { Call3, Multicall } from "@evmlord/multicall-sdk";

/* -------------------- ENV / CONSTANTS -------------------- */
const VVA_RPC_URL = process.env.VVA_RPC_URL;
if (!VVA_RPC_URL) {
  throw new Error("Missing VVA_RPC_URL env");
}

const blockChunk = 10000;
const startBlockDefault = 67306437; // https://bscscan.com/block/countdown/67306437
const weekStartUnix = 1762480800; // Nov 6th 9PM EST (Nov 7th 2AM UTC)
const WEEK_SEC = 7 * 24 * 60 * 60;
const MIN_STAKE = parseUnits("200", 9);

const MAX_RETRIES = 3;

const FIRST_CLAIM_QUALIFY = parseUnits("100", 9); // 100 VVA
const SHARES_PER = parseUnits("100", 9); // 100 VVA -> 1 share
const SHARES_CAP = 20;

const ERC20_IFACE = new Interface([
  "function balanceOf(address) view returns (uint256)",
]);

// --- SETUP ---
const provider = new JsonRpcProvider(VVA_RPC_URL);
const registry = new Contract(REGISTRY_CONTRACT, RegistryAbi, provider);
const staking = new Contract(STAKING_CONTRACT, StakingAbi, provider);
const sVVA = new Contract(sVVA_CONTRACT, ERC20_IFACE, provider);

const mc = new Multicall({ provider, chainId: 56 });

/* -------------------- FILES / PATHS -------------------- */
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.join(__dirname, "data");
const weeksDir = path.join(dataDir, "weeks");
const checkpointFile = path.join(dataDir, "checkpoint.json");
const participantsFile = path.join(dataDir, "participants.json");
const sponsorsFile = path.join(dataDir, "sponsors.json");

// --- TYPES ---
type WeekMeta = {
  [userAddrLower: string]: { shares: number };
};
type WeekAggregate = {
  week: number;
  eligibility: string[];
  ineligible: string[];
  meta?: WeekMeta;
  [k: string]: any;
};
type ParticipantWeek = {
  staked: string;
  claimed: string;
  forfeited: string;
  stakes: number;
  claims: number;
  forfeits: number;
  qualifiedForShares?: boolean;
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
  principalNow?: string; // authoritative: set from end-of-chunk block balance
  minPrincipalByWeek?: Record<string, string>; // set ONLY by weekly snapshots/backfills
  byWeek?: Record<string, ParticipantWeek>;
  _teamCounted?: boolean; // internal
};
type SponsorWeek = {
  newMembers: number;
  firstClaimSum: string;
  qualifying?: number;
  shareUnits?: string;
  perDownline?: Record<string, string>;
};
type Sponsor = {
  address: string;
  teamSize: number;
  byWeek: Record<string, SponsorWeek>;
};
type Checkpoint = { lastProcessedBlock: number };

/* -------------------- HELPERS -------------------- */
function toBN(v: string | bigint | number) {
  return toBigInt(v);
}
function addStr(a: string | undefined, b: string) {
  const A = a ? toBN(a) : 0n;
  return (A + toBN(b)).toString();
}
function getWeekIndex(ts: number) {
  if (!weekStartUnix || ts < weekStartUnix) return 0;
  return Math.floor((ts - weekStartUnix) / WEEK_SEC);
}
function weekBoundaryTs(weekIndex: number) {
  return weekStartUnix + weekIndex * WEEK_SEC;
}
function computeCappedSharesFromUnits(unitsStr: string | undefined): number {
  const units = unitsStr ? toBN(unitsStr) : 0n;
  const shares = units / toBN(SHARES_PER);
  const capped = shares > BigInt(SHARES_CAP) ? BigInt(SHARES_CAP) : shares;
  return Number(capped);
}
function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// --- FS HELPERS ---
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
  fs.renameSync(tmp, file);
}
function writeJSON(file: string, data: any) {
  atomicWriteJSON(file, data);
}
function ensureFileSync(file: string, defaultJson: any = {}) {
  ensureDirSync(path.dirname(file));
  if (!fs.existsSync(file))
    fs.writeFileSync(file, JSON.stringify(defaultJson, null, 2), "utf8");
}

/* -------------------- RECORD MUTATORS -------------------- */
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
function initSponsorWeek(sw: SponsorWeek | undefined): SponsorWeek {
  return (
    sw ?? {
      newMembers: 0,
      firstClaimSum: "0",
      qualifying: 0,
      shareUnits: "0",
      perDownline: {},
    }
  );
}
function initParticipantWeek(pw: ParticipantWeek | undefined): ParticipantWeek {
  return (
    pw ?? {
      staked: "0",
      claimed: "0",
      forfeited: "0",
      stakes: 0,
      claims: 0,
      forfeits: 0,
    }
  );
}

/* -------------------- DIFF LOGGER -------------------- */
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

  let teamSizeDelta = 0;
  for (const k of asKeys) {
    const before = beforeSponsors[k]?.teamSize ?? 0;
    const after = afterSponsors[k]?.teamSize ?? 0;
    if (after !== before) teamSizeDelta += after - before;
  }

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

// simple global throttle: at most 1 outstanding getBlock() at a time
let tsQueue: Promise<void> = Promise.resolve();

// async function getTs(blockNumber: number) {
//   const hit = tsCache.get(blockNumber);
//   if (hit) return hit;
//   const blk = await provider.getBlock(blockNumber);
//   const ts = Number(blk!.timestamp);
//   tsCache.set(blockNumber, ts);
//   return ts;
// }

async function getTs(blockNumber: number): Promise<number> {
  const cached = tsCache.get(blockNumber);
  if (cached != null) return cached;

  // serialize calls through a tiny queue to avoid 125/s burst
  const prev = tsQueue;
  let release!: () => void;
  tsQueue = new Promise<void>((r) => (release = r));
  await prev;

  try {
    let attempt = 0;
    let delay = 250; // ms

    // retry loop to handle QuickNode -32007 rate limits gracefully
    // and other transient provider errors
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        const blk = await provider.getBlock(blockNumber);
        if (!blk) throw new Error(`Missing block ${blockNumber}`);
        const ts = Number(blk.timestamp);
        tsCache.set(blockNumber, ts);
        return ts;
      } catch (e: any) {
        const code = e?.error?.code ?? e?.code;
        const msg: string = e?.message || e?.shortMessage || "";

        const isRateLimit =
          code === -32007 ||
          msg.includes("request limit reached") ||
          msg.includes("rate") ||
          msg.includes("Too Many Requests");

        if (isRateLimit && attempt < 5) {
          // backoff and try again
          await sleep(delay);
          delay *= 2;
          attempt++;
          continue;
        }

        // not rate-limit or too many retries → surface the error
        throw e;
      }
    }
  } finally {
    // let next getTs proceed
    release();
  }
}

/* -------------------- MULTICALL HELPERS -------------------- */
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
  for (const c of chunk(calls, 500)) {
    const start = calls.indexOf(c[0]);
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
  for (const c of chunk(calls, 500)) {
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

async function mcBatchGetUserBalance(
  users: string[],
  blockTag: number
): Promise<Record<string, string>> {
  if (!users.length) return {};
  const calls: Call3[] = users.map((u) => ({
    contract: sVVA,
    functionFragment: "balanceOf",
    args: [u],
    allowFailure: true,
  }));
  const out: Record<string, string> = {};
  for (const c of chunk(calls, 500)) {
    const start = calls.indexOf(c[0]);
    // const ret = await mc.aggregate3(c, { blockTag });
    const ret = await safeAggregate3(c, { blockTag });
    ret.forEach((r, i) => {
      const [ok, data] = r;
      const who = users[start + i];
      out[who.toLowerCase()] = ok ? ((data as bigint) ?? 0n).toString() : "0";
    });
  }
  return out;
}

async function safeAggregate3(
  calls: Call3[],
  opts?: { blockTag?: number }
): Promise<ReturnType<typeof mc.aggregate3>> {
  let attempt = 0;
  let delay = 300; // ms

  while (true) {
    try {
      return await mc.aggregate3(calls, opts);
    } catch (e: any) {
      const msg = e?.shortMessage || e?.message || `${e}`;
      const isProviderBusy =
        msg.includes("failed to serve request") ||
        msg.includes("timeout") ||
        msg.includes("rate") ||
        msg.includes("gateway") ||
        msg.includes("CALL_EXCEPTION"); // ethers wraps provider errors like this

      if (!isProviderBusy || attempt >= MAX_RETRIES) {
        // As a last resort: split big batches to bypass payload limits
        if (calls.length > 1) {
          const mid = Math.floor(calls.length / 2);
          const left = await safeAggregate3(calls.slice(0, mid), opts);
          const right = await safeAggregate3(calls.slice(mid), opts);
          // stitch results in original order:
          // Left + Right are arrays of [success, returnData]
          // Return same type the SDK returns
          // @ts-ignore
          return [...left, ...right];
        }
        throw e;
      }

      await new Promise((r) => setTimeout(r, delay));
      attempt++;
      delay *= 2;
    }
  }
}

/* -------------------- SNAPSHOT / LOCATOR -------------------- */
// Fast locator using time-based guess → small galloping bracket → binary search.
// Returns the greatest block with timestamp ≤ ts.
async function findBlockAtOrBefore(ts: number): Promise<number> {
  const latest = await provider.getBlockNumber();
  const latestTs = await getTs(latest);
  const anchor = Math.max(0, startBlockDefault - 1);
  const anchorTs = await getTs(anchor);

  if (ts >= latestTs) return latest;
  if (ts <= anchorTs) return anchor;

  const secPerBlock = 0.75; // heuristic
  const blocksPerSec = 1 / Math.max(1e-6, secPerBlock);

  let cand = Math.round(latest - (latestTs - ts) * blocksPerSec);
  cand = Math.min(Math.max(anchor, cand), latest);

  let tCand = await getTs(cand);
  if (tCand === ts) return cand;

  let lo = anchor;
  let hi = latest;
  let step = Math.max(1, Math.round(60 * blocksPerSec)); // ~80 blocks

  if (tCand <= ts) {
    lo = cand;
    let cur = cand;
    while (cur < latest) {
      const next = Math.min(latest, cur + step);
      const tNext = await getTs(next);
      if (tNext > ts) {
        lo = cur;
        hi = next;
        break;
      }
      cur = next;
      step <<= 1;
    }
    if ((await getTs(hi)) <= ts) return hi;
  } else {
    hi = cand;
    let cur = cand;
    while (cur > anchor) {
      const prev = Math.max(anchor, cur - step);
      const tPrev = await getTs(prev);
      if (tPrev <= ts) {
        lo = prev;
        hi = cur;
        break;
      }
      cur = prev;
      step <<= 1;
    }
    if ((await getTs(lo)) > ts) return lo;
  }

  while (lo + 1 < hi) {
    const mid = Math.floor((lo + hi) / 2);
    const t = await getTs(mid);
    if (t <= ts) lo = mid;
    else hi = mid;
  }
  return lo;
}

async function snapshotBalancesAtBlock(
  users: string[],
  blockTag: number
): Promise<Record<string, string>> {
  if (!users.length) return {};
  const out: Record<string, string> = {};
  for (const usersChunk of chunk(users, 500)) {
    const map = await mcBatchGetUserBalance(usersChunk, blockTag);
    Object.assign(out, map);
  }
  return out;
}

async function writeWeekSnapshot(
  weekIndex: number,
  blockTag: number,
  participants: Record<string, Participant>
) {
  const users = Object.keys(participants);
  if (!users.length) return;

  const balances = await snapshotBalancesAtBlock(users, blockTag);

  for (const u of users) {
    const p = participants[u];
    const bal = balances[u.toLowerCase()] ?? "0";
    p.minPrincipalByWeek ||= {};
    // Canonical snapshot for the week: principal entering the week
    p.minPrincipalByWeek[String(weekIndex)] = bal;
    // NOTE: do NOT touch principalNow here; it is set from end-of-chunk balance.
  }
}

/* -------------------- BALANCE CACHE (PER-EVENT) -------------------- */
// blockTag -> addrLower -> balance
const balanceCache: Record<number, Record<string, string>> = {};

async function prefetchBalances(blockTag: number, addrs: string[]) {
  if (!addrs.length) return;
  const uniq = Array.from(new Set(addrs.map((a) => a.toLowerCase())));
  const res = await snapshotBalancesAtBlock(uniq, blockTag);
  balanceCache[blockTag] ||= {};
  Object.assign(balanceCache[blockTag], res);
}

function getCachedBalance(blockTag: number, addr: string): string | undefined {
  return balanceCache[blockTag]?.[addr.toLowerCase()];
}

/* -------------------- ELIGIBILITY & SHARES -------------------- */
function buildEligibilityForWeeks(
  weeks: number[],
  participants: Record<string, Participant>,
  minStake: bigint,
  sponsors: Record<string, Sponsor>
): Record<
  string,
  { eligible: string[]; ineligible: string[]; meta?: WeekMeta }
> {
  const out: Record<
    string,
    { eligible: string[]; ineligible: string[]; meta?: WeekMeta }
  > = {};
  for (const wk of weeks) {
    const key = String(wk);
    const rec: { eligible: string[]; ineligible: string[]; meta: WeekMeta } = {
      eligible: [],
      ineligible: [],
      meta: {} as WeekMeta,
    };
    for (const [addr, p] of Object.entries(participants)) {
      // Default missing snapshot to "0" so users are not skipped
      const minStr = (p.minPrincipalByWeek || {})[key] || "0";
      const minBN = toBN(minStr);
      const sponsorQual = sponsors[addr]?.byWeek?.[key]?.qualifying ?? 0;

      const units = sponsors[addr]?.byWeek?.[key]?.shareUnits;
      const shares = computeCappedSharesFromUnits(units);

      rec.meta[addr] = { shares };

      const eligibleNow = minBN >= minStake && sponsorQual >= 2 && shares >= 2;

      if (eligibleNow) rec.eligible.push(addr);
      else rec.ineligible.push(addr);
    }
    out[key] = rec;
  }
  return out;
}

/* -------------------- MAIN INDEXER -------------------- */
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
    const firstSeenBlock: Record<string, number> = {};

    /* ------------- FIRST PASS: queue balances per block ------------- */
    const balanceQueueByBlock: Record<number, Set<string>> = {};
    const activeUsers = new Set<string>();

    function queue(block: number, addr: string) {
      (balanceQueueByBlock[block] ||= new Set()).add(addr.toLowerCase());
      activeUsers.add(addr.toLowerCase());
    }

    for (const log of stakedLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      queue(log.blockNumber, user);
    }
    for (const log of claimLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      queue(log.blockNumber, user);
    }
    for (const log of unstakedLogs) {
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      queue(log.blockNumber, user);
    }

    // Prefetch balances for all event blocks (cached per-block in memory)
    for (const [blockStr, set] of Object.entries(balanceQueueByBlock)) {
      const blockTag = Number(blockStr);
      await prefetchBalances(blockTag, Array.from(set));
    }

    /* ------------- REGISTRY: ReferralAnchorCreated ------------- */
    for (const log of refLogs) {
      const ts = await getTs(log.blockNumber);
      const { args } = registry.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (args.user as string).toLowerCase();
      const referee = (args.referee as string).toLowerCase();

      activeUsers.add(user);

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: referee !== ZeroAddress ? referee : null,
          checkInTime: ts,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: undefined,
          minPrincipalByWeek: {},
        };
        firstSeenBlock[user] = firstSeenBlock[user]
          ? Math.min(firstSeenBlock[user], log.blockNumber)
          : log.blockNumber; // new user: bootstrap balance
      } else {
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

      if (!participants[user].checkInTime) needCheckIn.push(user);
    }

    /* ---------------- STAKED ---------------- */
    for (const log of stakedLogs) {
      const ts = await getTs(log.blockNumber);
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();
      // const referrer = (parsed.args.referrer as string).toLowerCase();
      // const claimed = Boolean(parsed.args.claimed);

      const week = getWeekIndex(ts);
      dirtyWeeks.add(week);

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: null, // unknown until registry tells us
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: undefined,
          minPrincipalByWeek: {},
        };
        firstSeenBlock[user] = firstSeenBlock[user]
          ? Math.min(firstSeenBlock[user], log.blockNumber)
          : log.blockNumber; // new user: bootstrap balance

        needReferee.push(user);
      } else if (!participants[user].referee) {
        needReferee.push(user);
      }

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

      // NOTE: DO NOT touch principalNow or minPrincipalByWeek here.
      // If you need event-time balance for debugging:
      // const balAtEvent = getCachedBalance(log.blockNumber, user);
    }

    /* ---------------- CLAIMED ---------------- */
    for (const log of claimLogs) {
      const ts = await getTs(log.blockNumber);
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: undefined,
          minPrincipalByWeek: {},
        };
        firstSeenBlock[user] = firstSeenBlock[user]
          ? Math.min(firstSeenBlock[user], log.blockNumber)
          : log.blockNumber; // new user: bootstrap balance
      }

      const week = getWeekIndex(ts);
      const wkKey = String(week);
      dirtyWeeks.add(week);

      const isFirstEver = !participants[user].firstClaimAt;
      const referee = participants[user].referee || null;

      participants[user].claimedTotal = addStr(
        participants[user].claimedTotal,
        amount
      );
      participants[user].lastClaimAt = ts;
      bumpParticipantWeek(participants[user], week, "claimed", amount);

      participants[user].byWeek = participants[user].byWeek || {};
      participants[user].byWeek[wkKey] = initParticipantWeek(
        participants[user].byWeek[wkKey]
      );

      if (isFirstEver) {
        participants[user].firstClaimAt = ts;
        participants[user].firstClaimAmt = amount;

        // Only count "new members" if they SIGNED UP during the campaign
        const joinedAt = participants[user].checkInTime;
        const isCampaignSignup = joinedAt != null && joinedAt >= weekStartUnix;

        if (!isCampaignSignup) {
          // Old signup: they still have a referee, but they do NOT give the sponsor
          // newMembers/qualifying/shareUnits for the campaign.
          if (!referee) {
            // still try to backfill referee so the tree is correct
            needReferee.push(user);
          }
        } else if (referee && referee !== ZeroAddress) {
          const ref = referee.toLowerCase();
          initSponsorIfMissing(sponsors, ref);
          sponsors[ref].byWeek = sponsors[ref].byWeek || {};
          sponsors[ref].byWeek[wkKey] = initSponsorWeek(
            sponsors[ref].byWeek[wkKey]
          );

          const sw = sponsors[ref].byWeek[wkKey]!;
          sw.newMembers += 1;
          sw.firstClaimSum = addStr(sw.firstClaimSum, amount);

          if (toBN(amount) >= FIRST_CLAIM_QUALIFY) {
            sw.qualifying = (sw.qualifying ?? 0) + 1;
            participants[user].byWeek[wkKey].qualifiedForShares = true;

            const per = sw.perDownline!;
            per[user] = addStr(per[user], amount);
            sw.shareUnits = addStr(sw.shareUnits, amount);
          }
        } else {
          needReferee.push(user);
        }
      } else {
        if (referee && referee !== ZeroAddress) {
          const wasQualified =
            !!participants[user].byWeek[wkKey]?.qualifiedForShares;
          if (wasQualified) {
            const ref = referee.toLowerCase();
            initSponsorIfMissing(sponsors, ref);
            sponsors[ref].byWeek = sponsors[ref].byWeek || {};
            sponsors[ref].byWeek[wkKey] = initSponsorWeek(
              sponsors[ref].byWeek[wkKey]
            );

            const sw = sponsors[ref].byWeek[wkKey]!;
            const per = sw.perDownline!;
            per[user] = addStr(per[user], amount);
            sw.shareUnits = addStr(sw.shareUnits, amount);
          }
        }
      }

      // NOTE: DO NOT touch principalNow or minPrincipalByWeek here.
      // If needed: const balAtEvent = getCachedBalance(log.blockNumber, user);
    }

    /* ---------------- FORFEITED ---------------- */
    for (const log of forfeitLogs) {
      const ts = await getTs(log.blockNumber);
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: undefined,
          minPrincipalByWeek: {},
        };
        firstSeenBlock[user] = firstSeenBlock[user]
          ? Math.min(firstSeenBlock[user], log.blockNumber)
          : log.blockNumber; // new user: bootstrap balance
      }

      const week = getWeekIndex(ts);
      dirtyWeeks.add(week);

      participants[user].forfeitedTotal = addStr(
        participants[user].forfeitedTotal,
        amount
      );
      participants[user].lastForfeitAt = ts;
      bumpParticipantWeek(participants[user], week, "forfeited", amount);

      // NOTE: DO NOT touch principalNow/minPrincipalByWeek here.
    }

    /* ---------------- UNSTAKED ---------------- */
    for (const log of unstakedLogs) {
      const ts = await getTs(log.blockNumber);
      const parsed = staking.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (parsed.args.user as string).toLowerCase();
      const amount = (parsed.args.amount as bigint).toString();

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: null,
          checkInTime: null,
          stakedTotal: "0",
          claimedTotal: "0",
          forfeitedTotal: "0",
          principalNow: undefined,
          minPrincipalByWeek: {},
        };
        firstSeenBlock[user] = firstSeenBlock[user]
          ? Math.min(firstSeenBlock[user], log.blockNumber)
          : log.blockNumber; // new user: bootstrap balance
      }

      const week = getWeekIndex(ts);
      dirtyWeeks.add(week);

      // Counters only; principalNow is authoritative from end-of-chunk
      participants[user].lastForfeitAt =
        participants[user].lastForfeitAt ?? undefined;
      bumpParticipantWeek(participants[user], week, "forfeited", "0"); // no change; keep structure

      // Anti-gaming: deduct sponsor shareUnits credited this week if applicable
      const referee = participants[user].referee || null;
      const wk = String(week);
      if (referee && referee !== ZeroAddress) {
        const ref = referee.toLowerCase();
        const sw = sponsors[ref]?.byWeek?.[wk];
        const perDown = sw?.perDownline?.[user];
        if (sw && perDown) {
          const credited = toBN(perDown);
          if (credited > 0n) {
            // We don't know exact claim-vs-unstake ordering inside a block; this is best-effort anti-gaming.
            // Use event balance if you need more advanced heuristics.
            const ded = credited; // cap to credited units for same-week deductions
            sw.perDownline![user] = (toBN(perDown) - ded).toString();
            sw.shareUnits = (toBN(sw.shareUnits ?? "0") - ded).toString();

            // if they were counted as qualified and now fell below threshold,
            // reduce qualifying count
            if (
              credited >= FIRST_CLAIM_QUALIFY &&
              toBN(sw.perDownline![user]) < FIRST_CLAIM_QUALIFY
            ) {
              sw.qualifying = Math.max(0, (sw.qualifying ?? 0) - 1);
            }
          }
        }
      }
    }

    /* ---------------- MULTICALL BACKFILLS ---------------- */
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

    const uniqMissingRefs = [...new Set(needReferee)].filter(
      (u) => !participants[u]?.referee
    );
    const newlyBackfilled: string[] = [];
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
            newlyBackfilled.push(u);
          }
        }
      }
    }

    // Retroactively apply first-claim effects for newly backfilled referees
    for (const u of newlyBackfilled) {
      const p = participants[u];
      if (!p || !p.firstClaimAt || !p.referee) continue;

      const wk = String(getWeekIndex(p.firstClaimAt));
      const ref = p.referee.toLowerCase();

      // Only treat them as a "new member" if they signed up during campaign
      const joinedAt = p.checkInTime;
      const isCampaignSignup = joinedAt != null && joinedAt >= weekStartUnix;

      if (!isCampaignSignup) continue; // pre-campaign signup → no campaign newMember

      p.byWeek = p.byWeek || {};
      p.byWeek[wk] = initParticipantWeek(p.byWeek[wk]);

      sponsors[ref].byWeek = sponsors[ref].byWeek || {};
      sponsors[ref].byWeek[wk] = initSponsorWeek(sponsors[ref].byWeek[wk]);
      const sw = sponsors[ref].byWeek[wk];

      const amt = p.firstClaimAmt || "0";
      sw.newMembers += 1;
      sw.firstClaimSum = addStr(sw.firstClaimSum, amt);

      if (toBN(amt) >= FIRST_CLAIM_QUALIFY) {
        sw.qualifying = (sw.qualifying ?? 0) + 1;
        p.byWeek[wk].qualifiedForShares = true;

        const per = sw.perDownline!;
        per[u] = addStr(per[u], amt);
        sw.shareUnits = addStr(sw.shareUnits, amt);
      }

      dirtyWeeks.add(Number(wk));
    }

    /* ---------------- WEEKLY SNAPSHOTS ---------------- */
    const startTs = await getTs(cursor);
    const endTs = await getTs(toBlock);

    const firstWk = Math.max(0, getWeekIndex(startTs));
    const lastWk = Math.max(0, getWeekIndex(endTs));

    const boundaryWeeks: number[] = [];
    for (let wk = firstWk; wk <= lastWk; wk++) {
      const boundary = weekBoundaryTs(wk);
      if (boundary >= startTs && boundary <= endTs) boundaryWeeks.push(wk);
    }

    for (const wk of boundaryWeeks) {
      const boundaryTs = weekBoundaryTs(wk);
      const boundaryBlock = await findBlockAtOrBefore(boundaryTs);
      console.log(
        `[snapshot] week ${wk} @ block ${boundaryBlock} (ts=${boundaryTs})`
      );
      await writeWeekSnapshot(wk, boundaryBlock, participants);
      dirtyWeeks.add(wk);
    }

    // Backfill snapshots for users first seen in this chunk for all weeks up to lastWk
    if (Object.keys(firstSeenBlock).length) {
      const weeksToBackfill: number[] = [];
      for (let wk = 0; wk <= lastWk; wk++) weeksToBackfill.push(wk);

      const boundaryBlocks: Record<number, number> = {};
      for (const wk of weeksToBackfill) {
        const ts = weekBoundaryTs(wk);
        boundaryBlocks[wk] = await findBlockAtOrBefore(ts);
      }

      const newUsers = Object.keys(firstSeenBlock).map((u) => u.toLowerCase());
      for (const wk of weeksToBackfill) {
        const blockTag = boundaryBlocks[wk];
        console.log(
          `[backfill] week ${wk} for ${newUsers.length} new users @ block ${blockTag}`
        );
        const balances = await snapshotBalancesAtBlock(newUsers, blockTag);
        for (const u of newUsers) {
          const p = participants[u];
          if (!p) continue;
          const bal = balances[u] ?? "0";
          p.minPrincipalByWeek ||= {};
          p.minPrincipalByWeek[String(wk)] = bal;
        }
        dirtyWeeks.add(wk);
      }
    }

    /* ---------------- AUTHORITATIVE principalNow ---------------- */
    if (activeUsers.size) {
      const endBalances = await snapshotBalancesAtBlock(
        Array.from(activeUsers),
        toBlock
      );
      for (const [addr, bal] of Object.entries(endBalances)) {
        const p = participants[addr];
        if (p) p.principalNow = bal || "0";
      }
    }

    /* ---------------- FLUSH STATE FOR THIS CHUNK ---------------- */
    writeJSON(participantsFile, participants);
    writeJSON(sponsorsFile, sponsors);

    if (dirtyWeeks.size > 0) {
      const dirty = Array.from(dirtyWeeks.values()).sort((a, b) => a - b);
      const wkElig = buildEligibilityForWeeks(
        dirty,
        participants,
        MIN_STAKE,
        sponsors
      );
      for (const wk of dirty) {
        const file = path.join(weeksDir, `week_${wk}.json`);
        const existing = readJSON<WeekAggregate>(file, {
          week: wk,
          eligibility: [],
          ineligible: [],
          meta: {},
        });
        const merged: WeekAggregate = {
          ...existing,
          week: wk,
          eligibility: wkElig[String(wk)]?.eligible ?? [],
          ineligible: wkElig[String(wk)]?.ineligible ?? [],
          meta: wkElig[String(wk)]?.meta ?? existing.meta,
        };
        writeJSON(file, merged);
      }
    }

    writeJSON(checkpointFile, { lastProcessedBlock: toBlock });
    console.log(`[ok] processed ${cursor}→${toBlock}`);
    cursor = toBlock + 1;
  }

  /* ---------------- FINAL ELIGIBILITY PASS ---------------- */
  const allWeeksSet = new Set<number>();
  for (const p of Object.values(participants)) {
    const mpbw = p.minPrincipalByWeek || {};
    for (const wk of Object.keys(mpbw)) allWeeksSet.add(Number(wk));
  }
  const allWeeks = Array.from(allWeeksSet.values()).sort((a, b) => a - b);

  const finalElig = buildEligibilityForWeeks(
    allWeeks,
    participants,
    MIN_STAKE,
    sponsors
  );

  for (const wk of allWeeks) {
    const file = path.join(weeksDir, `week_${wk}.json`);
    const existing: WeekAggregate = readJSON<WeekAggregate>(file, {
      week: Number(wk),
      eligibility: [],
      ineligible: [],
      meta: {},
    });
    const computed = finalElig[String(wk)] || {
      eligible: [],
      ineligible: [],
      meta: {},
    };
    const agg: WeekAggregate = {
      ...existing,
      week: Number(wk),
      eligibility: computed.eligible,
      ineligible: computed.ineligible,
      meta: computed.meta ?? existing.meta,
    };
    writeJSON(file, agg);
  }

  writeJSON(participantsFile, participants);
  writeJSON(sponsorsFile, sponsors);

  logDiff(
    beforeParticipants,
    participants,
    beforeSponsors,
    sponsors,
    Object.fromEntries(
      allWeeks.map((wk) => [
        String(wk),
        {
          eligible: finalElig[String(wk)]?.eligible ?? [],
          ineligible: finalElig[String(wk)]?.ineligible ?? [],
        },
      ])
    )
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
