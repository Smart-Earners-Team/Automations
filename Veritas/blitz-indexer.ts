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
const startBlockDefault = 64582380; // 0x59fda2d8207e1bf3e16991878daf739302021489bda215a5f0d0930c6f20c926 demo
const weekStartUnix = 1760313600; // October 13 demo
const MIN_STAKE = parseUnits("200", 9);

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
  // extendable per-user bag; we store shares now
  [userAddrLower: string]: { shares: number };
};
type WeekAggregate = {
  week: number;
  eligibility: string[];
  ineligible: string[];
  meta?: WeekMeta;
  // we may have other leaderboard fields here; keep them optional
  [k: string]: any;
};
type ParticipantWeek = {
  staked: string;
  claimed: string;
  forfeited: string;
  stakes: number;
  claims: number;
  forfeits: number;
  qualifiedForShares?: boolean; // downline hit first ≥100 claim this week
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
  byWeek?: Record<string, ParticipantWeek>;
  // internal non-persisted marker to avoid double teamSize bumps
  _teamCounted?: boolean;
};
type SponsorWeek = {
  newMembers: number;
  firstClaimSum: string;
  qualifying?: number; // # of new members with first claim >= 100 VVA in this week
  shareUnits?: string; // total raw units added this week from qualified downlines (1e9)
  perDownline?: Record<string, string>; // downlineAddr → raw units (1e9)
};
type Sponsor = {
  address: string;
  teamSize: number;
  byWeek: Record<string, SponsorWeek>;
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
function computeCappedSharesFromUnits(unitsStr: string | undefined): number {
  const units = unitsStr ? toBN(unitsStr) : 0n; // raw 1e9
  const shares = units / toBN(SHARES_PER); // bigint floor
  const capped = shares > BigInt(SHARES_CAP) ? BigInt(SHARES_CAP) : shares;
  return Number(capped);
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

// Use sVVA.balanceOf(user) to bootstrap pre-campaign principal
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
  for (const c of chunk(calls, 1000)) {
    const start = calls.indexOf(c[0]);
    const ret = await mc.aggregate3(c, { blockTag });
    ret.forEach((r, i) => {
      const [ok, data] = r;
      const who = users[start + i];
      out[who.toLowerCase()] = ok ? ((data as bigint) ?? 0n).toString() : "0";
    });
  }
  return out;
}

/* -------------------- ELIGIBILITY & SHARES BUILD -------------------- */
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
      const minStr = (p.minPrincipalByWeek || {})[key];
      if (!minStr) continue; // user had no activity for that week yet

      const minBN = toBN(minStr);
      const sponsorQual = sponsors[addr]?.byWeek?.[key]?.qualifying ?? 0;

      // must have 200 sVVA + 2 qualified new members
      const eligibleNow = minBN >= minStake && sponsorQual >= 2;

      // shares
      const units = sponsors[addr]?.byWeek?.[key]?.shareUnits;
      const shares = computeCappedSharesFromUnits(units);

      rec.meta[addr] = { shares };

      if (eligibleNow) rec.eligible.push(addr);
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

  // Bootstrap pre-existing principal for already known participants (first run / safety)
  const bootstrapUsers = Object.keys(participants).filter(
    (u) => !participants[u].principalNow
  );

  if (bootstrapUsers.length) {
    console.log(`[bootstrap] fetching balances for ${bootstrapUsers.length}`);
    const balances = await mcBatchGetUserBalance(
      bootstrapUsers,
      Math.max(0, startBlockDefault - 1)
    );
    for (const [u, bal] of Object.entries(balances)) {
      participants[u].principalNow = bal;
      // seed week 0 minPrincipal (pre-campaign stake may qualify immediately)
      const min0 = toBN(bal) >= MIN_STAKE ? bal : "0";
      participants[u].minPrincipalByWeek =
        participants[u].minPrincipalByWeek || {};
      if (!participants[u].minPrincipalByWeek!["0"]) {
        participants[u].minPrincipalByWeek!["0"] = min0;
      }
    }
    writeJSON(participantsFile, participants); // persist bootstrap
  }

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
    const firstSeenBlock: Record<string, number> = {}; // pick the earliest seen block per user
    // const needBalanceBootstrap: string[] = [];

    /* ------------- REGISTRY: ReferralAnchorCreated ------------- */
    for (const log of refLogs) {
      const { args } = registry.interface.parseLog({
        topics: log.topics,
        data: log.data,
      })!;
      const user = (args.user as string).toLowerCase();
      const referee = (args.referee as string).toLowerCase();

      const ts = await getTs(log.blockNumber); // we can derive check-in from block ts

      if (!participants[user]) {
        participants[user] = {
          address: user,
          referee: referee !== ZeroAddress ? referee : null,
          checkInTime: ts, // set from event block
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
          principalNow: undefined,
          minPrincipalByWeek: {},
        };
        firstSeenBlock[user] = firstSeenBlock[user]
          ? Math.min(firstSeenBlock[user], log.blockNumber)
          : log.blockNumber;
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
          : log.blockNumber;
      }

      const ts = await getTs(log.blockNumber);
      const week = getWeekIndex(ts);
      const wkKey = String(week);

      dirtyWeeks.add(week);

      // capture BEFORE we mutate
      const isFirstEver = !participants[user].firstClaimAt;
      const referee = participants[user].referee || null;

      // aggregate user totals
      participants[user].claimedTotal = addStr(
        participants[user].claimedTotal,
        amount
      );
      participants[user].lastClaimAt = ts;
      bumpParticipantWeek(participants[user], week, "claimed", amount);

      // principal increases on claim
      participants[user].principalNow = addStr(
        participants[user].principalNow,
        amount
      );
      const nowBN = toBN(participants[user].principalNow!);
      const prev = participants[user].minPrincipalByWeek![wkKey];
      participants[user].minPrincipalByWeek![wkKey] = !prev
        ? nowBN.toString()
        : (nowBN < toBN(prev) ? nowBN : toBN(prev)).toString();

      // first claim ever → possibly qualifies + shareUnits
      participants[user].byWeek = participants[user].byWeek || {};
      participants[user].byWeek[wkKey] = initParticipantWeek(
        participants[user].byWeek[wkKey]
      );

      if (isFirstEver) {
        // finalize first-claim markers
        participants[user].firstClaimAt = ts;
        participants[user].firstClaimAmt = amount;

        if (referee && referee !== ZeroAddress) {
          const ref = referee.toLowerCase();
          initSponsorIfMissing(sponsors, ref);
          sponsors[ref].byWeek = sponsors[ref].byWeek || {};
          sponsors[ref].byWeek[wkKey] = initSponsorWeek(
            sponsors[ref].byWeek[wkKey]
          );

          // always count new member + sum of their first claim
          const sw = sponsors[ref].byWeek[wkKey]!;
          sw.newMembers += 1;
          sw.firstClaimSum = addStr(sw.firstClaimSum, amount);

          // IF first claim ≥ 100 → they qualify this week AND credit shareUnits to sponsor
          if (toBN(amount) >= FIRST_CLAIM_QUALIFY) {
            sw.qualifying = (sw.qualifying ?? 0) + 1;
            participants[user].byWeek[wkKey].qualifiedForShares = true;

            const per = sw.perDownline!;
            per[user] = addStr(per[user], amount);
            sw.shareUnits = addStr(sw.shareUnits, amount);
          }
        } else {
          // backfill later
          needReferee.push(user);
        }
      } else {
        // Not first claim ever: only add shareUnits for subsequent claims in SAME week
        // if they already qualifiedForShares this week (from a ≥100 first claim).
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
    }

    /* --------------------- FORFEITED --------------------- */
    for (const log of forfeitLogs) {
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
          : log.blockNumber;
      }

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

    /* --------------------- UNSTAKED --------------------- */
    for (const log of unstakedLogs) {
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
          : log.blockNumber;
      }

      const ts = await getTs(log.blockNumber);
      const week = getWeekIndex(ts);
      dirtyWeeks.add(week);

      // principalNow decreases
      participants[user].principalNow = subFloor(
        participants[user].principalNow!,
        amount
      );
      const wk = String(week);
      const curMin =
        (participants[user].minPrincipalByWeek || {})[wk] ||
        participants[user].principalNow!;
      const now = toBN(participants[user].principalNow!);

      participants[user].minPrincipalByWeek =
        participants[user].minPrincipalByWeek || {};
      participants[user].minPrincipalByWeek![wk] =
        toBN(curMin) === 0n || now < toBN(curMin)
          ? now.toString()
          : toBN(curMin).toString();

      // Anti-gaming: deduct sponsor shareUnits that came from this downline in SAME week
      const referee = participants[user].referee || null;
      if (referee && referee !== ZeroAddress) {
        const ref = referee.toLowerCase();
        const sw = sponsors[ref]?.byWeek?.[wk];
        const perDown = sw?.perDownline?.[user];
        if (sw && perDown) {
          const credited = toBN(perDown);
          if (credited > 0n) {
            const ded = toBN(amount) > credited ? credited : toBN(amount);
            sw.perDownline![user] = subFloor(perDown, ded.toString());
            sw.shareUnits = subFloor(sw.shareUnits ?? "0", ded.toString());
          }
        }
      }
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
    const newlyBackfilled: string[] = []; // track which users we just filled

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

    // >>> Retroactively apply first-claim side-effects for newly backfilled referees
    for (const u of newlyBackfilled) {
      const p = participants[u];
      if (!p || !p.firstClaimAt || !p.referee) continue;

      const wk = String(getWeekIndex(p.firstClaimAt));
      const ref = p.referee.toLowerCase();

      // Ensure week bags exist
      p.byWeek = p.byWeek || {};
      p.byWeek[wk] = initParticipantWeek(p.byWeek[wk]);

      sponsors[ref].byWeek = sponsors[ref].byWeek || {};
      sponsors[ref].byWeek[wk] = initSponsorWeek(sponsors[ref].byWeek[wk]);
      const sw = sponsors[ref].byWeek[wk];

      // Always count first-claim new member + sum
      const amt = p.firstClaimAmt || "0";
      sw.newMembers += 1;
      sw.firstClaimSum = addStr(sw.firstClaimSum, amt);

      // If first claim >= 100, mark qualified + add shareUnits
      if (toBN(amt) >= FIRST_CLAIM_QUALIFY) {
        sw.qualifying = (sw.qualifying ?? 0) + 1;
        p.byWeek[wk].qualifiedForShares = true;

        const per = sw.perDownline!;
        per[u] = addStr(per[u], amt);
        sw.shareUnits = addStr(sw.shareUnits, amt);
      }

      // Make sure this week’s aggregate gets recomputed/flushed in this chunk
      dirtyWeeks.add(Number(wk));
    }

    // Build the set of users that need pre-event balance reads
    const uniqNewUsers = Object.keys(firstSeenBlock).filter(
      (u) => !participants[u]?.principalNow
    );

    if (uniqNewUsers.length) {
      const buckets: Record<number, string[]> = {};

      for (const u of uniqNewUsers) {
        // Use the earliest known block before their first activity
        const blockTag = (firstSeenBlock[u] ?? startBlockDefault) - 1;
        const b = Math.max(0, blockTag);
        (buckets[b] ||= []).push(u);
      }

      // Run multicall batches per block tag
      for (const [blockTagStr, usersAtBlock] of Object.entries(buckets)) {
        const blockTag = Number(blockTagStr);
        const map = await mcBatchGetUserBalance(usersAtBlock, blockTag);

        for (const [u, bal] of Object.entries(map)) {
          if (participants[u] && !participants[u].principalNow) {
            participants[u].principalNow = bal;
            participants[u].minPrincipalByWeek ||= {};
            if (!participants[u].minPrincipalByWeek["0"]) {
              participants[u].minPrincipalByWeek["0"] =
                toBN(bal) >= MIN_STAKE ? bal : "0";
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
      const wkElig = buildEligibilityForWeeks(
        dirty,
        participants,
        MIN_STAKE,
        sponsors
      );
      for (const wk of dirty) {
        const file = path.join(weeksDir, `week_${wk}.json`);
        // merge with existing if needed (keep extra fields)
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

    // checkpoint
    writeJSON(checkpointFile, { lastProcessedBlock: toBlock });
    console.log(`[ok] processed ${cursor}→${toBlock}`);
    cursor = toBlock + 1;
  }

  /* --------------------- FINAL ELIGIBILITY PASS --------------------- */
  // Collect every week index we have data for
  const allWeeksSet = new Set<number>();
  for (const p of Object.values(participants)) {
    const mpbw = p.minPrincipalByWeek || {};
    for (const wk of Object.keys(mpbw)) allWeeksSet.add(Number(wk));
  }
  const allWeeks = Array.from(allWeeksSet.values()).sort((a, b) => a - b);

  // Rebuild using the same combined rule (stake + sponsor.qualifying)
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

  // --- DIFF LOG ---
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
