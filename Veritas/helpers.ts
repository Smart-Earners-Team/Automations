import fs from "fs";
import path from "path";

// --- TYPES ---
export type WeekMeta = {
  [userAddrLower: string]: { shares: number };
};
export type WeekAggregate = {
  week: number;
  eligibility: string[];
  ineligible: string[];
  meta?: WeekMeta;
  [k: string]: any;
};
export type ParticipantWeek = {
  staked: string;
  claimed: string;
  forfeited: string;
  stakes: number;
  claims: number;
  forfeits: number;
  qualifiedForShares?: boolean;
};
export type Participant = {
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
export type SponsorWeek = {
  newMembers: number;
  firstClaimSum: string;
  qualifying?: number;
  shareUnits?: string;
  perDownline?: Record<string, string>;
};
export type Sponsor = {
  address: string;
  teamSize: number;
  byWeek: Record<string, SponsorWeek>;
};
export type Checkpoint = { lastProcessedBlock: number };

// --- FS HELPERS ---
export function ensureDirSync(dir: string) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
}

export function readJSON<T>(file: string, fallback: T): T {
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

export function writeJSON(file: string, data: any) {
  atomicWriteJSON(file, data);
}

export function ensureFileSync(file: string, defaultJson: any = {}) {
  ensureDirSync(path.dirname(file));
  if (!fs.existsSync(file))
    fs.writeFileSync(file, JSON.stringify(defaultJson, null, 2), "utf8");
}
