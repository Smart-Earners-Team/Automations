import fs from "node:fs";
import path from "path";

import * as dotenv from "dotenv";
dotenv.config();

import {
  JsonRpcProvider,
  NonceManager,
  Wallet,
  Contract,
  isAddress,
} from "ethers";

import { Multicall, Call3 } from "@evmlord/multicall-sdk";

const CACHE_FILE = path.join(__dirname, "vva-main-cache.json");
const LOG_FILE = path.join(__dirname, "vva-main-log.txt");

// ── ABIs ──────────────────────────────────────────────────────────────────────
const COMP_PLAN_ABI = [
  "function checkStakingCriteria(address user) view returns (bool isQualified)",
  "function getSVVABalanceInDAI(address user) view returns (uint256 daiValue)",
  "function claimableNow(address referrer) view returns (uint256)",
  "function isQualifiedReferral(address) view returns (bool)",
  "function settleReferralOnUsers(address[] users)",
];

const REGISTRY_ABI = [
  "function getAllUsers() view returns (uint256)",
  "function getParticipants(uint256 pageNumber, uint256 pageLength) view returns (address[])",
];

/* ── Dual logger ─────────────────────────────────────────────────── */
// open once in “append” mode
const logStream = fs.createWriteStream(LOG_FILE, { flags: "a" });

/**
 * Central logger: writes to both stdout and log file.
 * @param  {...unknown} parts  arbitrary things to join with spaces
 */
const log = (...parts: unknown[]) => {
  const line = parts.join(" ");
  console.log(line);
  logStream.write(line + "\n");
};

// cleanup handlers
process.on("beforeExit", () => {
  log("🛑 Process beforeExit, flushing log…");
  logStream.end();
});
process.on("SIGINT", () => {
  log("🛑 Caught SIGINT, exiting…");
  logStream.end();
  process.exit(0);
});
process.on("SIGTERM", () => {
  log("🛑 Caught SIGTERM, exiting…");
  logStream.end();
  process.exit(0);
});

if (!process.env.VVA_PRIVATE_KEY) {
  console.error("❌  Add VVA_PRIVATE_KEY to your .env file");
  process.exit(1);
}

if (!process.env.VVA_RPC_URL) {
  console.error("❌  Add VVA_RPC_URL to your .env file");
  process.exit(1);
}

// ── Config ────────────────────────────────────────────────────────────────────
const REGISTRY_CONTRACT = "0x028f50D6c826A37D50d81E17B8934c1e98082CA6";
const COMPPLAN_ADDRESS = "0xAF5B99f0d14e556bAa6F0E28b4d2a09BDD48Ad54";

const PRIVATE_KEY = process.env.VVA_PRIVATE_KEY;
const CHAIN_ID = 56;
const READ_BATCH_SIZE = 1000; // how many wallets to read per Multicall
const PAGES_PER_CALL = 5; // how many pages we batch in one aggregate3
const BATCH_SIZE = READ_BATCH_SIZE * PAGES_PER_CALL; // 5 000 addresses
const POKE_BATCH_SIZE = 2048; // how many addresses to poke max per tx
const GAS_HEADROOM_BPS = 500n; // +5% on top of estimate

// ── Setup RPC, Wallet, Contracts ───────────────────────────────────────────────
const provider = new JsonRpcProvider(process.env.VVA_RPC_URL);
const wallet = new Wallet(PRIVATE_KEY, provider);
const signer = new NonceManager(wallet.connect(provider));
const registry = new Contract(REGISTRY_CONTRACT, REGISTRY_ABI, provider);
const compPlan = new Contract(COMPPLAN_ADDRESS, COMP_PLAN_ABI, signer);

// one persistent Multicall instance for reads
const mc = new Multicall({ chainId: CHAIN_ID, provider });

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Compute the elapsed time (in milliseconds) since a given hrtime start.
 * @param start - The tuple returned by process.hrtime() at start.
 * @returns Milliseconds elapsed, with sub-millisecond precision.
 */
function hrTimeMs(start: [number, number]): number {
  const [sec, nano] = process.hrtime(start);
  return sec * 1e3 + nano / 1e6; // → milliseconds
}

/**
 * Load the locally cached wallet list (if any).
 * @returns Array of wallet addresses, or [] if no cache exists.
 */
function loadCachedWallets(): string[] {
  try {
    const raw = fs.readFileSync(CACHE_FILE, "utf8");
    return JSON.parse(raw) as string[];
  } catch (e) {
    return [];
  }
}

/**
 * Save the given wallet list to the local cache (atomically).
 * @param wallets - Array of wallet addresses to cache.
 */
function saveCachedWallets(wallets: string[]): void {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(wallets, null, 2));
  log(`🔒 updated cache on disk (${wallets.length} entries)`);
}

/**
 * Fetch a contiguous ID range via the paginated
 * `getParticipants(uint256 pageNumber, uint256 pageLength)` view.
 *
 * @param startId   First wallet ID (inclusive, **1‑based**).
 * @param endId     Last  wallet ID (inclusive, **1‑based**).
 * @param pageSize  Page length to request on‑chain (defaults to 1 000).
 *
 * @throws If any individual page call fails.
 * @returns Array of wallet addresses covering [startId, endId].
 */
async function fetchWalletsRange(
  startId: number,
  endId: number,
  pageSize = 1_000
): Promise<string[]> {
  if (endId < startId) throw new Error("`endId` must be ≥ `startId`");

  // 1‑based page numbers
  const firstPage = Math.floor((startId - 1) / pageSize) + 1; // ← +1
  const lastPage = Math.floor((endId - 1) / pageSize) + 1; // ← +1

  const calls: Call3[] = [];

  for (let page = firstPage; page <= lastPage; page++) {
    calls.push({
      contract: registry,
      functionFragment: "getParticipants",
      args: [page, pageSize],
      allowFailure: false,
    });
  }

  const raw = await mc.aggregate3(calls);

  const wallets: string[] = [];
  raw.forEach(([ok, participants], i) => {
    if (!ok) {
      throw new Error(
        `getParticipants(page=${firstPage + i}, len=${pageSize}) failed`
      );
    }

    const pageNumber = firstPage + i; // 1‑based
    (participants as string[]).forEach((addr, j) => {
      const id =
        (pageNumber - 1) * pageSize + // ← −1 here
        j +
        1; // ← +1 for 1‑based IDs
      if (id >= startId && id <= endId) wallets.push(addr);
    });
  });

  return wallets;
}

/**
 * Return `count` unique random elements from `array`.
 */
function sample<T>(array: T[], count: number): T[] {
  const result: T[] = [];
  const taken = new Set<number>();
  while (result.length < count && result.length < array.length) {
    const i = Math.floor(Math.random() * array.length);
    if (!taken.has(i)) {
      taken.add(i);
      result.push(array[i]);
    }
  }
  return result;
}

/*********************************************************************
 * Ensure we have an up‑to‑date cache of all user wallets.
 * Works with the paginated `getParticipants` view and pulls data in
 * bigger chunks (default 5 000 addresses / Multicall).
 *
 * 1) Load the local cache
 * 2) Compare it with on‑chain `registry.getAllUsers()`
 *    – length equality
 *    – random‑page sample equality (case‑insensitive)
 * 3) If something looks stale, download either the missing tail or
 *    the whole list, re‑writing the cache if it changed.
 *********************************************************************/
async function fetchAllWallets(pageSize = READ_BATCH_SIZE): Promise<string[]> {
  log("⏳ load wallet cache…");
  const hrStart = process.hrtime();

  const cached = loadCachedWallets();

  const totalUsers = Number(await registry.getAllUsers());

  // ------------------------------------------------------------
  // 1. QUICK SANITY CHECK – length and random page samples
  // ------------------------------------------------------------
  let needsFullRefresh = false;

  if (cached.length === totalUsers && cached.length > 0) {
    const totalPages = Math.ceil(totalUsers / pageSize);
    const samplePages = sample(
      [...Array(totalPages).keys()].map((i) => i + 1), // 1‑based pages
      Math.min(3, totalPages)
    );

    for (const page of samplePages) {
      // fetch a whole page once
      const onChain = (await registry.getParticipants(
        page,
        pageSize
      )) as string[];
      const offset = (page - 1) * pageSize;
      const cachedSlice = cached.slice(offset, offset + onChain.length);

      const same = onChain.every(
        (addr, i) => addr.toLowerCase() === cachedSlice[i]?.toLowerCase()
      );

      if (!same) {
        needsFullRefresh = true;
        break;
      }
    }

    if (!needsFullRefresh) {
      log(`✅ cache sample passed (${samplePages.length} pages checked)`);
      log(`   fetchAllWallets took ${hrTimeMs(hrStart).toFixed(2)} ms`);
      return cached;
    }

    log("⚠️ sample mismatch — refreshing full wallet cache");
  } else {
    // size mismatch ⇒ maybe we can just append
    log(`⚠️ cache length mismatch: ${cached.length} vs ${totalUsers}`);
  }

  /* ---------------------------------
   * Decide refresh scope
   * --------------------------------- */
  const refreshFromId = needsFullRefresh ? 1 : cached.length + 1;
  const refreshed: string[] = needsFullRefresh ? [] : [...cached];

  log(
    `⚠️ cache has ${cached.length}, but totalUsers = ${totalUsers}. ` +
      `Fetching IDs ${refreshFromId}–${totalUsers}…`
  );

  /* ---------------------------------
   * Download missing range in 5 000‑ID chunks
   * --------------------------------- */
  for (
    let startId = refreshFromId;
    startId <= totalUsers;
    startId += BATCH_SIZE
  ) {
    const endId = Math.min(totalUsers, startId + BATCH_SIZE - 1);
    const slice = await fetchWalletsRange(startId, endId, pageSize);
    refreshed.push(...slice);
    log(`  → fetched ${startId}–${endId}`);
  }

  /* ---------------------------------
   * Persist if actually changed
   * --------------------------------- */
  const changed =
    cached.length !== refreshed.length ||
    cached.some((a, i) => a.toLowerCase() !== refreshed[i].toLowerCase());

  if (changed) {
    saveCachedWallets(refreshed);
    log(`🔒 updated cache on disk (${refreshed.length} entries)`);
  } else {
    log("✅ cache contents match, skipping write");
  }

  log(`   fetchAllWallets took ${hrTimeMs(hrStart).toFixed(2)} ms`);
  return refreshed;
}

async function filterAddressesByCompPlan(
  wallets: string[],
  opts?: {
    daiMin?: bigint; // e.g. 100e18n
    requireStakingCriteria?: boolean; // default true
    requireQualifiedReferral?: boolean; // default false
    requireClaimableNow?: boolean; // default false
  }
): Promise<string[]> {
  const {
    daiMin = 0n,
    requireStakingCriteria = true,
    requireQualifiedReferral = false,
    requireClaimableNow = false,
  } = opts || {};

  log("→ comp-plan filtering…");

  const unique = Array.from(new Set(wallets.filter((a) => isAddress(a)))).map(
    (a) => a
  );

  if (unique.length === 0) return [];

  const chunkSize = READ_BATCH_SIZE;
  const qualified: string[] = [];

  log(
    `→ comp-plan screening ${unique.length} unique addrs (chunk=${chunkSize})`
  );

  for (let i = 0; i < unique.length; i += chunkSize) {
    const slice = unique.slice(i, i + chunkSize);

    // Build multicall reads
    const calls: Call3[] = [];

    // DAI balance in plan
    slice.forEach((addr) =>
      calls.push({
        contract: compPlan,
        functionFragment: "getSVVABalanceInDAI",
        args: [addr],
        allowFailure: true,
      })
    );

    // Staking criteria
    if (requireStakingCriteria) {
      slice.forEach((addr) =>
        calls.push({
          contract: compPlan,
          functionFragment: "checkStakingCriteria",
          args: [addr],
          allowFailure: true,
        })
      );
    }

    // Qualified referral flag
    if (requireQualifiedReferral) {
      slice.forEach((addr) =>
        calls.push({
          contract: compPlan,
          functionFragment: "isQualifiedReferral",
          args: [addr],
          allowFailure: true,
        })
      );
    }

    // Claimable amount now
    if (requireClaimableNow) {
      slice.forEach((addr) =>
        calls.push({
          contract: compPlan,
          functionFragment: "claimableNow",
          args: [addr],
          allowFailure: true,
        })
      );
    }

    const results = await mc.aggregate3(calls);

    // Unpack results in the order we pushed
    let p = 0;

    // DAI balances
    const daiVals: bigint[] = slice.map(() => {
      const [ok, ret] = results[p++];
      return ok ? (ret as bigint) : 0n;
    });

    // Staking criteria
    const stakingOK: boolean[] = requireStakingCriteria
      ? slice.map(() => {
          const [ok, ret] = results[p++];
          return ok ? (ret as boolean) : false;
        })
      : slice.map(() => true);

    // Qualified referral
    const qualRefOK: boolean[] = requireQualifiedReferral
      ? slice.map(() => {
          const [ok, ret] = results[p++];
          return ok ? (ret as boolean) : false;
        })
      : slice.map(() => true);

    // Claimable now
    const claimableOK: boolean[] = requireClaimableNow
      ? slice.map(() => {
          const [ok, ret] = results[p++];
          const amt = ok ? (ret as bigint) : 0n;
          return amt > 0n;
        })
      : slice.map(() => true);

    slice.forEach((addr, idx) => {
      if (
        daiVals[idx] >= daiMin &&
        stakingOK[idx] &&
        qualRefOK[idx] &&
        claimableOK[idx]
      ) {
        qualified.push(addr);
      }
    });

    log(
      `  → comp-plan screened ${i + 1}–${i + slice.length} → kept ${
        qualified.length
      }`
    );
  }

  return qualified;
}

async function findMaxSettleSlice(
  start: number,
  users: string[],
  txGasCap: bigint,
  maxProbe = POKE_BATCH_SIZE // a soft cap if desired
): Promise<{ size: number; gasLimit: bigint }> {
  const n = users.length;
  if (start >= n) return { size: 0, gasLimit: 0n };

  let lastGood = 0;
  let lastGoodGas = 0n;
  let probe = 1;

  // Phase 1: exponential growth
  while (start + probe <= n && probe <= maxProbe) {
    const slice = users.slice(start, start + probe);
    try {
      const est = await compPlan.settleReferralOnUsers.estimateGas(slice);
      const padded = est + (est * GAS_HEADROOM_BPS) / 10_000n;
      if (padded <= txGasCap) {
        lastGood = probe;
        lastGoodGas = padded;
        probe *= 2;
        continue;
      }
      break;
    } catch {
      break;
    }
  }

  // Bounds
  let high = Math.min(start + probe, n) - start;
  if (lastGood === 0) {
    // see if at least 1 fits
    let L = 0,
      R = Math.min(probe, n - start, maxProbe) + 1;
    while (L + 1 < R) {
      const mid = Math.floor((L + R) / 2);
      const slice = users.slice(start, start + mid);
      try {
        const est = await compPlan.settleReferralOnUsers.estimateGas(slice);
        const padded = est + (est * GAS_HEADROOM_BPS) / 10_000n;
        if (padded <= txGasCap) {
          L = mid;
          lastGoodGas = padded;
        } else {
          R = mid;
        }
      } catch {
        R = mid;
      }
    }
    if (L === 0)
      throw new Error(`estimateGas fails even for 1 user at ${start}`);
    return { size: L, gasLimit: lastGoodGas };
  }

  // Binary search between lastGood and high
  let L = lastGood;
  let R = high + 1;
  while (L + 1 < R) {
    const mid = Math.floor((L + R) / 2);
    const slice = users.slice(start, start + mid);
    try {
      const est = await compPlan.settleReferralOnUsers.estimateGas(slice);
      const padded = est + (est * GAS_HEADROOM_BPS) / 10_000n;
      if (padded <= txGasCap) {
        L = mid;
        lastGoodGas = padded;
      } else {
        R = mid;
      }
    } catch {
      R = mid;
    }
  }

  return { size: L, gasLimit: lastGoodGas };
}

// ── Main ──────────────────────────────────────────────────────────────────────
/**
 * Main driver:
 * 1) Load/cache all wallets
 * 2) Multi‐level scan for due renewals
 * 3) Batch‐submit renewals at RENEW_BATCH_SIZE
 */
async function main() {
  log(`\n=== Starting settle run at ${new Date().toISOString()} ===\n`);

  // fetch+cache ALL wallets once
  const allWallets = await fetchAllWallets();

  // Scan every level in parallel, in chunks
  log("→ fetching qualified users…");

  const qualifiedAddrs = await filterAddressesByCompPlan(allWallets, {
    daiMin: 100n * 10n ** 18n,
    requireStakingCriteria: false,
    requireQualifiedReferral: true,
    requireClaimableNow: false,
  });

  if (qualifiedAddrs.length === 0) {
    log("Nothing to do.");
    return;
  }

  log(`Qualified users: ${qualifiedAddrs.length}`);

  for (let i = 0; i < qualifiedAddrs.length; ) {
    const { size, gasLimit } = await findMaxSettleSlice(
      i,
      qualifiedAddrs,
      27_000_000n // ~90% of 30M
    );
    const slice = qualifiedAddrs.slice(i, i + size);

    log(`→ settle batch @i=${i} size=${size} gas≈${gasLimit.toLocaleString()}`);

    const tx = await compPlan.settleReferralOnUsers(slice, { gasLimit });
    log(`   → submitted ${tx.hash}, awaiting…`);
    const rcpt = await tx.wait();
    log(`   ✓ block ${rcpt?.blockNumber} gasUsed=${rcpt.gasUsed?.toString()}`);

    i += size;
  }

  log("Done.");
}

// ── retry wrapper ─────────────────────────────────────────────────────────────
/**
 * Wrapper to retry `main()` up to `maxRetries` times on failure.
 * @param retryDelay - Milliseconds to wait between attempts.
 * @param maxRetries - Maximum retry count before giving up.
 */
async function mainWithRetry(
  retryDelay = 60000,
  maxRetries = 5
): Promise<void> {
  // retryDelay is in milliseconds, 300000ms = 5 minutes
  let attempts = 0;

  while (attempts < maxRetries) {
    try {
      await main();
      return; // If main() succeeds, exit the loop
    } catch (err: any) {
      console.error(`Error in main():`, err);
      log(`Unhandled error: ${err.message || err}`);

      attempts++;
      log(`Retry attempt ${attempts}/${maxRetries} in ${retryDelay}ms`);

      await new Promise((r) => setTimeout(r, retryDelay));
    }
  }

  console.error("Max retries reached, exiting.");
  process.exit(1);
}

mainWithRetry().catch((e) => console.error("Fatal error:", e));
