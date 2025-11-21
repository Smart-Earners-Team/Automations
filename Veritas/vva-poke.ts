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
  Interface,
} from "ethers";

import { Multicall, Call3, Call3Value } from "@evmlord/multicall-sdk";

// ── ABIs ──────────────────────────────────────────────────────────────────────
import erc20ABI from "../HHV2/ERC20ABI.json";
import { fileURLToPath } from "node:url";

const REGISTRY_ABI = [
  "function getAllUsers() view returns (uint256)",
  "function getParticipants(uint256 pageNumber, uint256 pageLength) view returns (address[])",
];

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const CACHE_FILE = path.join(__dirname, "vva-main-cache.json");
const LOG_FILE = path.join(__dirname, "vva-main-log.txt");

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

// ── Config ────────────────────────────────────────────────────────────────────
const sVVA_ADDRESS = "0xf3fb455Af19fa0Fa314c6128dD5Fdee5A44FcE8e";
const REGISTRY_CONTRACT = "0x028f50D6c826A37D50d81E17B8934c1e98082CA6";

const PRIVATE_KEY = process.env.VVA_PRIVATE_KEY;
const CHAIN_ID = 56;
const READ_BATCH_SIZE = 1000; // how many wallets to read per Multicall
const PAGES_PER_CALL = 5; // how many pages we batch in one aggregate3
const BATCH_SIZE = READ_BATCH_SIZE * PAGES_PER_CALL; // 5 000 addresses
const POKE_BATCH_SIZE = 2048; // how many addresses to poke per tx
const GAS_HEADROOM_BPS = 500n; // +5% on top of estimate
const RPC_URL = "https://bsc-rpc.publicnode.com";

// ── Setup RPC, Wallet, Contracts ───────────────────────────────────────────────
const provider = new JsonRpcProvider(RPC_URL);
const wallet = new Wallet(PRIVATE_KEY, provider);
const signer = new NonceManager(wallet.connect(provider));
const sVVA = new Contract(sVVA_ADDRESS, erc20ABI, signer);
const registry = new Contract(REGISTRY_CONTRACT, REGISTRY_ABI, provider);

// one persistent Multicall instance for reads
const mc = new Multicall({ chainId: CHAIN_ID, provider, signer });

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

/**
 * In one pass, scan wallet and collect which need to be poked.
 * - Splits `wallets` into chunks so that (#calls = chunkSize × MAX_LEVEL) ≤ READ_BATCH_SIZE.
 * - Phase 1: Multicall `users(address)` to read each wallet’s current balance.
 * - Phase 2: Multicall `get3x6Entry(address, level)` only up to that level.
 * - Filters entries by vault ≥ plan(level)+MONTHLY_FEE and lastRenewTime ≤ cutoff.
 *
 * @param wallets - Full list of user wallet addresses.
 * @returns A map `{ [level]: string[] }` of addresses due at each level.
 */
async function filterAddressesByBalance(
  wallets: string[],
  threshold = 27n * 10n ** 9n
): Promise<string[]> {
  // sanitize: keep only valid, lowercased checks for dedupe
  const uniqueValid = Array.from(
    new Set(wallets.filter((a) => isAddress(a)).map((a) => a))
  );

  if (uniqueValid.length === 0) return [];

  // one call per address per chunk
  const chunkSize = READ_BATCH_SIZE;

  log(
    `→ scanning ${uniqueValid.length} unique wallets in chunks of ${chunkSize} each`
  );

  const rich: string[] = [];

  // overall timer
  const overallStart = process.hrtime();

  for (let i = 0; i < uniqueValid.length; i += chunkSize) {
    const slice = uniqueValid.slice(i, i + chunkSize);

    // timer for this chunk
    const chunkStart = process.hrtime();

    // Build balance calls
    const balanceCalls: Call3[] = slice.map((addr) => ({
      contract: sVVA,
      functionFragment: "balanceOf",
      args: [addr],
      allowFailure: true,
    }));

    const rawBalances = await mc.aggregate3(balanceCalls);

    rawBalances.forEach(([ok, ret], idx) => {
      const bal: bigint = ok ? ret : 0n;

      if (bal > threshold) {
        rich.push(slice[idx]);
      }
    });

    // log chunk timing
    log(
      `  → scanned balances ${i + 1}–${i + slice.length} in ${hrTimeMs(
        chunkStart
      ).toFixed(2)} ms`
    );
  }

  log(
    `   filterAddressesByBalance took ${hrTimeMs(overallStart).toFixed(2)} ms`
  );

  return rich;
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// Build pairwise calls; add ring hop if odd length (auto-chain)
function buildPokeCalls(
  users: string[],
  token: Contract,
  allowFailure: boolean = false
): Call3Value[] {
  const calls: Call3Value[] = [];

  // Pairs: (u0->u1), (u2->u3), ...
  for (let i = 0; i + 1 < users.length; i += 2) {
    calls.push({
      contract: token,
      allowFailure,
      functionFragment: "transferFrom",
      args: [users[i], users[i + 1], 0n],
      value: 0n,
    });
  }

  // If odd count, add a ring link: (last -> first)
  if (users.length % 2 === 1) {
    calls.push({
      contract: token,
      allowFailure,
      functionFragment: "transferFrom",
      args: [users[users.length - 1], users[0], 0n],
      value: 0n,
    });
  }

  return calls;
}

// Convert Call3Value[] to Multicall3-compatible tuple for aggregate3Value
function encodeAggregate3Value(calls: Call3Value[]): {
  calls: {
    target: string;
    allowFailure: boolean;
    callData: string;
    value: bigint;
  }[];
  totalValue: bigint;
} {
  let total: bigint = 0n;
  const out = calls.map((c) => {
    const iface = c.contract.interface as Interface;
    const callData = iface.encodeFunctionData(c.functionFragment, c.args);
    total += c.value;
    return {
      target: c.contract.target as string,
      allowFailure: c.allowFailure,
      callData,
      value: c.value,
    };
  });
  return { calls: out, totalValue: total };
}

/**
 * Finds the largest slice of `calls` starting at `start` that fits under txGasCap,
 * using estimateGas on Multicall3.aggregate3Value.
 */
async function findMaxFittingCalls(
  calls: Call3Value[],
  start: number,
  txGasCap: bigint,
  methodName: string = "aggregate3Value"
): Promise<{ size: number; gasLimit: bigint }> {
  if (!mc.contract) return { size: 0, gasLimit: 0n };

  const n = calls.length;
  if (start >= n) return { size: 0, gasLimit: 0n };

  let lastGood = 0;
  let lastGoodGas = 0n;
  let probe = 1;

  // Phase 1: exponential growth
  while (start + probe <= n && probe <= POKE_BATCH_SIZE) {
    const slice = calls.slice(start, start + probe);
    const { calls: wire, totalValue } = encodeAggregate3Value(slice);
    try {
      const est = await mc.contract[methodName].estimateGas(wire, {
        value: totalValue,
      });

      const padded = est + (est * GAS_HEADROOM_BPS) / 10_000n;
      if (padded <= txGasCap) {
        lastGood = probe;
        lastGoodGas = padded;
        probe *= 2;
        continue;
      }
      break;
    } catch {
      break; // exceeded intrinsic/other → go to binary search
    }
  }

  // Setup bounds
  let low = lastGood + 1;
  let high = Math.min(start + probe, n) - start;
  if (lastGood === 0) {
    low = 1;
    high = Math.min(probe, n - start, POKE_BATCH_SIZE);
  }

  // Phase 2: binary search between lastGood and high
  let L = lastGood; // known good
  let R = high + 1; // known bad (exclusive)
  while (L + 1 < R) {
    const mid = Math.floor((L + R) / 2);
    const slice = calls.slice(start, start + mid);
    const { calls: wire, totalValue } = encodeAggregate3Value(slice);
    try {
      const est = await mc.contract[methodName].estimateGas(wire, {
        value: totalValue,
      });
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

  if (L === 0) {
    throw new Error(
      `estimateGas failed even for a single call at index ${start}`
    );
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
  log(`\n=== Starting poke run at ${new Date().toISOString()} ===\n`);

  // fetch+cache ALL wallets once
  const allWallets = await fetchAllWallets();

  // Scan every level in parallel, in chunks
  log("→ fetching qualified users…");

  const qualifiedAddrs = await filterAddressesByBalance(allWallets);

  // Collect all batchRenew calls
  const pokeCalls: Call3Value[] = buildPokeCalls(qualifiedAddrs, sVVA);

  if (pokeCalls.length === 0) {
    log("Nothing to do.");
    return;
  }

  log(`Prepared ${pokeCalls.length} transferFrom(…, 0) calls`);

  for (let i = 0; i < pokeCalls.length; ) {
    const { size, gasLimit } = await findMaxFittingCalls(
      pokeCalls,
      i,
      29_000_000n
    ); // 90% of 30M
    const slice = pokeCalls.slice(i, i + size);

    log(`→ Sending batch @i=${i} size=${size} gasLimit≈${gasLimit.toString()}`);

    const tx = await mc.sendAggregate3Value(slice, {
      value: 0, // 0n for this use-case
      gasLimit, // conservative padded limit
    });

    log(`   → submitted tx ${tx.hash}, awaiting confirmation…`);
    const rcpt = await tx.wait();
    rcpt &&
      log(
        `    → confirmed in block ${rcpt.blockNumber}` +
          ` (gasUsed: ${rcpt.gasUsed.toString()})`
      );

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
