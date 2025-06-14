import fs from "node:fs";
import path from "path";

import * as dotenv from "dotenv";
dotenv.config();

import {
  parseEther,
  JsonRpcProvider,
  NonceManager,
  Wallet,
  Contract,
} from "ethers";

import { Multicall, Call3 } from "@evmlord/multicall-sdk";

const CACHE_FILE = path.join(__dirname, "wallets-cache.json");
const LOG_FILE = path.join(__dirname, "renewals_log.txt");

// ── ABIs ──────────────────────────────────────────────────────────────────────
import hhABI from "./ABI.json";
import erc20ABI from "./ERC20ABI.json";
import dexABI from "./DEXABI.json";

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

if (!process.env.HH_PRIVATE_KEY) {
  console.error("❌  Add HH_PRIVATE_KEY to your .env file");
  process.exit(1);
}

// ── Config ────────────────────────────────────────────────────────────────────
const SECONDS_IN_DAY = 86_400;
const MATRIX_ADDRESS =
  // "0x7E623888B34E3c66C40751f5A4689A8DE56B595C";
  "0x0e950F60387C5d9DE09D5710E135776370c713dc";
const USDT_ADDRESS = "0x3807C468D722aAf9e9A82d8b4b1674E66a12E607";
const DEX_ADDRESS = "0xfD28480E8fABbC1f3D66cF164DFe6B0818249A25";
const PRIVATE_KEY = process.env.HH_PRIVATE_KEY;
const MAX_LEVEL = 6;
const CHAIN_ID = 97;
const MONTHLY_FEE = parseEther("1");
const READ_BATCH_SIZE = 1500; // how many wallets to read per Multicall
const RENEW_BATCH_SIZE = 100; // how many addresses to renew per tx
const RENEW_DURATION = 2 * SECONDS_IN_DAY; // ~2 days in seconds
const RPC_URL = "https://bsc-testnet.publicnode.com";

// ── Setup RPC, Wallet, Contracts ───────────────────────────────────────────────
const provider = new JsonRpcProvider(RPC_URL);
const wallet = new Wallet(PRIVATE_KEY, provider);
const signer = new NonceManager(wallet.connect(provider));
const matrix = new Contract(MATRIX_ADDRESS, hhABI, signer);
const usdt = new Contract(USDT_ADDRESS, erc20ABI, signer);
const dex = new Contract(DEX_ADDRESS, dexABI, signer);

// one persistent Multicall instance for reads
const mc = new Multicall({ chainId: CHAIN_ID, provider });
const cutoff = Math.floor(Date.now() / 1000) - RENEW_DURATION;

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
 * Calculate the on-chain cost for renewing at a given level.
 *   cost = 50 USDT * 2^(level-1), in 18-decimal units.
 * @param level - Level number (1 through 6).
 * @throws If level is not an integer between 1 and 6.
 * @returns The renewal cost as a bigint (scaled by 10^18).
 */
function plan(level: number): bigint {
  if (!Number.isInteger(level) || level < 1 || level > MAX_LEVEL)
    throw new Error(`level must be an integer between 1 and ${MAX_LEVEL}`);

  const base = parseEther("50"); // 50n * 10n ** 18n; // 5e18 in BigInt form

  return base << BigInt(level - 1); // multiply by 2^(level-1)
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
 * Fetch a contiguous range of wallet IDs via Multicall.
 * @param startId - First wallet ID (inclusive).
 * @param endId - Last wallet ID (inclusive).
 * @returns Promise resolving to an array of addresses.
 * @throws If any individual call fails.
 */
async function fetchWalletsRange(
  startId: number,
  endId: number
): Promise<string[]> {
  const calls: Call3[] = [];

  for (let id = startId; id <= endId; id++) {
    calls.push({
      contract: matrix,
      functionFragment: "wallets",
      args: [id],
      allowFailure: false,
    });
  }

  const raw = await mc.aggregate3(calls);

  return raw.map(([ok, addr], idx) => {
    if (!ok) throw new Error(`wallets(${startId + idx}) multicall failed`);

    return addr as string;
  });
}

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

/**
 * Ensure we have an up-to-date cache of all user wallets.
 * 1) Read local cache
 * 2) Compare against matrix.stats().totalUsers
 * 3) If too short, batch-fetch only the missing tail range
 * @returns Promise resolving to the full array of wallet addresses.
 */
async function fetchAllWallets() {
  log("⏳ load wallet cache…");

  const hrStart = process.hrtime();

  const cached = loadCachedWallets();
  const stats = await matrix.stats();
  const totalUsers = Number(stats.totalUsers);

  // Skip if cache is empty
  if (cached.length > 0 && cached.length === totalUsers) {
    const sampleSize = Math.min(10, cached.length);
    const sampleIndices = sample([...Array(cached.length).keys()], sampleSize);

    let mismatchFound = false;

    for (const index of sampleIndices) {
      const [fetched] = await fetchWalletsRange(index + 1, index + 1); // 1-based indexing
      if (fetched[0] !== cached[index]) {
        mismatchFound = true;
        break;
      }
    }

    if (!mismatchFound) {
      log(`✅ cache sample passed (${sampleSize} checks)`);
      log(`   fetchAllWallets took ${hrTimeMs(hrStart).toFixed(2)} ms`);
      return cached;
    } else {
      log(`⚠️ sample mismatch — refreshing full wallet cache`);
    }
  } else {
    log(`⚠️ cache length mismatch: ${cached.length} vs ${totalUsers}`);
  }

  log(
    `⚠️ cache has ${cached.length}, but totalUsers = ${totalUsers}. ` +
      `Fetching IDs ${cached.length + 1}–${totalUsers}…`
  );

  // Full refresh fallback
  const newWallets: string[] = [];

  for (
    let start = cached.length + 1;
    start <= totalUsers;
    start += READ_BATCH_SIZE
  ) {
    const end = Math.min(totalUsers, start + READ_BATCH_SIZE - 1);
    const slice = await fetchWalletsRange(start, end);
    newWallets.push(...slice);
    log(`  → fetched ${start}–${end}`);
  }

  const all = [...cached, ...newWallets];

  if (JSON.stringify(cached) !== JSON.stringify(all)) {
    saveCachedWallets(all);
  } else {
    log(`✅ cache contents match, skipping write`);
  }

  log(`   fetchAllWallets took ${hrTimeMs(hrStart).toFixed(2)} ms`);

  return all;
}

/**
 * In one pass, scan every level for every wallet and collect which need renewal.
 * - Splits `wallets` into chunks so that (#calls = chunkSize × MAX_LEVEL) ≤ READ_BATCH_SIZE.
 * - Phase 1: Multicall `users(address)` to read each wallet’s current level.
 * - Phase 2: Multicall `get3x6Entry(address, level)` only up to that level.
 * - Filters entries by vault ≥ plan(level)+MONTHLY_FEE and lastRenewTime ≤ cutoff.
 *
 * @param wallets - Full list of user wallet addresses.
 * @returns A map `{ [level]: string[] }` of addresses due at each level.
 */
async function fetchUsersDueAllLevels(
  wallets: string[]
): Promise<Record<number, string[]>> {
  const dueByLevel: Record<number, string[]> = {};

  for (let lvl = 1; lvl <= MAX_LEVEL; lvl++) {
    dueByLevel[lvl] = [];
  }

  // ensure (#users × MAX_LEVEL) per slice ≤ READ_BATCH_SIZE
  // const chunkSize = Math.floor(READ_BATCH_SIZE / MAX_LEVEL);
  const chunkSize = 100;

  log(`→ scanning ${wallets.length} wallets in chunks of ${chunkSize} each`);

  // overall timer
  const overallStart = process.hrtime();

  for (let i = 0; i < wallets.length; i += chunkSize) {
    const slice = wallets.slice(i, i + chunkSize);

    const chunkLabel = `${i + 1}–${i + slice.length}`;

    // timer for this chunk
    const chunkStart = process.hrtime();

    // Who’s at what level? - fetch each user’s matrix3x6Level
    const userCalls: Call3[] = slice.map((addr) => ({
      contract: matrix,
      functionFragment: "users",
      args: [addr],
      allowFailure: false,
    }));

    const rawUsers = await mc.aggregate3(userCalls);

    const levels: number[] = rawUsers.map(([ok, data]) => {
      if (!ok) return 0;
      // `data` is the decoded User tuple as an array
      // [wallet, referrer, id, refsCount, matrix3x6Level]
      const [, , , , matrix3x6Level] = data;

      // can also be collapsed into return Number((data as any)[4]);

      return Number(matrix3x6Level);
    });

    // Build only the needed get3x6Entry calls
    const entryCalls: Call3[] = [];

    const mapping: { user: string; level: number }[] = [];

    slice.forEach((user, idx) => {
      const maxLvl = Math.min(levels[idx], MAX_LEVEL);

      for (let lvl = 1; lvl <= maxLvl; lvl++) {
        entryCalls.push({
          contract: matrix,
          functionFragment: "get3x6Entry",
          args: [user, lvl],
          allowFailure: true,
        });
        mapping.push({ user, level: lvl });
      }
    });

    const rawEntries = await mc.aggregate3(entryCalls);

    // Decode & filter each entry
    rawEntries.forEach(([ok, ret], idx) => {
      if (!ok) return;

      const { userAddress, vault, lastRenewTime } = ret as {
        userAddress: string;
        vault: bigint;
        lastRenewTime: bigint;
      };

      const lvl = mapping[idx].level;

      if (vault >= plan(lvl) + MONTHLY_FEE && Number(lastRenewTime) <= cutoff) {
        dueByLevel[lvl].push(userAddress);
      }
    });

    // log chunk timing
    log(
      `  → scanned wallets ${chunkLabel} in ${hrTimeMs(chunkStart).toFixed(
        2
      )} ms`
    );
  }

  log(`   fetchUsersDueAllLevels took ${hrTimeMs(overallStart).toFixed(2)} ms`);

  return dueByLevel;
}
// async function fetchUsersDueAllLevels(
//   wallets: string[]
// ): Promise<Record<number, string[]>> {
//   const dueByLevel: Record<number, string[]> = {};

//   for (let lvl = 1; lvl <= MAX_LEVEL; lvl++) {
//     dueByLevel[lvl] = [];
//   }

//   // ensure (#users × MAX_LEVEL) per slice ≤ READ_BATCH_SIZE
//   // const chunkSize = Math.floor(READ_BATCH_SIZE / MAX_LEVEL);
//   const MAX_CONCURRENCY = 2;
//   const chunkSize = 100;
//   const tasks: Promise<void>[] = [];

//   log(
//     `→ scanning ${wallets.length} wallets in chunks of ${chunkSize} each`
//   );

//   // overall timer
//   const overallStart = process.hrtime();

//   for (let i = 0; i < wallets.length; i += chunkSize) {
//     const slice = wallets.slice(i, i + chunkSize);

//     const chunkLabel = `${i + 1}–${i + slice.length}`;

//     // build a task that does exactly one slice
//     const job = (async () => {
//       // timer for this chunk
//       const chunkStart = process.hrtime();

//       // Who’s at what level? - fetch each user’s matrix3x6Level
//       const userCalls: Call3[] = slice.map((addr) => ({
//         contract: matrix,
//         functionFragment: "users",
//         args: [addr],
//         allowFailure: false,
//       }));

//       const rawUsers = await mc.aggregate3(userCalls);

//       const levels: number[] = rawUsers.map(([ok, data]) => {
//         if (!ok) return 0;
//         // `data` is the decoded User tuple as an array
//         // [wallet, referrer, id, refsCount, matrix3x6Level]
//         const [, , , , matrix3x6Level] = data;

//         // can also be collapsed into return Number((data as any)[4]);

//         return Number(matrix3x6Level);
//       });

//       // Build only the needed get3x6Entry calls
//       const entryCalls: Call3[] = [];

//       const mapping: { user: string; level: number }[] = [];

//       slice.forEach((user, idx) => {
//         const maxLvl = Math.min(levels[idx], MAX_LEVEL);

//         for (let lvl = 1; lvl <= maxLvl; lvl++) {
//           entryCalls.push({
//             contract: matrix,
//             functionFragment: "get3x6Entry",
//             args: [user, lvl],
//             allowFailure: true,
//           });
//           mapping.push({ user, level: lvl });
//         }
//       });

//       const rawEntries = await mc.aggregate3(entryCalls);

//       // Decode & filter each entry
//       rawEntries.forEach(([ok, ret], idx) => {
//         if (!ok) return;

//         const { userAddress, vault, lastRenewTime } = ret as {
//           userAddress: string;
//           vault: bigint;
//           lastRenewTime: bigint;
//         };

//         const lvl = mapping[idx].level;

//         if (
//           vault >= plan(lvl) + MONTHLY_FEE &&
//           Number(lastRenewTime) <= cutoff
//         ) {
//           dueByLevel[lvl].push(userAddress);
//         }
//       });

//       // log chunk timing
//       log(
//         `  → scanned wallets ${chunkLabel} in ${hrTimeMs(chunkStart).toFixed(
//           2
//         )} ms`
//       );
//     })();

//     tasks.push(job);

//     // once we have MAX_CONCURRENCY slices in flight, wait for them all
//     if (tasks.length >= MAX_CONCURRENCY) {
//       await Promise.all(tasks);
//       tasks.length = 0;
//     }
//   }

//   // finish off any remainder
//   if (tasks.length) {
//     await Promise.all(tasks);
//   }

//   log(
//     `   fetchUsersDueAllLevels took ${hrTimeMs(overallStart).toFixed(2)} ms`
//   );

//   return dueByLevel;
// }

/**
 * Send a single `batchRenew` transaction for a slice of addresses.
 * @param addrs - Up to RENEW_BATCH_SIZE wallet addresses.
 * @param lvl - Matrix level to renew.
 * @returns The transaction hash once mined.
 */
async function renewBatch(addrs: string[], lvl: number): Promise<string> {
  const tx = await matrix.batchRenew(addrs, lvl);
  log(`  [L-${lvl}] tx ${tx.hash} for ${addrs.length} users`);
  await tx.wait();

  return tx.hash;
}

/**
 * If needed, swap leftover USDT to BNB to pay gas. (Optional.)
 */
async function processUSDT(): Promise<void> {
  // Check USDT balance
  const bal: bigint = await usdt.balanceOf(wallet.address);

  if (bal < parseEther("5")) {
    console.log("Low USDT balance");
    return;
  }

  console.log("Converting USDT to BNB");

  const allowance: bigint = await usdt.allowance(wallet.address, DEX_ADDRESS);

  if (bal > allowance) {
    console.log("Granting Approval");
    const approval = await usdt.approve(DEX_ADDRESS, bal * 5n);
    console.log("Transaction hash(approval):", approval.hash);
    await approval.wait();
  } else {
    console.log("Enough Allowance");
  }

  const swapData = {
    fork: "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73",
    referee: "0x9B71B4Dc9E9DCeFAF0e291Cf2DC5135A862A463d",
    fee: true,
  };

  const swap = await dex.swapExactTokensForETH(swapData, bal - 1n, 0, [
    USDT_ADDRESS,
    "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
  ]);

  console.log("Transaction hash(swap):", swap.hash);

  await swap.wait();
}

// ── Main ──────────────────────────────────────────────────────────────────────
/**
 * Main driver:
 * 1) Load/cache all wallets
 * 2) Multi‐level scan for due renewals
 * 3) Batch‐submit renewals at RENEW_BATCH_SIZE
 */
async function main() {
  log(`\n=== Starting renewals run at ${new Date().toISOString()} ===\n`);

  // fetch+cache ALL wallets once
  const allWallets = await fetchAllWallets();

  // await processUSDT();

  // Scan every level in parallel, in chunks
  log("→ fetching due users for all levels…");

  const dueByLevel = await fetchUsersDueAllLevels(allWallets);

  // For each level, submit batchRenew() in groups of RENEW_BATCH_SIZE
  for (let lvl = 1; lvl <= MAX_LEVEL; lvl++) {
    const due = dueByLevel[lvl];
    log(`\n--> Level ${lvl}: ${due.length} due`);

    for (let i = 0; i < due.length; i += RENEW_BATCH_SIZE) {
      const chunk = due.slice(i, i + RENEW_BATCH_SIZE);
      log(`  renewing ${chunk.length} users…`);
      const txHash = await renewBatch(chunk, lvl);
      log(`    → tx ${txHash}`);
    }
  }

  log("✅ All renewals dispatched.");
  log(`=== Finished renewals run at ${new Date().toISOString()} ===\n`);
  // logStream.end();
}

async function sendLogToTelegram() {
  const token = process.env.TELEGRAM_BOT_TOKEN;
  const chatId = process.env.TELEGRAM_CHAT_ID;
  if (!token || !chatId) {
    console.warn("Skipping Telegram send: BOT_TOKEN or CHAT_ID not set");
    return;
  }

  if (!fs.existsSync(LOG_FILE)) {
    console.warn(`No ${LOG_FILE} to send`);
    return;
  }

  await new Promise<void>((resolve) => {
    logStream.end(() => {
      // all buffered writes are now flushed to disk
      resolve();
    });
  });

  // read entire log into memory (fine if it's small)
  const fileBuf = fs.readFileSync(LOG_FILE);

  // Turn Buffer → ArrayBuffer slice (so we don't include the entire Buffer pool)
  const arrayBuf = fileBuf.buffer.slice(
    fileBuf.byteOffset,
    fileBuf.byteOffset + fileBuf.byteLength
  );

  // Wrap in the global Blob
  const blob = new Blob([arrayBuf], { type: "text/plain" });

  const isoDate = new Date().toISOString();

  // now use the built-in FormData
  const form = new FormData();
  form.append("chat_id", chatId);
  form.append("caption", `🔄 HHV2 renewals run @ ${isoDate}`);
  form.append("document", blob, `renewals_log_${isoDate}.txt`);

  const res = await fetch(`https://api.telegram.org/bot${token}/sendDocument`, {
    method: "POST",
    body: form,
  });

  const json = await res.json();

  if (!json.ok) {
    console.error("Telegram API error:", json);
  } else {
    console.log(`✅ Sent renewals_log_${isoDate}.txt to Telegram`);
  }
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

mainWithRetry()
  .catch((e) => console.error("Fatal error:", e))
  .then(async () => {
    await sendLogToTelegram();
  });
