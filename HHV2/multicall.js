require("dotenv").config();
const fs = require("fs");
const path = require("path");

const {
  parseEther,
  JsonRpcProvider,
  NonceManager,
  Wallet,
  Contract,
} = require("ethers");

const { Multicall } = require("@evmlord/multicall-sdk");

const CACHE_FILE = path.join(__dirname, "wallets-cache.json");

// ── Config ──
const SECONDS_IN_DAY = 86_400;
const MATRIX_ADDRESS = "0x7E623888B34E3c66C40751f5A4689A8DE56B595C";
// "0x0e950F60387C5d9DE09D5710E135776370c713dc";
const USDT_ADDRESS = "0x3807C468D722aAf9e9A82d8b4b1674E66a12E607";
const DEX_ADDRESS = "0xfD28480E8fABbC1f3D66cF164DFe6B0818249A25";
const PRIVATE_KEY = process.env.HH_PRIVATE_KEY;
const MAX_LEVEL = 6;
const CHAIN_ID = 97;
const MONTHLY_FEE = parseEther("1");
const READ_BATCH_SIZE = 1500; // for fetchAllWallets / fetchUsersDue
const RENEW_BATCH_SIZE = 100; // strictly for the write calls
const RENEW_DURATION = 2 * SECONDS_IN_DAY; //  (≈ 2 days);
const RPC_URL = "https://bsc-testnet.publicnode.com";

// ── ABIs ──────────────────────────────────────────────────────────────────────
const hhABI = require("./ABI.json");
const erc20ABI = require("./ERC20ABI.json");
const dexABI = require("./DEXABI.json");

// ── Setup RPC, Wallet, Contracts ───────────────────────────────────────────────
const provider = new JsonRpcProvider(RPC_URL);
const wallet = new Wallet(PRIVATE_KEY, provider);
// const signer = wallet.connect(provider);
const signer = new NonceManager(wallet.connect(provider));
const matrix = new Contract(MATRIX_ADDRESS, hhABI, signer);
const usdt = new Contract(USDT_ADDRESS, erc20ABI, signer);
const dex = new Contract(DEX_ADDRESS, dexABI, signer);

// ── Helpers ───────────────────────────────────────────────────────────────────
function appendLog(text) {
  const logFilePath = path.join(__dirname, "renewals_log.txt");
  appendLogToFile(logFilePath, text + "\n");
}

/**
 * Cost of level L: 50 USDT × 2^(L-1)  (18-decimals, like ERC-20)
 * @param {number} level – positive integer ≥ 1 && <= 6
 * @returns {bigint}    – value scaled by 1 e18
 */
function plan(level) {
  if (!Number.isInteger(level) || level < 1 || level > 6)
    throw new Error("level must be a positive integer <= 6");

  const base = parseEther("50"); // 50n * 10n ** 18n; // 5e18 in BigInt form
  return base << BigInt(level - 1); // multiply by 2^(level-1)
}

// — load whatever is on disk, or start from an empty array
function loadCachedWallets() {
  try {
    const raw = fs.readFileSync(CACHE_FILE, "utf8");
    return JSON.parse(raw);
  } catch (e) {
    return [];
  }
}

// — atomically overwrite the cache file
function saveCachedWallets(wallets) {
  fs.writeFileSync(CACHE_FILE, JSON.stringify(wallets, null, 2));
  console.log(`🔒 updated cache on disk (${wallets.length} entries)`);
}

// — fetch just a slice of IDs via multicall
async function fetchWalletsRange(startId, endId) {
  const mc = new Multicall({ chainId: CHAIN_ID, provider });
  const calls = [];
  for (let id = startId; id <= endId; id++) {
    calls.push({
      contract: matrix,
      functionFragment: "wallets",
      args: [id],
      allowFailure: false,
    });
  }
  const raw = await mc.aggregate3(calls);
  return raw.map(([ok, addr]) => {
    if (!ok) throw new Error(`wallets(${id}) failed`);
    return addr;
  });
}

/**
 * 1) Load cache from disk
 * 2) Check on-chain totalUsers
 * 3) If the cache is “short”, fetch only the tail end
 * 4) Otherwise reuse cached
 */
async function fetchAllWallets() {
  console.log("⏳ load wallet cache…");
  const cached = loadCachedWallets();
  const stats = await matrix.stats();
  const totalUsers = Number(stats.totalUsers);

  if (cached.length === totalUsers) {
    console.log(`✅ cache is up-to-date (${totalUsers} wallets)`);
    return cached;
  }

  console.log(
    `⚠️ cache has ${cached.length}, but totalUsers = ${totalUsers}. ` +
      `Fetching IDs ${cached.length + 1}–${totalUsers}…`
  );

  const newWallets = [];

  for (
    let start = cached.length + 1;
    start <= totalUsers;
    start += READ_BATCH_SIZE
  ) {
    const end = Math.min(totalUsers, start + READ_BATCH_SIZE - 1);
    const slice = await fetchWalletsRange(start, end);
    newWallets.push(...slice);
    console.log(`  → fetched ${start}–${end}`);
  }

  const all = cached.concat(newWallets);
  saveCachedWallets(all);
  return all;
}

/**
 * Fetch all wallet addresses that must renew **level L** with an on-chain multicall:
 *   accepts a pre‐fetched `wallets` array.
 *
 *   1) stats() → totalUsers
 *   2) chunk [1…totalUsers] into READ_BATCH_SIZE, multicall wallets(id) → address[]
 *   3) for each level, multicall get3x6Entry(user, level) → struct
 *   4) filter by vault ≥ plan(level)+MONTHLY_FEE and lastRenewTime ≤ cutoff
 */
async function fetchUsersDue(level, wallets) {
  const cutoff = Math.floor(Date.now() / 1000) - RENEW_DURATION;
  const renewAmt = plan(level) + MONTHLY_FEE;

  const mc = new Multicall({ chainId: CHAIN_ID, provider });

  // For this level, multicall get3x6Entry(...) in chunks
  const due = [];

  console.log(`⏳ level ${level}: scanning ${wallets.length} wallets…`);

  for (let i = 0; i < wallets.length; i += READ_BATCH_SIZE) {
    const chunk = wallets.slice(i, i + READ_BATCH_SIZE);
    const calls = chunk.map((addr) => ({
      contract: matrix,
      functionFragment: "get3x6Entry",
      args: [addr, level],
      allowFailure: true,
    }));

    const raw = await mc.aggregate3(calls);

    raw.forEach(([ok, decoded]) => {
      if (!ok) return;
      // console.log({ decoded });
      // decoded is a plain JS object with our struct’s keys:
      const vault = BigInt(decoded.vault);
      const lastRenewTime = Number(decoded.lastRenewTime);
      if (vault >= renewAmt && lastRenewTime <= cutoff) {
        due.push(decoded.userAddress);
      }
    });
    console.log(`  → scanned wallets ${i + 1}–${i + chunk.length}`);
  }

  console.log(`✅ level ${level}: ${due.length} users due`);
  return due;
}

// batchRenew helper
async function renewBatch(addrs, lvl) {
  const tx = await matrix.batchRenew(addrs, lvl);
  console.log(`  [L-${lvl}] tx ${tx.hash} for ${addrs.length} users`);
  await tx.wait();

  return tx.hash;
}

async function processUSDT() {
  // Check USDT balance
  const bal = await usdt.balanceOf(wallet.address);

  if (bal >= parseEther("5")) {
    console.log("Converting USDT to BNB");
    const allowance = await usdt.allowance(wallet.address, DEX_ADDRESS);
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

    const swap = await dex.swapExactTokensForETH(
      swapData,
      BigInt(bal) - BigInt(1),
      0,
      [USDT_ADDRESS, "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"]
    );

    console.log("Transaction hash(swap):", swap.hash);

    await swap.wait();
  } else {
    console.log("Low USDT balance");
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  appendLog(`\n=== Starting renewals run at ${new Date().toISOString()} ===`);

  // fetch+cache ALL wallets once
  const allWallets = await fetchAllWallets();

  // await processUSDT();

  for (let lvl = 1; lvl <= MAX_LEVEL; lvl++) {
    console.log(`\n--> Level ${lvl}: fetching due users…`);
    const due = await fetchUsersDue(lvl, allWallets);
    console.log(`Found ${due.length} users due at level ${lvl}`);
    appendLog(`Level ${lvl}: ${due.length} due`);

    if (due.length === 0) {
      appendLog(`Level ${lvl}: no users due, skipping.`);
      continue;
    }

    // batch on-chain calls
    for (let i = 0; i < due.length; i += RENEW_BATCH_SIZE) {
      const chunk = due.slice(i, i + RENEW_BATCH_SIZE);
      console.log(
        `Renewing ${chunk.length} users at level ${lvl}`
        // [${i}–${
        //   i + chunk.length - 1
        // }]
      );
      try {
        const txHash = "txHash"; // await renewBatch(chunk, lvl);

        console.log("→ TX (renewals):", txHash);
        appendLog(`batchRenew lvl=${lvl} idx=${i}: ${txHash}`);
      } catch (err) {
        console.error("Error in batchRenew:", err);
        appendLog(`ERROR batchRenew lvl=${lvl} idx=${i}: ${err.message}`);
      }
    }
  }

  console.log("✅ All renewals dispatched.");
  appendLog(`=== Finished renewals run at ${new Date().toISOString()} ===\n`);
}

/**
 * Split an array into equally‐sized chunks.
 */
function chunkArray(arr, size) {
  const chunks = [];
  for (let i = 0; i < arr.length; i += size) {
    chunks.push(arr.slice(i, i + size));
  }
  return chunks;
}

function appendLogToFile(filePath, text) {
  fs.appendFile(filePath, text, (err) => {
    if (err) {
      console.error("Error writing to log file", err);
    }
    // else {
    //   console.log("Log data appended to file");
    // }
  });
}

// ── retry wrapper ─────────────────────────────────────────────────────────────
async function mainWithRetry(retryDelay = 60000, maxRetries = 5) {
  // retryDelay is in milliseconds, 300000ms = 5 minutes
  let retries = 0;

  while (retries < maxRetries) {
    try {
      await main();
      break; // If main() succeeds, exit the loop
    } catch (error) {
      console.error(error);
      appendLog(`Unhandled error: ${error}`);

      retries++;
      console.log(`Retry attempt ${retries}...`);

      if (retries < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay)); // Wait for 1 minute before retrying
      } else {
        console.log("Max retries reached. Exiting.");
        process.exit(1);
      }
    }
  }
}

mainWithRetry().catch((error) => {
  console.error("Unhandled error:", error);
  process.exit(1);
});
