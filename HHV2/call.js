const fs = require("fs");
const path = require("path");
// const dotenv = require("dotenv");

const {
  parseEther,
  JsonRpcProvider,
  NonceManager,
  Wallet,
  Contract,
} = require("ethers");
const { GraphQLClient, gql } = require("graphql-request");

// dotenv.config();

// ── Config ──
const SECONDS_IN_DAY = 86_400;
const SUBGRAPH_URL = process.env.HH10X_SUBGRAPH_DEMO;
const MATRIX_ADDRESS = "0x0e950F60387C5d9DE09D5710E135776370c713dc";
const USDT_ADDRESS = "0x3807C468D722aAf9e9A82d8b4b1674E66a12E607";
const DEX_ADDRESS = "0xfD28480E8fABbC1f3D66cF164DFe6B0818249A25";
const PRIVATE_KEY = process.env.HH_PRIVATE_KEY;
const MAX_LEVEL = 6;
const MONTHLY_FEE = parseEther("1");
const BATCH_SIZE = 100;
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

// GraphQL client
const graphQLClient = new GraphQLClient(SUBGRAPH_URL);

// ── GraphQL Query ─────────────────────────────────────────────────────────────
/**
 * Build a GraphQL query whose where-clause uses the
 *   renewVault{L} / lastRenewed{L}
 * columns produced by the subgraph schema.
 *
 * Note: only the *field names* need to be inlined.  The *values* can
 * still be sent through GraphQL variables, which keeps the query
 * compact and avoids string-escaping very large BigInt literals.
 */
function buildUsersDueQuery(level) {
  const vaultField = `renewVault${level}_gte: $minVault`;
  const renewedField = `lastRenewed${level}_lte: $deadline`;

  return gql`
    query UsersDue(
      $minVault:  BigInt!
      $deadline:  BigInt!
      $first:     Int!
      $skip:      Int!
    ) {
      userInfos(
        where: { ${vaultField}, ${renewedField} }
        first: $first
        skip:  $skip
      ) {
        id
      }
    }
  `;
}

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

/**
 * Fetch all wallet addresses that must renew **level L**.
 *
 * • Uses the dynamic query above so the right columns are filtered.
 * • Pagination is unchanged (still uses $first / $skip).
 */
async function fetchUsersDue(level) {
  const nowSec = Math.floor(Date.now() / 1000);
  const deadline = (nowSec - RENEW_DURATION).toString();

  let all = [];
  let skip = 0,
    guard = 0;

  const renewAmt = (plan(level) + MONTHLY_FEE).toString(); // BigInt → string

  while (true) {
    if (++guard > 100) throw new Error("GraphQL pagination overflow");

    const query = buildUsersDueQuery(level);

    const vars = {
      minVault: renewAmt,
      deadline,
      first: BATCH_SIZE,
      skip,
    };

    let res;

    try {
      res = await graphQLClient.request(query, vars);
      console.log({ res });
    } catch (e) {
      console.error("GraphQL query failed:", e);
      appendLog(`GraphQL ERROR level=${lvl}: ${e.message}`);
      throw e;
    }

    const batch = res.userInfos.map((u) => u.id);
    if (batch.length === 0) break;
    all.push(...batch);
    skip += batch.length;
  }

  return all;
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

  // await processUSDT();

  for (let lvl = 1; lvl <= MAX_LEVEL; lvl++) {
    console.log(`\n--> Level ${lvl}: fetching due users…`);
    const due = await fetchUsersDue(lvl);
    console.log(`Found ${due.length} users due at level ${lvl}`);
    appendLog(`Level ${lvl}: ${due.length} due`);

    if (due.length === 0) {
      appendLog(`Level ${lvl}: no users due, skipping.`);
      continue;
    }

    // batch on-chain calls
    for (let i = 0; i < due.length; i += BATCH_SIZE) {
      const chunk = due.slice(i, i + BATCH_SIZE);
      console.log(
        `Renewing ${chunk.length} users [${i}–${
          i + chunk.length - 1
        }] at level ${lvl}`
      );
      try {
        const txHash = await renewBatch(chunk, lvl);

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
