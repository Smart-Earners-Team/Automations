import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  JsonRpcProvider,
  Wallet,
  Contract,
  parseUnits,
  formatUnits,
  toBigInt,
  NonceManager,
} from "ethers";
import { DISTRIBUTOR_CONTRACT, VVA_CONTRACT } from "./constants";
import { getWeekIndex, toBN } from "./blitz-indexer";
import {
  ensureDirSync,
  readJSON,
  type WeekAggregate,
  writeJSON,
} from "./helpers";

// import { getBlitzWeek } from "./fetch";

// -------------------- ENV / CONSTANTS -------------------- //

const VVA_RPC_URL = process.env.VVA_RPC_URL;
if (!VVA_RPC_URL) {
  throw new Error("Missing VVA_RPC_URL env");
}

// Private key of the wallet that holds the weekly reward pool
const PAYOUT_PRIVATE_KEY = process.env.VVA_PRIVATE_KEY;
if (!PAYOUT_PRIVATE_KEY) {
  throw new Error("Missing VVA_PRIVATE_KEY env");
}

// Reward token address.
const REWARD_TOKEN_ADDRESS = VVA_CONTRACT;

// Weekly pool amount, in whole tokens; default 15000 VVA with 9 decimals
const WEEKLY_POOL_TOKENS = process.env.BLITZ_WEEKLY_POOL ?? "15000";
const TOKEN_DECIMALS = Number(process.env.REWARD_TOKEN_DECIMALS ?? "9");

const WEEKLY_POOL = parseUnits(WEEKLY_POOL_TOKENS, TOKEN_DECIMALS);

// -------------------- FILES / PATHS -------------------- //

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const dataDir = path.join(__dirname, "data");
const weeksDir = path.join(dataDir, "weeks");
const payoutsDir = path.join(dataDir, "payouts");

function payoutLogFile(weekIndex: number) {
  return path.join(payoutsDir, `week_${weekIndex}_payouts.json`);
}

// -------------------- TYPES -------------------- //

type PayoutRow = {
  wallet: string;
  shares: bigint;
  amount: bigint; // token amount in smallest unit
};

type WeekPayoutEntry = {
  wallet: string;
  shares: string;
  amount: string;
  txHash: string;
  batchIndex: number;
  timestamp: number;
};

type WeekPayoutLog = {
  week: number;
  totalPaid: string; // sum of amounts
  entries: WeekPayoutEntry[];
};

// -------------------- ETHERS SETUP -------------------- //

const provider = new JsonRpcProvider(VVA_RPC_URL);
const wallet = new Wallet(PAYOUT_PRIVATE_KEY, provider);
const signer = new NonceManager(wallet.connect(provider));

const DISTRIBUTOR_ABI = [
  "function sendErc20(address _tokenAddress, address[] _to, uint256[] _value) payable returns (bool _success)",
];

// --- ERC20 with allowance ---
const TOKEN_ABI = [
  "function balanceOf(address) view returns (uint256)",
  "function allowance(address owner, address spender) view returns (uint256)",
  "function approve(address spender, uint256 amount) returns (bool)",
];

const distributor = new Contract(DISTRIBUTOR_CONTRACT, DISTRIBUTOR_ABI, signer);
const rewardToken = new Contract(REWARD_TOKEN_ADDRESS, TOKEN_ABI, signer);

// -------------------- HELPERS -------------------- //

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * Decide which week to pay out, given "now".
 * We consider the "just ended week" to be (currentWeekIndex - 1).
 * Example: if now is in week 3, we pay week 2.
 */
function computePayoutWeek(nowTs: number = Math.floor(Date.now() / 1000)) {
  const currentWeek = getWeekIndex(nowTs);
  const candidate = currentWeek - 1;
  return candidate < 0 ? 0 : candidate;
}

function readWeekAggregate(weekIndex: number): WeekAggregate {
  const file = path.join(weeksDir, `week_${weekIndex}.json`);
  if (!fs.existsSync(file)) {
    throw new Error(`Week aggregate file not found: ${file}`);
  }
  const raw = fs.readFileSync(file, "utf8");
  return JSON.parse(raw) as WeekAggregate;
}

function loadWeekPayoutLog(weekIndex: number): WeekPayoutLog {
  ensureDirSync(payoutsDir);
  const file = payoutLogFile(weekIndex);
  return readJSON<WeekPayoutLog>(file, {
    week: weekIndex,
    totalPaid: "0",
    entries: [],
  });
}

function saveWeekPayoutLog(weekIndex: number, log: WeekPayoutLog) {
  const file = payoutLogFile(weekIndex);
  writeJSON(file, log);
}

function computePayouts(agg: WeekAggregate): {
  payouts: PayoutRow[];
  totalShares: bigint;
  amountPerShare: bigint;
} {
  const meta = agg.meta ?? {};
  const eligible = agg.eligibility ?? [];

  const rows: { wallet: string; shares: bigint }[] = [];

  for (const addr of eligible) {
    const bag = meta[addr.toLowerCase()];
    const sharesNum = bag?.shares ?? 0;
    if (!sharesNum) continue; // safety: ignore zero-share eligibles

    rows.push({
      wallet: addr,
      shares: BigInt(sharesNum),
    });
  }

  if (rows.length === 0) {
    return { payouts: [], totalShares: 0n, amountPerShare: 0n };
  }

  const totalShares = rows.reduce((acc, r) => acc + r.shares, 0n as bigint);

  if (totalShares === 0n) {
    return { payouts: [], totalShares: 0n, amountPerShare: 0n };
  }

  // Integer division; remainder stays in the payer wallet (or can be handled separately)
  const amountPerShare = toBigInt(WEEKLY_POOL) / totalShares;

  const payouts: PayoutRow[] = rows.map((r) => ({
    wallet: r.wallet,
    shares: r.shares,
    amount: r.shares * amountPerShare,
  }));

  return { payouts, totalShares, amountPerShare };
}

async function ensureDistributorAllowance(required: bigint) {
  if (required <= 0n) return;

  const owner = await wallet.getAddress();
  const current: bigint = await rewardToken.allowance(
    owner,
    DISTRIBUTOR_CONTRACT
  );

  if (current >= required) {
    console.log(
      `[allowance] current=${current.toString()} >= required=${required.toString()} → ok`
    );
    return;
  }

  console.log(
    `[allowance] insufficient: current=${current.toString()}, required=${required.toString()}`
  );

  // some ERC20s require setting to 0 before changing allowance
  if (current > 0n) {
    console.log("[allowance] resetting allowance to 0 first…");
    const tx0 = await rewardToken.approve(DISTRIBUTOR_CONTRACT, 0n);
    console.log(`  approve(0) tx: ${tx0.hash}`);
    await tx0.wait();
  }

  console.log(
    `[allowance] approving distributor for ${required.toString()} tokens…`
  );
  const tx = await rewardToken.approve(DISTRIBUTOR_CONTRACT, required);
  console.log(`  approve(required) tx: ${tx.hash}`);
  await tx.wait();
}

async function sendPayouts(
  allPayouts: PayoutRow[],
  opts: { dryRun: boolean; weekIndex: number },
  maxBatch: number = 200
) {
  const { dryRun, weekIndex } = opts;

  console.log(
    `\n=== Blitz Payouts for Week ${weekIndex} (dryRun=${dryRun}) ===`
  );

  if (!allPayouts.length) {
    console.log("No payouts computed for this week.");
    return;
  }

  // ---- Load log & filter out already-paid addresses ----
  let log = loadWeekPayoutLog(weekIndex);
  const alreadyPaid = new Set(log.entries.map((e) => e.wallet.toLowerCase()));

  const payouts = allPayouts.filter(
    (p) => !alreadyPaid.has(p.wallet.toLowerCase()) && p.amount > 0n
  );

  if (!payouts.length) {
    console.log(
      `All ${allPayouts.length} computed payouts for week ${weekIndex} are already logged as paid. Nothing to do.`
    );
    return;
  }

  // print summary
  console.log(
    `Have ${allPayouts.length} computed payouts; ${alreadyPaid.size} already paid, ${payouts.length} to send now.`
  );
  for (const row of payouts) {
    console.log(
      `${row.wallet} -> shares=${row.shares.toString()} amount=${formatUnits(
        row.amount,
        TOKEN_DECIMALS
      )}`
    );
  }

  if (dryRun) {
    console.log("\nDry run only – no transactions sent, log unchanged.");
    return;
  }

  // ---- Ensure allowance for total unpaid amount ----
  const totalRequired: bigint = payouts.reduce(
    (acc, p) => acc + p.amount,
    0n as bigint
  );

  console.log(
    `\nTotal required this run: ${formatUnits(
      totalRequired,
      TOKEN_DECIMALS
    )} tokens`
  );

  await ensureDistributorAllowance(totalRequired);

  const nonZeroPayouts = payouts.filter((p) => p.amount > 0n);
  const batches = chunk(nonZeroPayouts, maxBatch);

  console.log(
    `Sending ${payouts.length} payouts in ${batches.length} distributor batches (max ${maxBatch} per tx)…`
  );

  for (let i = 0; i < batches.length; i++) {
    const batch = batches[i];
    const to = batch.map((r) => r.wallet);
    const values = batch.map((r) => r.amount);

    const totalBatchAmount = values.reduce((acc, v) => acc + v, 0n as bigint);

    console.log(
      `\n[batch ${i + 1}/${batches.length}] recipients=${
        batch.length
      }, totalAmount=${formatUnits(totalBatchAmount, TOKEN_DECIMALS)}`
    );
    console.log(`  calling distributor.sendErc20(...)`);

    // ethers v6: last arg is overrides (for msg.value, gasLimit, etc.)
    const tx = await distributor.sendErc20(REWARD_TOKEN_ADDRESS, to, values, {
      value: 0,
    });

    console.log(`  submitted: ${tx.hash}`);
    const receipt = await tx.wait();
    console.log(`  mined in block ${receipt.blockNumber}`);

    // ---- 4) Append to week payout log ----
    const now = Date.now();
    for (const row of batch) {
      const entry: WeekPayoutEntry = {
        wallet: row.wallet.toLowerCase(),
        shares: row.shares.toString(),
        amount: row.amount.toString(),
        txHash: tx.hash,
        batchIndex: i,
        timestamp: now,
      };
      log.entries.push(entry);
      alreadyPaid.add(entry.wallet);
      log.totalPaid = (toBN(log.totalPaid) + row.amount).toString();
    }

    saveWeekPayoutLog(weekIndex, log);
  }

  console.log(
    `\nDone. Week ${weekIndex} payouts logged in ${payoutLogFile(weekIndex)}`
  );
}

// -------------------- MAIN -------------------- //

async function main() {
  const args = process.argv.slice(2);

  let weekOverride: number | undefined;
  let autoWeek = false;
  let dryRun = false;

  for (let i = 0; i < args.length; i++) {
    const a = args[i];
    if (a === "--week" && args[i + 1]) {
      weekOverride = Number(args[++i]);
    } else if (a === "--auto-week") {
      autoWeek = true;
    } else if (a === "--dry") {
      dryRun = true;
    }
  }

  if (weekOverride == null && !autoWeek) {
    throw new Error(
      "Missing week selection. Use either `--week <N>` or `--auto-week` (just-ended week)."
    );
  }

  const nowTs = Math.floor(Date.now() / 1000);
  const currentWeek = getWeekIndex(nowTs);

  const MAX_WEEK = 5; // campaign weeks: 0..5 (6 weeks total)
  const weekToPay =
    weekOverride != null ? weekOverride : computePayoutWeek(nowTs);

  if (weekToPay < 0 || weekToPay > MAX_WEEK) {
    console.log(
      `[payout] week ${weekToPay} is outside campaign range [0, ${MAX_WEEK}], aborting.`
    );
    return;
  }

  // *** HARD GUARD: never pay a week that isn't finished yet ***
  if (weekToPay >= currentWeek) {
    throw new Error(
      `Refusing to pay week ${weekToPay}: currentWeek=${currentWeek}. ` +
        `You can only pay weeks < currentWeek (finished weeks).`
    );
  }

  console.log(
    `[payout] nowTs=${nowTs}, currentWeek=${currentWeek}, targetWeek=${weekToPay}, dry=${dryRun}`
  );

  console.log(`Payer address: ${wallet.address}`);
  console.log(
    `Reward token: ${REWARD_TOKEN_ADDRESS}, weekly pool: ${WEEKLY_POOL_TOKENS} (decimals=${TOKEN_DECIMALS})`
  );

  const agg = readWeekAggregate(weekToPay);
  // const agg = await getBlitzWeek(weekToPay);
  const { payouts, totalShares, amountPerShare } = computePayouts(agg);

  console.log(
    `\nWeek ${weekToPay} summary: eligible=${
      payouts.length
    }, totalShares=${totalShares.toString()}, amountPerShare=${formatUnits(
      amountPerShare,
      TOKEN_DECIMALS
    )}`
  );

  await sendPayouts(payouts, { dryRun, weekIndex: weekToPay });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
