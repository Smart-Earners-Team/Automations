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
import { VVA_CONTRACT } from "./constants";
import type { WeekAggregate } from "./blitz-indexer";
import { getBlitzWeek } from "./fetch";

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

// -------------------- TYPES -------------------- //

type PayoutRow = {
  wallet: string;
  shares: bigint;
  amount: bigint;
};

// -------------------- ETHERS SETUP -------------------- //

const provider = new JsonRpcProvider(VVA_RPC_URL);
const wallet = new Wallet(PAYOUT_PRIVATE_KEY, provider);
const signer = new NonceManager(wallet.connect(provider));

const ERC20_ABI = [
  "function transfer(address to, uint256 value) returns (bool)",
  "function balanceOf(address owner) view returns (uint256)",
];

const rewardToken = new Contract(REWARD_TOKEN_ADDRESS, ERC20_ABI, signer);

// -------------------- HELPERS -------------------- //

function readWeekAggregate(weekIndex: number): WeekAggregate {
  const file = path.join(weeksDir, `week_${weekIndex}.json`);
  if (!fs.existsSync(file)) {
    throw new Error(`Week aggregate file not found: ${file}`);
  }
  const raw = fs.readFileSync(file, "utf8");
  return JSON.parse(raw) as WeekAggregate;
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

async function sendPayouts(
  payouts: PayoutRow[],
  opts: { dryRun: boolean; weekIndex: number }
) {
  const { dryRun, weekIndex } = opts;

  console.log(
    `\n=== Blitz Payouts for Week ${weekIndex} (dryRun=${dryRun}) ===`
  );

  if (!payouts.length) {
    console.log("No payouts: zero eligible addresses with shares.");
    return;
  }

  // Log summary table
  for (const row of payouts) {
    console.log(
      `${row.wallet} -> shares=${row.shares.toString()} amount=${formatUnits(
        row.amount,
        TOKEN_DECIMALS
      )}`
    );
  }

  if (dryRun) {
    console.log("\nDry run only – no transactions sent.");
    return;
  }

  console.log(
    `\nSending ${payouts.length} transfers from ${wallet.address} ...`
  );

  for (const row of payouts) {
    if (row.amount === 0n) continue;

    console.log(
      `\n[tx] Week ${weekIndex} → ${row.wallet} : ${formatUnits(
        row.amount,
        TOKEN_DECIMALS
      )} tokens`
    );

    const tx = await rewardToken.transfer(row.wallet, row.amount);
    console.log(`  submitted: ${tx.hash}`);
    const receipt = await tx.wait();
    console.log(`  mined in block ${receipt.blockNumber}`);
  }

  console.log("\nAll payouts submitted.");
}

// -------------------- MAIN -------------------- //

async function main() {
  const [weekArg, ...restArgs] = process.argv.slice(2);
  if (!weekArg) {
    console.error("Usage: tsx blitz-payout.ts <weekIndex> [--dry]");
    process.exit(1);
  }

  const weekIndex = Number(weekArg);
  if (!Number.isInteger(weekIndex) || weekIndex < 0) {
    throw new Error(`Invalid weekIndex: ${weekArg}`);
  }

  const dryRun = restArgs.includes("--dry");

  console.log(`Payer address: ${wallet.address}`);
  console.log(
    `Reward token: ${REWARD_TOKEN_ADDRESS}, weekly pool: ${WEEKLY_POOL_TOKENS} (decimals=${TOKEN_DECIMALS})`
  );

  //   const agg = readWeekAggregate(weekIndex);
  const agg = await getBlitzWeek(weekIndex);
  const { payouts, totalShares, amountPerShare } = computePayouts(agg);

  console.log(
    `\nWeek ${weekIndex} summary: eligible=${
      payouts.length
    }, totalShares=${totalShares.toString()}, amountPerShare=${formatUnits(
      amountPerShare,
      TOKEN_DECIMALS
    )}`
  );

  await sendPayouts(payouts, { dryRun, weekIndex });
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
