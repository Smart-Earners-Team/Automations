import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { formatUnits, parseUnits } from "ethers";
import {
  ensureDirSync,
  ensureFileSync,
  readJSON,
  WeekAggregate,
} from "./helpers";

const WEEK0_POOL_STR = "15000";
const TOKEN_DECIMALS = 9;

// BigInt total pool in smallest units
const WEEK0_POOL = parseUnits(WEEK0_POOL_STR, TOKEN_DECIMALS);

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const patchDir = path.join(__dirname, "patch");

const outJsonPath = path.join(patchDir, "week0_corrections.json");

const outCsvPath = path.join(patchDir, "week0_corrections.csv");

const OLD_WEEK_PATH = path.join(__dirname, "data_v1", "weeks", "week_0.json");
const NEW_WEEK_PATH = path.join(__dirname, "data", "weeks", "week_0.json");

function buildSharesMap(weekAgg: WeekAggregate): Record<string, number> {
  const meta = weekAgg.meta || {};
  const out: Record<string, number> = {};
  for (const [addr, info] of Object.entries(meta)) {
    if (!info) continue;
    const s = info.shares ?? 0;
    if (!Number.isFinite(s) || s <= 0) continue;
    out[addr.toLowerCase()] = Number(s);
  }
  return out;
}

function sumShares(map: Record<string, number>, eligSet: Set<string>): number {
  let total = 0;
  for (const addr of eligSet) {
    const s = map[addr] ?? 0;
    if (s > 0) total += s;
  }
  return total;
}

// BigInt-safe mulDiv: floor(amount * num / den)
function mulDivFloor(amountBigInt: bigint, num: number, den: number): bigint {
  if (den === 0) return 0n;
  return (amountBigInt * BigInt(num)) / BigInt(den);
}

function main() {
  ensureDirSync(patchDir);
  ensureFileSync(outJsonPath, {});
  ensureFileSync(outCsvPath);

  console.log("Reading week_0 aggregates...");

  // We know these files exist, so fallback is just a type helper
  const oldWeek0 = readJSON<WeekAggregate>(OLD_WEEK_PATH, {
    week: 0,
    eligibility: [],
    ineligible: [],
    meta: {},
  });

  const newWeek0 = readJSON<WeekAggregate>(NEW_WEEK_PATH, {
    week: 0,
    eligibility: [],
    ineligible: [],
    meta: {},
  });

  const oldElig = new Set(
    (oldWeek0.eligibility || []).map((a) => a.toLowerCase())
  );
  const newElig = new Set(
    (newWeek0.eligibility || []).map((a) => a.toLowerCase())
  );

  const oldSharesMap = buildSharesMap(oldWeek0);
  const newSharesMap = buildSharesMap(newWeek0);

  const allAddrs = new Set(
    [
      ...Object.keys(oldSharesMap),
      ...Object.keys(newSharesMap),
      ...oldElig,
      ...newElig,
    ].map((a) => a.toLowerCase())
  );

  const totalOldShares = sumShares(oldSharesMap, oldElig);
  const totalNewShares = sumShares(newSharesMap, newElig);

  console.log("Total old shares (v1):", totalOldShares);
  console.log("Total new shares (corrected):", totalNewShares);
  console.log("Week 0 reward pool (raw):", WEEK0_POOL.toString());
  console.log(
    "Week 0 reward pool (human):",
    WEEK0_POOL_STR,
    `(decimals=${TOKEN_DECIMALS})`
  );

  if (totalNewShares === 0) {
    console.error(
      "totalNewShares is 0 – nothing to distribute under corrected logic."
    );
    process.exit(1);
  }
  if (totalOldShares === 0) {
    console.warn(
      "WARNING: totalOldShares is 0 – script will treat all original payouts as 0."
    );
  }

  type CorrectionRow = {
    address: string;
    oldShares: number;
    newShares: number;
    deltaShares: number;
    originalAmountRaw: string;
    correctAmountRaw: string;
    correctionAmountRaw: string;
    originalAmount: string;
    correctAmount: string;
    correctionAmount: string;
  };

  const corrections: CorrectionRow[] = [];
  let totalCorrection = 0n;
  let totalUnderpaidShares = 0;
  let affectedAddresses = 0;

  for (const addr of allAddrs) {
    const oldShares = oldElig.has(addr) ? oldSharesMap[addr] ?? 0 : 0;
    const newShares = newElig.has(addr) ? newSharesMap[addr] ?? 0 : 0;

    if (newShares === 0 && oldShares === 0) continue;

    // Correct share-based amount under fixed logic
    const correctAmount = mulDivFloor(WEEK0_POOL, newShares, totalNewShares);

    // Original amount under v1 logic
    const originalAmount =
      totalOldShares > 0
        ? mulDivFloor(WEEK0_POOL, oldShares, totalOldShares)
        : 0n;

    let deltaAmount = correctAmount - originalAmount;

    // We don't claw back overpayments; only pay positive deltas
    if (deltaAmount <= 0n) continue;

    totalCorrection += deltaAmount;
    totalUnderpaidShares += newShares - oldShares;
    affectedAddresses++;

    corrections.push({
      address: addr,
      oldShares,
      newShares,
      deltaShares: newShares - oldShares,
      originalAmountRaw: originalAmount.toString(),
      correctAmountRaw: correctAmount.toString(),
      correctionAmountRaw: deltaAmount.toString(),
      originalAmount: formatUnits(originalAmount, TOKEN_DECIMALS),
      correctAmount: formatUnits(correctAmount, TOKEN_DECIMALS),
      correctionAmount: formatUnits(deltaAmount, TOKEN_DECIMALS),
    });
  }

  // Sort by correction descending for convenience
  corrections.sort((a, b) => {
    const diff = BigInt(b.correctionAmountRaw) - BigInt(a.correctionAmountRaw);
    if (diff > 0n) return 1;
    if (diff < 0n) return -1;
    return 0;
  });

  const outJson = {
    week: 0,
    tokenDecimals: TOKEN_DECIMALS,
    rewardPoolHuman: WEEK0_POOL_STR,
    rewardPoolRaw: WEEK0_POOL.toString(),
    totalOldShares,
    totalNewShares,
    totalUnderpaidShares,
    totalCorrectionRaw: totalCorrection.toString(),
    totalCorrection: formatUnits(totalCorrection, TOKEN_DECIMALS),
    affectedAddresses,
    corrections,
  };

  fs.writeFileSync(outJsonPath, JSON.stringify(outJson, null, 2), "utf8");

  const csvLines = [
    [
      "address",
      "oldShares",
      "newShares",
      "deltaShares",
      "originalAmount",
      "correctAmount",
      "correctionAmount",
      "originalAmountRaw",
      "correctAmountRaw",
      "correctionAmountRaw",
    ].join(","),
    ...corrections.map((c) =>
      [
        c.address,
        c.oldShares,
        c.newShares,
        c.deltaShares,
        c.originalAmount,
        c.correctAmount,
        c.correctionAmount,
        c.originalAmountRaw,
        c.correctAmountRaw,
        c.correctionAmountRaw,
      ].join(",")
    ),
  ];
  fs.writeFileSync(outCsvPath, csvLines.join("\n"), "utf8");

  console.log(`\n[done] Week 0 correction summary:`);
  console.log(`  Affected addresses:      ${affectedAddresses}`);
  console.log(`  Total underpaid shares:  ${totalUnderpaidShares}`);
  console.log(`  Total correction (raw):  ${outJson.totalCorrectionRaw}`);
  console.log(
    `  Total correction (human): ${outJson.totalCorrection} (decimals=${TOKEN_DECIMALS})`
  );
  console.log(`  JSON: ${outJsonPath}`);
  console.log(`  CSV:  ${outCsvPath}`);
}

main();
