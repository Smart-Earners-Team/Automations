import "dotenv/config";
import { JsonRpcProvider, Wallet, Contract } from "ethers";
import { ORACLE_CONTRACT } from "./constants";

// ====== CONFIG (env) ======
if (!process.env.VVA_PRIVATE_KEY) {
  console.error("❌  Add VVA_PRIVATE_KEY to your .env file");
  process.exit(1);
}

if (!process.env.VVA_RPC_URL) {
  console.error("❌  Add VVA_RPC_URL to your .env file");
  process.exit(1);
}

const RPC_URL = process.env.VVA_RPC_URL;
const PRIVATE_KEY = process.env.VVA_PRIVATE_KEY;

// ====== ABI (minimal) ======
const ORACLE_ABI = [
  "function secondsUntilUpdate() view returns (uint256)",
  "function update()",
];

async function main() {
  const provider = new JsonRpcProvider(RPC_URL);
  const wallet = new Wallet(PRIVATE_KEY, provider);
  const oracle = new Contract(ORACLE_CONTRACT, ORACLE_ABI, wallet);

  const net = await provider.getNetwork();
  console.log(
    `[init] chainId=${net.chainId.toString()} updater=${wallet.address} oracle=${ORACLE_CONTRACT}`,
  );

  const secondsUntil: bigint = await oracle.secondsUntilUpdate();
  console.log(`[check] secondsUntilUpdate=${secondsUntil.toString()}`);

  if (secondsUntil !== 0n) {
    console.log("[skip] Not ready to update yet. Exiting; cron will retry.");
    return;
  }

  console.log("[send] Calling update()…");
  const tx = await oracle.update();
  console.log(`[tx] submitted=${tx.hash}`);

  const rcpt = await tx.wait();
  if (!rcpt || rcpt.status !== 1) {
    throw new Error("update() tx failed or no receipt");
  }

  console.log(
    `[ok] updated in block=${rcpt.blockNumber} gasUsed=${rcpt.gasUsed.toString()}`,
  );
}

main().catch((e) => {
  console.error("[fatal]", e);
  process.exit(1);
});
