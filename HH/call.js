const ethers = require('ethers');
const fs = require('fs');
const path = require('path');

const hhABI = require("./ABI.json");
const erc20ABI = require("./ERC20ABI.json");
const dexABI = require("./DEXABI.json");
const proxyABI = require("./PROXY.json");

const key = process.env.PRIVATE_KEY;
// const key = "";
const hhCA = '0x178365Ee4Cd481321C8aB3Aef71979b510cab643';
const usdtCA = '0x55d398326f99059fF775485246999027B3197955';
const dexCA = '0xfD28480E8fABbC1f3D66cF164DFe6B0818249A25';
const proxyCA = '0xbc1f6Ba807bCdf61769e88cA554bD4b804AF36a0';
// const prov = 'https://bsc-testnet.publicnode.com';
const prov = 'https://bsc-dataseed.binance.org/';


// Set up the provider
const provider = new ethers.JsonRpcProvider(prov);

// Create a wallet and signer instance
const wallet = new ethers.Wallet(key, prov);
const signer = wallet.connect(provider);
const hh1 = new ethers.Contract(hhCA, hhABI, provider);
const hh2 = new ethers.Contract(hhCA, hhABI, signer);
const USDT1 = new ethers.Contract(usdtCA, erc20ABI, provider);
const USDT2 = new ethers.Contract(usdtCA, erc20ABI, signer);
const dex = new ethers.Contract(dexCA, dexABI, signer);
const proxy = new ethers.Contract(proxyCA, proxyABI, provider);

async function main() {

  // Check USDT balance
  const bal = await USDT1.balanceOf(wallet.address);
  if (BigInt(bal) >= BigInt(5) * (BigInt(10) ** BigInt(18))) {
    console.log("Converting USDT to BNB")
    const allowance = await USDT1.allowance(wallet.address, dexCA);
    if (BigInt(bal) > BigInt(allowance)) {
      console.log("Granting Approval")
      const approval = await USDT2.approve(dexCA, BigInt(bal));
      console.log('Transaction hash(approval):', approval.hash);
      await approval.wait();
    } else {
      console.log("Enough Allowance")
    }

    const swapData = {
      fork: '0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73',
      referee: '0x9B71B4Dc9E9DCeFAF0e291Cf2DC5135A862A463d',
      fee: true
    };

    const swap = await dex.swapExactTokensForETH(swapData, (BigInt(bal) - BigInt(1)), 0, [usdtCA, "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c"]);
    console.log('Transaction hash(swap):', swap.hash);
    await swap.wait();
  } else {
    console.log("Low USDT balance")
  }

  const usersFit4RenewalRN = await proxy.usersFit4RenewalRN();
  console.log("usersFit4RenewalRN: ", usersFit4RenewalRN)

  if (BigInt(usersFit4RenewalRN) > BigInt(0)) {
    // Call the smart contract function
    const tx = await hh2.autoReNew();
    console.log('Transaction hash(renewals):', tx.hash);

    // Wait for the transaction to be mined
    const receipt = await tx.wait();
    console.log('Transaction was mined in block number', receipt.blockNumber);

  }

  const gVar = await hh1.globalVariables();
  const globalVar = {
    date: new Date().toLocaleString(),
    members: ethers.toNumber(gVar[0]),
    matrixRewardsPaid: `${ethers.formatUnits(gVar[1], 18)} USDT`,
    dailyRewardsPaid: `${ethers.formatUnits(gVar[2], 18,)} USDT`,
    referralRewardsPaid: `${ethers.formatUnits(gVar[3], 18)} USDT`,
    renewalsProcessed: ethers.toNumber(gVar[4]),
  }
  console.log(globalVar)

  const logString = `Date: ${globalVar.date}\nMembers: ${globalVar.members}\nMatrix Rewards Paid: ${globalVar.matrixRewardsPaid}\nDaily Rewards Paid: ${globalVar.dailyRewardsPaid}\nReferral Rewards Paid: ${globalVar.referralRewardsPaid}\nRenewals Processed: ${globalVar.renewalsProcessed}\n\n`;
  const logFilePath = path.join(__dirname, 'results_log.txt');
  appendLogToFile(logFilePath, logString);
}

function appendLogToFile(filePath, text) {
  fs.appendFile(filePath, text, (err) => {
    if (err) {
      console.error('Error writing to log file', err);
    } else {
      console.log('Log data appended to file');
    }
  });
}

async function mainWithRetry(retryDelay = 300000, maxRetries = 5) { // retryDelay is in milliseconds, 300000ms = 5 minutes
  let retries = 0;

  while (retries < maxRetries) {
    try {
      await main();
      break; // If main() succeeds, exit the loop
    } catch (error) {
      console.error(error);
      retries++;
      console.log(`Retry attempt ${retries}...`);
      if (retries < maxRetries) {
        await new Promise(resolve => setTimeout(resolve, retryDelay)); // Wait for 5 minutes before retrying
      } else {
        console.log('Max retries reached. Exiting.');
        process.exit(1);
      }
    }
  }
}

// main().catch(error => {
//   console.error(error);
//   process.exit(1);
// });

mainWithRetry().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});