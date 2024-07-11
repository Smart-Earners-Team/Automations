const ethers = require("ethers");

const lotteryABI = require("../ABIs/lotteryABI.json");
const randomizerABI = require("../ABIs/randomizerABI.json");

const key = process.env.PRIVATE_KEY;
// const key = "";

const lotteryCA = "0xC1D2eB15b09dE06d1c70559963B731D2f2965d82";
const randomizerCA = "0xC670060dED5057fBeC4D55cCd4446901A3E6E3f0";

const prov = "https://testnet-rpc.ubitscan.io/";

// Set up the provider
const provider = new ethers.JsonRpcProvider(prov);

// Create a wallet and signer instance
const wallet = new ethers.Wallet(key, prov);
const signer = wallet.connect(provider);

const lottery = new ethers.Contract(lotteryCA, lotteryABI, signer);
const randomizer = new ethers.Contract(randomizerCA, randomizerABI, signer);

// function getRandomNumber() {
    // const min = 1000000;
    // const max = 1999999;
    // return Math.floor(Math.random() * (max - min + 1)) + min;
// }

// Example usage
// console.log(getRandomNumber());


async function main() {
  // Fetch the current gas price from the network
  const fees = await signer.provider.getFeeData();
  console.log(fees);
  console.log(
    `Current gas price: ${ethers.formatUnits(fees.gasPrice, "gwei")} gwei`
  );
//   console.log("address: ", wallet.address);

  const callbackGasLimit = BigInt(300000);

  const amountOfGas =
    (fees.gasPrice * callbackGasLimit * BigInt(3)) / BigInt(2);
  console.log(`Amount of gas: ${amountOfGas.toString()}`); // 12374999999977500000

  try {
    const currentRound = await lottery.viewCurrentLotteryId();

    console.log("currentRound: ", String(currentRound));

    const closeTx = await lottery.closeLottery(currentRound, callbackGasLimit, amountOfGas);

    await closeTx.wait();

    console.log('Transaction hash(closeTx):', closeTx.hash);

    // Add a 2-minute delay before proceeding
    await new Promise(resolve => setTimeout(resolve, 120000));

    const drawTx = await lottery.drawFinalNumberAndMakeLotteryClaimable(currentRound, true)

    await drawTx.wait();

    console.log('Transaction hash(drawTx):', drawTx.hash);

    // Get the current date and time
    const now = new Date();

    // Create a new Date object for midnight of the next day in UTC
    const nextZeroHourUTC = new Date(
      Date.UTC(
        now.getUTCFullYear(),
        now.getUTCMonth(),
        now.getUTCDate(),
        0,
        0,
        0
      )
    );

    // Increment the date by one day
    nextZeroHourUTC.setUTCDate(nextZeroHourUTC.getUTCDate() + 1)

    // Get the timestamp of the next zerohour in UTC
    const nextZeroHourTimestampUTC = nextZeroHourUTC.getTime();

    console.log((nextZeroHourTimestampUTC/1000).toFixed());

    const newRoundTx = await lottery.startLottery(
      (nextZeroHourTimestampUTC / 1000).toFixed(),
      ethers.parseEther("0.5"),
      2000,
      [250, 375, 625, 1250, 2500, 5000],
      200
    );
    await newRoundTx.wait();
    console.log("Transaction hash(newRoundTx):", newRoundTx.hash);
  } catch (error) {
    console.log(error);
  }
}

async function mainWithRetry(retryDelay = 300000, maxRetries = 5) {
  // retryDelay is in milliseconds, 300000ms = 5 minutes
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
        await new Promise((resolve) => setTimeout(resolve, retryDelay)); // Wait for 5 minutes before retrying
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
