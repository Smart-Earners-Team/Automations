const ethers = require("ethers");

const lotteryABI = require("../ABIs/lotteryABI.json");
const randomizerABI = require("../ABIs/randomizerABI.json");
const pragmaABI = require("../ABIs/PragmaEthFeedABI.json");

const key = process.env.PRIVATE_KEY;

const lotteryCA = "0xCFB024DE4bD45700d53d91CDdF7836348DA3c8CC";
const randomizerCA = "0xbAe61D17445AdD862C306A24d12F41906cc0FFa3";
const pragmaEthFeed = "0x3899D87a02eFaB864C9306DCd2EDe06B90f28B14";

const prov = "https://sepolia-rpc.kakarot.org/";

// Set up the provider
const provider = new ethers.JsonRpcProvider(prov);

// Create a wallet and signer instance
const wallet = new ethers.Wallet(key, prov);
const signer = wallet.connect(provider);

const lottery = new ethers.Contract(lotteryCA, lotteryABI, signer);
const randomizer = new ethers.Contract(randomizerCA, randomizerABI, signer);

// Function to pause execution for a given duration
async function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function getEthPrice() {
  const oracle = new ethers.Contract(pragmaEthFeed, pragmaABI, signer);

  const res = await oracle.latestAnswer();

  return Number(ethers.formatUnits(res, 8));
}

async function closeLottery(currentRound, callbackGasLimit, amountOfGas) {
  try {
    const closeTx = await lottery.closeLottery(
      currentRound,
      callbackGasLimit,
      amountOfGas
    );

    await closeTx.wait();

    console.log("Transaction hash(closeTx):", closeTx.hash);
  } catch (error) {
    console.log("Erroe closing lottery: ", error);
    throw error;
  }
}

async function drawFinalNumberAndMakeLotteryClaimable(currentRound) {
  try {
    const drawTx = await lottery.drawFinalNumberAndMakeLotteryClaimable(
      currentRound,
      true
    );

    await drawTx.wait();

    console.log("Transaction hash(drawTx):", drawTx.hash);
  } catch (error) {
    console.log("Erroe with drawFinalNumberAndMakeLotteryClaimable: ", error);
    throw error;
  }
}

async function startLottery() {
  try {
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
    nextZeroHourUTC.setUTCDate(nextZeroHourUTC.getUTCDate() + 1);

    // Get the timestamp of the next zerohour in UTC
    const nextZeroHourTimestampUTC = nextZeroHourUTC.getTime();

    // console.log((nextZeroHourTimestampUTC / 1000).toFixed());

    let ethPrice = await getEthPrice();
    console.log("ethPrice: ", ethPrice);

    // Calculate how much ETH is worth $5
    const ethForFiveDollars = 5 / ethPrice;

    const newRoundTx = await lottery.startLottery(
      (nextZeroHourTimestampUTC / 1000).toFixed(),
      ethers.parseEther(
        ethForFiveDollars > 0.0015 ? String(ethForFiveDollars) : "0.002"
      ),
      2000,
      [250, 375, 625, 1250, 2500, 5000],
      200
    );
    await newRoundTx.wait();
    console.log("Transaction hash(newRoundTx):", newRoundTx.hash);
  } catch (error) {
    console.log("Erroe starting lottery: ", error);
    throw error;
  }
}

async function closeLotteryWithRetry(
  currentRound,
  callbackGasLimit,
  amountOfGas,
  retryDelay = 5000,
  maxRetries = 3
) {
  // retryDelay is in milliseconds, 60000ms = 1 minute
  let retries = 0;

  while (retries < maxRetries) {
    try {
      await closeLottery(currentRound, callbackGasLimit, amountOfGas);
      // Add a 90 seconds delay before proceeding
      await delay(90000);
      break; // If closeLottery() succeeds, exit the loop
    } catch (error) {
      console.error(error);
      retries++;
      console.log(`closeLottery retry attempt ${retries}...`);
      if (retries < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay)); // Wait for 5 seconds before retrying
      } else {
        console.log("Max retries reached. Skipping closeLottery!");
        break;
      }
    }
  }
}

async function drawFinalNumberAndMakeLotteryClaimableWithRetry(
  currentRound,
  retryDelay = 5000,
  maxRetries = 3
) {
  // retryDelay is in milliseconds, 60000ms = 1 minute
  let retries = 0;

  while (retries < maxRetries) {
    try {
      await drawFinalNumberAndMakeLotteryClaimable(currentRound);
      break; // If main() succeeds, exit the loop
    } catch (error) {
      console.error(error);
      retries++;
      console.log(
        `drawFinalNumberAndMakeLotteryClaimableWithRetry retry attempt ${retries}...`
      );
      if (retries < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay)); // Wait for 5 seconds before retrying
      } else {
        console.log(
          "Max retries reached. Skipping drawFinalNumberAndMakeLotteryClaimableWithRetry!"
        );
        break;
      }
    }
  }
}

async function startLotteryWithRetry(retryDelay = 5000, maxRetries = 3) {
  // retryDelay is in milliseconds, 60000ms = 1 minute
  let retries = 0;

  while (retries < maxRetries) {
    try {
      await startLottery();
      break; // If main() succeeds, exit the loop
    } catch (error) {
      console.error(error);
      retries++;
      console.log(`startLottery retry attempt ${retries}...`);
      if (retries < maxRetries) {
        await new Promise((resolve) => setTimeout(resolve, retryDelay)); // Wait for 5 seconds before retrying
      } else {
        // Max retries reached, throw an error
        console.log("Max retries reached for startLottery. Throwing error.");
        throw new Error("Failed to start lottery after multiple retries.");
      }
    }
  }
}

async function main() {
  // Fetch the current gas price from the network
  const fees = await signer.provider.getFeeData();
  console.log(fees);
  console.log(
    `Current gas price: ${ethers.formatUnits(fees.gasPrice, "gwei")} gwei`
  );

  const callbackGasLimit = BigInt(200000);

  const amountOfGas =
    ((fees.gasPrice + fees.maxFeePerGas) * callbackGasLimit * BigInt(3)) /
    BigInt(2);
  console.log(
    `Amount of gas: ${amountOfGas.toString()} wei - ${ethers.formatEther(
      amountOfGas
    )} ETH`
  ); // 900000000000000 - 0.0009

  try {
    const currentRound = await lottery.viewCurrentLotteryId();

    console.log("currentRound: ", String(currentRound));

    await closeLotteryWithRetry(currentRound, callbackGasLimit, amountOfGas);

    await drawFinalNumberAndMakeLotteryClaimableWithRetry(currentRound);

    await startLotteryWithRetry();
  } catch (error) {
    console.log(error);
  }
}

async function mainWithRetry(retryDelay = 60000, maxRetries = 5) {
  // retryDelay is in milliseconds, 60000ms = 1 minute
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
