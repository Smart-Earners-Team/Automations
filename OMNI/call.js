const ethers = require('ethers');

const voterABI = require('./ABIs/VoterABI.json');
const minterABI = require('./ABIs/MinterABI.json');

const key = process.env.PRIVATE_KEY;
// const key = "";

// const minterCA = "0x6c55BC74C70578bfdb7704D1fB6BABaa34dccF2c"; // zeta testnet
const minterCA = "0x6B6541d64027ae136fC35Dc58AE65efaACf179C6"; // zeta mainnet
const voterCA = "0x257d1ef4EF5e4b65967d93F4BBEc9e54af4F1610"; // zeta mainnet

const prov = 'https://zetachain-evm.blockpi.network/v1/rpc/public';

// Set up the provider
const provider = new ethers.JsonRpcProvider(prov);

// Create a wallet and signer instance
const wallet = new ethers.Wallet(key, prov);
const signer = wallet.connect(provider);

const minter = new ethers.Contract(minterCA, minterABI, signer);
const voter = new ethers.Contract(voterCA, voterABI, signer);

async function main() {
    const update = await minter.updatePeriod();
    const res = await update.wait();
    console.log('Transaction hash(updatePeriod):', update.hash);

    try {
        // const distribute = await voter.distribute(BigInt(0), BigInt(18));
        const distribute = await voter.distribute(["0x4EA73F285A3D0437720a328208BbD589f34B0661"]);
        const res2 = await distribute.wait();
        console.log('Transaction hash(distribute):', distribute.hash);
    } catch (error) {
        console.log(error)
        //try {
          //  const distribute = await voter.distribute();
          //  const res2 = await distribute.wait();
     //       console.log('Transaction hash(distribute):', distribute.hash);
       // } catch (error) {
      //      console.log(error)
       // }
    }
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

mainWithRetry().catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
});
