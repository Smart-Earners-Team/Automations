const ethers = require('ethers');

// const voterABI = require('./ABIs/VoterABI.json');
const minterABI = require('./ABIs/MinterABI.json');

const key = process.env.PRIVATE_KEY;
// const key = "";

const minterCA = "0x6c55BC74C70578bfdb7704D1fB6BABaa34dccF2c"; // zeta testnet

const prov = 'https://rpc.ankr.com/zetachain_evm_athens_testnet/';

// Set up the provider
const provider = new ethers.JsonRpcProvider(prov);

// Create a wallet and signer instance
const wallet = new ethers.Wallet(key, prov);
const signer = wallet.connect(provider);

const minter = new ethers.Contract(minterCA, minterABI, signer);

async function main() {
    const update = await minter.updatePeriod()
    const res = await update.wait();
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
