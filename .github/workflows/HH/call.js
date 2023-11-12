const ethers = require('ethers');

const hhABI = require("./ABI.json");

const key = process.env.PRIVATE_KEY;
const hhCA = '0xEE8EB4eec272e6Cb00B665345B5c9d95Ae0645C8';
const prov = 'https://bsc-testnet.publicnode.com';

// Set up the provider
const provider = new ethers.JsonRpcProvider(prov);

// Create a wallet and signer instance
const wallet = new ethers.Wallet(key, prov);
const signer = wallet.connect(provider);
const hh = new ethers.Contract(hhCA, hhABI, signer);

async function main() {
  // Call the smart contract function
  // And add any required parameters for the function call
  const tx = await hh.autoReNew(); 
  console.log('Transaction hash:', tx.hash);

  // Wait for the transaction to be mined
  const receipt = await tx.wait();
  console.log('Transaction was mined in block number', receipt.blockNumber);
}

main().catch(error => {
  console.error(error);
  process.exit(1);
});
