const ENDPOINT = process.env.JOB_BASE_URL;
const SECRET = process.env.JOB_CRON_SECRET; // optional

if (!ENDPOINT) {
  console.error("Missing JOB_BASE_URL environment variable");
  process.exit(1);
}

async function one() {
  const res = await fetch(`${ENDPOINT}one`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(SECRET ? { "x-cron-secret": SECRET } : {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed: ${res.status} ${text}`);
  }

  console.log("✅ Job triggered successfully");
}

async function two() {
  const res = await fetch(`${ENDPOINT}two`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      ...(SECRET ? { "x-cron-secret": SECRET } : {}),
    },
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Request failed: ${res.status} ${text}`);
  }

  console.log("✅ Job triggered successfully");
}

one().catch((err) => {
  console.error("❌ Failed to trigger job 1:");
  console.error(err);
  process.exit(1);
});

two().catch((err) => {
  console.error("❌ Failed to trigger job 2:");
  console.error(err);
  process.exit(1);
});
