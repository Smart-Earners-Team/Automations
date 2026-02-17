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

// Initialize with an immediately invoked async function
(async () => {
  const results = await Promise.allSettled([one(), two()]);

  results.forEach((result, index) => {
    if (result.status === "rejected") {
      console.error(`❌ Job ${index + 1} failed:`);
      console.error(result.reason);
    }
  });

  if (results.some((r) => r.status === "rejected")) {
    process.exit(1);
  }

  console.log("✅ All jobs completed");
})();
