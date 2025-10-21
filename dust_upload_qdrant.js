import axios from "axios";
import crypto from "crypto";
import dotenv from "dotenv";

dotenv.config();

const QDRANT_URL = "http://localhost:6333";
const COLLECTION = "financial_reports_2025";
const DUST_API_URL = process.env.DUST_API_URL;
const DUST_API_KEY = process.env.DUST_API_KEY;
const DUST_DATA_SOURCE_ID = process.env.DUST_DATA_SOURCE_ID;

if (!DUST_API_URL || !DUST_API_KEY || !DUST_DATA_SOURCE_ID) {
  throw new Error("Missing required environment variables. Check your .env file.");
}

async function fetchQdrantPoints() {
  let points = [];
  let offset = 0;

  while (true) {
    try {
      const res = await axios.post(`${QDRANT_URL}/collections/${COLLECTION}/points/scroll`, {
        limit: 100,
        offset,
      });

      const fetched = res.data.result.points || [];
      points = points.concat(fetched);

      if (!res.data.result.next_page_offset) break;
      offset = res.data.result.next_page_offset;
    } catch (err) {
      console.error("Error fetching from Qdrant:", err.response?.data || err.message);
      break;
    }
  }

  console.log(`Fetched ${points.length} points from Qdrant`);
  return points;
}

async function upsertToDust(point) {
  const docId = crypto.createHash("sha256").update(point.payload.chunk_id).digest("hex");

  const payload = {
    text: point.payload.text,
    metadata: {
      company: point.payload.company,
      year: point.payload.year,
      page_number: point.payload.page_number,
    },
  };

  try {
    await axios.post(
      `${DUST_API_URL}/${DUST_DATA_SOURCE_ID}/documents/${docId}`,
      payload,
      {
        headers: {
          Authorization: `Bearer ${DUST_API_KEY}`,
          "Content-Type": "application/json",
        },
      }
    );
    console.log(`Upserted document: ${docId}`);
  } catch (err) {
    console.error("Error upserting to Dust:", err.response?.data || err.message);
  }
}

async function syncQdrantToDust() {
  console.log("Starting sync between Qdrant and Dust...");
  const points = await fetchQdrantPoints();

  for (const point of points) {
    await upsertToDust(point);
  }

  console.log("All documents upserted successfully to Dust.");
}

syncQdrantToDust().catch((err) => {
  console.error("Sync process failed:", err);
  process.exit(1);
});
