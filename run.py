import os
import re
import unicodedata
import time
import json
import threading
import requests
from google import genai  # SDK baru: google-genai


# ============================
# KONFIG
# ============================
API_FILE = "api.txt"  # semua API key di sini
JOBS_API_URL = "https://domainmu.com/jobs_api.php"  # GANTI ke URL jobs_api.php kamu

APIS_PER_JOB = 5                  # 1 job pakai 5 API key = 5 thread
MIN_SECONDS_PER_REQUEST = 30      # delay per request per thread
MAX_RETRIES_PER_TITLE = 3
DEFAULT_QUOTA_SLEEP_SECONDS = 60


# ============================
# FUNGSI BANTUAN
# ============================
def slugify(text: str) -> str:
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text)
    text = text.strip("-")
    text = text.lower()
    return text or "article"


def parse_retry_delay_seconds(err_str: str) -> float:
    m = re.search(r"retry in ([0-9.]+)s", err_str)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return DEFAULT_QUOTA_SLEEP_SECONDS


def build_prompt(judul: str) -> str:
    judul_safe = judul.replace('"', "'")
    prompt = f"""
ABSOLUTELY NO <h1> TAG ALLOWED. START WITH <p> OR OUTPUT IS USELESS.

You are a professional SEO content writer.. 
Your articles regularly hit position #1–3 on Google because they are helpful, authoritative, and feel genuinely human.

Main title to write about: "{judul_safe}"

Your task:
Write one complete, high-quality SEO article in English that perfectly satisfies Google’s E-E-A-T guidelines.

Do these steps internally (never show them in the output):
1. Create 10 alternative, more clickable title variations (for your reference only).
2. Build a logical, value-packed outline with at least 7–9 H2 sections before FAQ & Conclusion.
3. Research/recall the most recent 2024–2025 data, statistics, tools, or trends related to the topic.

STRICT WRITING RULES YOU MUST FOLLOW:
- Write in a warm, conversational yet authoritative tone — like a trusted expert talking directly to the reader.
- Use “you” frequently to make it personal and engaging.
- Naturally weave in real-world experience or observations.
- Use smooth transitions (however, here’s the thing, the good news is, interestingly, for example, etc.).
- Keep passive voice under 8%.
- Avoid keyword stuffing — use the main keyword and related terms naturally.
- Every section must deliver real value; no fluff.
- When using lists, make them numbered H3s (1., 2., 3…) and explain each item in depth.
- Include up-to-date facts, statistics, tools, or case studies where relevant.
- Opening paragraph: instantly engaging, data-rich or insight-rich, no rhetorical questions.

REQUIRED STRUCTURE:
- Strong introduction
- Logical H2 sections
- Use numbered <h3> for lists inside sections
- End with exactly these two sections:
  <h2>FAQ</h2>
  <h2>Conclusion</h2>

OUTPUT FORMAT:
1. ONLY the clean article HTML (no <html>, <head>, or <body>).
2. After the HTML, add one blank line, lalu:
   META_DESC: your compelling meta description (145–160 characters, plain text, no quotes)

Now write the best possible article for this title:
"{judul_safe}"
"""
    return prompt


# ============================
# LOAD SEMUA API KEY
# ============================
if not os.path.exists(API_FILE):
    raise FileNotFoundError(f"File {API_FILE} tidak ditemukan.")

with open(API_FILE, "r", encoding="utf-8") as f:
    all_api_keys = [line.strip() for line in f if line.strip()]

if not all_api_keys:
    raise ValueError("File api.txt kosong atau tidak berisi API key yang valid.")

# GROUP_INDEX dari matrix GitHub Actions (0,1,2,...)
group_index_str = os.getenv("GROUP_INDEX", "0")
try:
    GROUP_INDEX = int(group_index_str)
except ValueError:
    raise ValueError(f"GROUP_INDEX bukan integer valid: {group_index_str}")

start_idx = GROUP_INDEX * APIS_PER_JOB
end_idx = start_idx + APIS_PER_JOB

if start_idx >= len(all_api_keys):
    raise IndexError(
        f"GROUP_INDEX={GROUP_INDEX} terlalu besar. "
        f"start_idx={start_idx}, jumlah API key={len(all_api_keys)}"
    )

api_keys_for_job = all_api_keys[start_idx:end_idx]
print(f"👷 GROUP_INDEX={GROUP_INDEX}, pakai API index {start_idx}..{end_idx - 1}")
print(f"🔑 Total API di job ini: {len(api_keys_for_job)}")


# ============================
# GLOBAL COUNTER
# ============================
global_counter_lock = threading.Lock()
global_article_counter = 0


# ============================
# FUNGSI: AMBIL JOB
# ============================
def get_next_job():
    try:
        r = requests.get(JOBS_API_URL, params={"action": "next"}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[JOB] ❌ Error ambil job: {e}")
        return None

    if not data.get("ok"):
        print("[JOB] ❌ Response tidak OK:", data)
        return None

    return data.get("job")


# ============================
# FUNGSI: KIRIM HASIL
# ============================
def submit_result(job_id, status, judul=None, slug=None, metadesc=None, artikel=None):
    payload = {"job_id": job_id, "status": status}

    if status == "done":
        payload.update({
            "judul": judul,
            "slug": slug,
            "metadesc": metadesc,
            "artikel": artikel,
        })

    try:
        r = requests.post(
            JOBS_API_URL,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=60
        )
        r.raise_for_status()
        print(f"[JOB {job_id}] 📤 POST sukses:", r.json())
    except Exception as e:
        print(f"[JOB {job_id}] ❌ Error submit: {e}")


# ============================
# WORKER THREAD
# ============================
def worker_thread(worker_id: int, api_key: str):
    global global_article_counter

    print(f"[T{worker_id}] Start dengan API prefix {api_key[:8]}...")
    client = genai.Client(api_key=api_key)
    last_call = 0.0
    local_done = 0

    while True:
        job = get_next_job()
        if not job:
            print(f"[T{worker_id}] Tidak ada job lagi, stop.")
            break

        job_id = job["id"]
        judul = job["keyword"]
        print(f"\n[T{worker_id}][JOB {job_id}] 🎯 Judul: {judul}")

        success = False

        for attempt in range(1, MAX_RETRIES_PER_TITLE + 1):
            try:
                # throttle per thread / per key
                elapsed = time.time() - last_call
                if elapsed < MIN_SECONDS_PER_REQUEST:
                    time.sleep(MIN_SECONDS_PER_REQUEST - elapsed)

                print(f"[T{worker_id}][JOB {job_id}] 🔄 Gemini attempt {attempt}")

                prompt = build_prompt(judul)
                res = client.models.generate_content(
                    model="gemini-2.5-flash",
                    contents=prompt,
                )
                last_call = time.time()

                raw = (res.text or "").strip()
                if not raw:
                    print(f"[T{worker_id}][JOB {job_id}] ⚠ Output kosong.")
                    break

                # META_DESC parsing
                m = re.search(r"META_DESC\s*:(.*)$", raw, re.IGNORECASE | re.DOTALL)
                if m:
                    metadesc = m.group(1).strip()
                    artikel_html = raw[: m.start()].strip()
                else:
                    print(f"[T{worker_id}][JOB {job_id}] ⚠ META_DESC tidak ada, generate manual.")
                    artikel_html = raw
                    txt = re.sub(r"<.*?>", " ", artikel_html)
                    txt = re.sub(r"\s+", " ", txt).strip()
                    metadesc = txt[:155]

                if not artikel_html:
                    print(f"[T{worker_id}][JOB {job_id}] ⚠ Artikel kosong setelah parsing.")
                    break

                slug = slugify(judul)

                submit_result(
                    job_id=job_id,
                    status="done",
                    judul=judul,
                    slug=slug,
                    metadesc=metadesc,
                    artikel=artikel_html,
                )

                local_done += 1
                with global_counter_lock:
                    global_article_counter += 1
                    total_now = global_article_counter

                print(
                    f"[T{worker_id}][JOB {job_id}] ✅ DONE. "
                    f"Local: {local_done}, Global: {total_now}"
                )
                success = True
                break

            except Exception as e:
                err_str = str(e)
                low = err_str.lower()
                print(f"[T{worker_id}][JOB {job_id}] ❌ Error Gemini: {err_str}")

                if "quota" in low or "limit" in low or "exceeded" in low:
                    delay = parse_retry_delay_seconds(err_str)
                    print(
                        f"[T{worker_id}][JOB {job_id}] 🚫 Quota/limit → tidur {delay:.1f}s"
                    )
                    time.sleep(delay)
                    continue

                print(
                    f"[T{worker_id}][JOB {job_id}] ⚠ Error lain → sleep 10 detik lalu retry."
                )
                time.sleep(10)

        if not success:
            print(f"[T{worker_id}][JOB {job_id}] ❌ Gagal permanen, tandai failed.")
            submit_result(job_id=job_id, status="failed")

    print(f"[T{worker_id}] Selesai. Local selesai: {local_done}")


# ============================
# MAIN: START 5 THREAD
# ============================
def main():
    threads = []
    for idx, key in enumerate(api_keys_for_job):
        t = threading.Thread(
            target=worker_thread,
            args=(idx + 1, key),
            daemon=True
        )
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    print(f"\n🎉 GROUP_INDEX={GROUP_INDEX} selesai. Total artikel global: {global_article_counter}")


if __name__ == "__main__":
    main()


# Misal kamu punya 20 key di api.txt.
# Kita bikin 4 job parallel, masing-masing ambil 5 key:

# GROUP_INDEX 0 → key 0–4

# GROUP_INDEX 1 → key 5–9

# GROUP_INDEX 2 → key 10–14

# GROUP_INDEX 3 → key 15–19