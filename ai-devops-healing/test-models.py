import time
from google import genai
from google.genai import types, errors

client = genai.Client(http_options=types.HttpOptions(timeout=15_000))

SKIP = ("tts", "image", "audio", "live", "embed", "imagen", "veo",
        "robotics", "computer", "native", "transcribe")

names = []
for m in client.models.list():
    n = m.name.replace("models/", "")
    if "generateContent" in (m.supported_actions or []) and not any(k in n for k in SKIP):
        names.append(n)

print(f"Testing {len(names)} text models (15s timeout each)...\n")

working = []
for n in names:
    start = time.time()
    try:
        r = client.models.generate_content(model=n, contents="Reply with the single word: OK")
        dt = time.time() - start
        print(f"  OK     {dt:5.1f}s  {n}")
        working.append((dt, n))
    except errors.APIError as e:
        print(f"  FAIL   {e.code}     {n}")
    except Exception as e:
        print(f"  FAIL   {type(e).__name__}  {n}")

working.sort()
print("\nFastest working models:")
for dt, n in working[:4]:
    print(f"  {n}  ({dt:.1f}s)")
if working:
    print("\nexport GEMINI_MODELS=" + ",".join(n for _, n in working[:3]))
