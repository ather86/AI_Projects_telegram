import json
import requests
import os
import glob
import sys

# --- 1. CONFIGURATION ---
COMFY_URL = "http://127.0.0.1:8188/prompt"
WORKFLOW_FILE = r"E:\AI\AI_Projects\OpenClaw_Bridge\workflow_api\video_ltx2_3_i2v.json"
# Your verified Stability Matrix path on D:
COMFY_INPUT_PATH = r"D:\StabilityMatrix-win-x64\Data\Packages\ComfyUI\input"

def get_latest_image(folder):
    """Finds the most recently added image in the input folder."""
    files = glob.glob(os.path.join(folder, "*"))
    # Filter for common image extensions
    images = [f for f in files if f.lower().endswith(('.png', '.jpg', '.jpeg', '.webp'))]
    if not images:
        return None
    return os.path.basename(max(images, key=os.path.getctime))

# --- 2. DYNAMIC INPUT HANDLING ---
# Use command line argument if provided, otherwise use a default
if len(sys.argv) > 1:
    user_text = sys.argv[1]
else:
    user_text = "A cinematic talking head video, high detail, professional lighting."

# Find the image to animate
image_filename = get_latest_image(COMFY_INPUT_PATH)

if not image_filename:
    print(f"❌ Error: No images found in {COMFY_INPUT_PATH}")
    sys.exit()

# --- 3. WORKFLOW INJECTION ---
try:
    with open(WORKFLOW_FILE, 'r', encoding='utf-8') as f:
        workflow = json.load(f)

    # Node 269: The Image Loader (LoadImage class)
    workflow["269"]["inputs"]["image"] = image_filename

    # Node 267:266: The Dialogue Text (PrimitiveStringMultiline class)
    workflow["267:266"]["inputs"]["value"] = user_text

    # --- 4. TRIGGER COMFYUI API ---
    print(f"🔄 Connecting to ComfyUI...")
    payload = {"prompt": workflow}
    response = requests.post(COMFY_URL, data=json.dumps(payload))

    if response.status_code == 200:
        print(f"🚀 SUCCESS! Render Triggered.")
        print(f"📸 Using Image: {image_filename}")
        print(f"✍️  With Prompt: {user_text}")
        print(f"💡 Check your RTX 5070 Ti usage in Task Manager now.")
    else:
        print(f"❌ ComfyUI rejected the request: {response.text}")

except FileNotFoundError:
    print(f"❌ Error: Could not find the workflow file at {WORKFLOW_FILE}")
except Exception as e:
    print(f"❌ Connection Error: Is ComfyUI open? Details: {e}")