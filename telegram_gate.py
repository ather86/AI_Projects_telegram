import os
import sys
import json
import time
import asyncio
import logging
import threading
import re
import shutil
import glob
import unicodedata
import math

import requests
import psutil
import ollama
import websocket
import subprocess
from pynvml import *
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest, TimedOut, NetworkError
from telegram.ext import ApplicationBuilder, ContextTypes, MessageHandler, CommandHandler, filters, CallbackQueryHandler
from flask import Flask, request, jsonify
import video_processor

# --- Flask App for Trading Alerts ---
flask_app = Flask(__name__)

async def send_trading_alert_async(app, chat_id, title, message):
    """Asynchronously sends a message using the bot application."""
    try:
        await app.bot.send_message(chat_id=chat_id, text=f"*{title}*\n\n{message}", parse_mode='Markdown')
        logging.info(f"Successfully sent trading alert to chat {chat_id}.")
        return True
    except Exception as e:
        logging.error(f"Failed to send trading alert to {chat_id}: {e}")
        return False

@flask_app.route('/send_alert', methods=['POST'])
def receive_alert():
    data = request.json
    title = data.get('title')
    message = data.get('message')
    ticker = data.get('ticker')
    
    if not all([title, message, ticker]):
        return jsonify({"status": "error", "message": "Missing title, message, or ticker"}), 400

    if not TRADING_CHAT_ID:
        logging.error("TRADING_CHAT_ID is not set. Cannot send alert.")
        return jsonify({"status": "error", "message": "Trading chat ID not configured on server"}), 500

    # The magic happens here: we run the async function in the main event loop
    if 'bot_app' in flask_app.config:
        bot_app = flask_app.config['bot_app']
        asyncio.run_coroutine_threadsafe(
            send_trading_alert_async(bot_app, TRADING_CHAT_ID, title, message),
            bot_app.loop
        )
        return jsonify({"status": "success", "message": "Alert queued for sending"}), 202
    else:
        logging.error("Bot application not found in Flask config.")
        return jsonify({"status": "error", "message": "Bot not initialized"}), 500


def run_flask_app(bot_app):
    """Run the Flask app in a separate thread."""
    flask_app.config['bot_app'] = bot_app
    flask_thread = threading.Thread(target=lambda: flask_app.run(port=5001, debug=False, use_reloader=False))
    flask_thread.daemon = True
    flask_thread.start()
    logging.info("Flask alert receiver is running in a background thread.")


# --- End Flask App ---


def load_local_env_file(env_path: str) -> None:
    """Load simple KEY=VALUE pairs from a local .env file into process env."""
    if not os.path.exists(env_path):
        return

    try:
        with open(env_path, "r", encoding="utf-8") as env_file:
            for raw_line in env_file:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as error:
        logging.warning(f"Failed to load .env file at {env_path}: {error}")


PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
load_local_env_file(os.path.join(PROJECT_DIR, ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

# Silence noisy third-party info logs that don't add operational insight.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("apscheduler").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("websocket").setLevel(logging.WARNING)


# --- 1. CONFIGURATION ---
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TRADING_CHAT_ID = os.getenv("TRADING_CHAT_ID", "").strip() # New ID for the trading channel
COMFY_URL = os.getenv("COMFY_URL", "http://127.0.0.1:8188").strip()
COMFY_WS_URL = f"ws://{COMFY_URL.split('//')[1]}/ws"
WORKFLOW_API_PATH = os.getenv("WORKFLOW_API_PATH", os.path.join(PROJECT_DIR, "workflow_api"))
COMFY_INPUT_PATH = os.getenv("COMFY_INPUT_PATH", r"D:\StabilityMatrix-win-x64\Data\Packages\ComfyUI\input")
COMFY_OUTPUT_PATH = os.getenv("COMFY_OUTPUT_PATH", r"D:\StabilityMatrix-win-x64\Data\Packages\ComfyUI\output\video")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5-coder:32b").strip()
OLLAMA_VISION_MODEL = os.getenv("OLLAMA_VISION_MODEL", "llama3.2-vision:latest").strip()

admin_chat_raw = os.getenv("ADMIN_CHAT_ID", "")
ADMIN_CHAT_ID = int(admin_chat_raw) if admin_chat_raw.isdigit() else None
ALERT_INTERVAL_SECONDS = 300

THRESHOLDS = {
    "gpu_temp": 85,
    "cpu_usage": 90,
    "ram_usage": 98,
    "disk_usage_c": 98,
    "disk_usage_d": 98,
}

alert_state = {
    "gpu_temp": False,
    "cpu_usage": False,
    "ram_usage": False,
    "disk_usage_c": False,
    "disk_usage_d": False,
}

# Prompts currently tracked for detailed ComfyUI terminal logs.
ACTIVE_PROMPT_IDS = set()
COMFY_PROGRESS_STATE = {}
LAST_QUEUE_REMAINING = None

WIZARD_STEPS = {
    "image": "image",
    "prompt": "prompt",
    "mode": "mode",
    "duration": "duration",
    "quality": "quality",
}

UPSCALE_PRESETS = {
    "no": None,
    "none": None,
    "off": None,
    "2k": 1440,
    "4k": 2160,
}


# --- 2. COMFYUI TERMINAL STATUS (WebSocket) ---
def on_ws_message(ws, message):
    global LAST_QUEUE_REMAINING
    try:
        payload = json.loads(message)
        event_type = payload.get("type")
        data = payload.get("data", {})

        if event_type == "status":
            queue_remaining = data.get("status", {}).get("exec_info", {}).get("queue_remaining")
            if queue_remaining is not None and queue_remaining != LAST_QUEUE_REMAINING:
                logging.info(f"[ComfyUI] Queue remaining: {queue_remaining}")
                LAST_QUEUE_REMAINING = queue_remaining
            return

        prompt_id = data.get("prompt_id")
        tracked = prompt_id in ACTIVE_PROMPT_IDS if prompt_id else False

        if event_type == "execution_start" and prompt_id:
            COMFY_PROGRESS_STATE[prompt_id] = {"last_percent": -1, "last_node": None}
            logging.info(f"[ComfyUI] ▶ Execution started | prompt_id={prompt_id}")
            return

        if event_type == "progress" and tracked:
            value = data.get("value", 0)
            total = data.get("max", 1)
            percent = (value / total * 100.0) if total else 0.0
            node = data.get("node")

            state = COMFY_PROGRESS_STATE.setdefault(prompt_id, {"last_percent": -1, "last_node": None})
            last_percent = state["last_percent"]
            last_node = state["last_node"]

            should_log = (
                last_percent < 0
                or int(percent // 5) > int(last_percent // 5)
                or node != last_node
                or percent >= 99.9
            )

            if should_log:
                filled = int((percent / 100) * 20)
                bar = "█" * filled + "░" * (20 - filled)
                logging.info(
                    f"[ComfyUI] [{bar}] {percent:5.1f}% | node={node} | prompt_id={prompt_id[:8]}"
                )
                state["last_percent"] = percent
                state["last_node"] = node
            return

        if event_type == "executing" and tracked:
            node = data.get("node")
            if node is None:
                logging.info(f"[ComfyUI] ■ Execution stream ended | prompt_id={prompt_id}")
            else:
                state = COMFY_PROGRESS_STATE.setdefault(prompt_id, {"last_percent": -1, "last_node": None})
                if state.get("last_node") != node:
                    logging.info(f"[ComfyUI] ⏳ Node {node} | prompt_id={prompt_id[:8]}")
                    state["last_node"] = node
            return

        if event_type == "execution_success" and prompt_id:
            logging.info(f"[ComfyUI] ✅ Execution success | prompt_id={prompt_id}")
            ACTIVE_PROMPT_IDS.discard(prompt_id)
            COMFY_PROGRESS_STATE.pop(prompt_id, None)
            return

        if event_type == "execution_error" and prompt_id:
            logging.error(f"[ComfyUI] Execution error | prompt_id={prompt_id} | details={data}")
            ACTIVE_PROMPT_IDS.discard(prompt_id)
            COMFY_PROGRESS_STATE.pop(prompt_id, None)
            return

    except Exception as error:
        logging.error(f"[ComfyUI-WS] Failed to parse event: {error}")


def on_ws_open(ws):
    logging.info("[ComfyUI-WS] Connected")


def on_ws_close(ws, close_status_code, close_msg):
    logging.warning(f"[ComfyUI-WS] Disconnected | code={close_status_code} | reason={close_msg}")


def on_ws_error(ws, error):
    logging.error(f"[ComfyUI-WS] Error: {error}")


def listen_to_comfyui_websocket():
    while True:
        try:
            ws = websocket.WebSocketApp(
                COMFY_WS_URL,
                on_open=on_ws_open,
                on_message=on_ws_message,
                on_close=on_ws_close,
                on_error=on_ws_error,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as error:
            logging.error(f"[ComfyUI-WS] Listener crashed: {error}")
        time.sleep(5)


# --- 3. PROMPT GENERATION ---
def sanitize_prompt(text):
    clean_text = re.sub(r"[^\u0900-\u097Fa-zA-Z0-9\s.,!?]", "", text)
    return clean_text[:250].strip()


def extract_spoken_text(caption: str) -> str:
    """Extract speech text from caption while keeping compatibility with old formats.

    Supported patterns:
    - "instruction: speech text"
    - "dialogue: speech text"
    - plain text (entire caption)
    """
    if not caption:
        return "Hello from Bengaluru."

    lowered = caption.lower()
    markers = ["dialogue:", "speech:", "say:", "says:"]
    for marker in markers:
        index = lowered.find(marker)
        if index != -1:
            candidate = caption[index + len(marker):].strip()
            if candidate:
                return candidate

    if ":" in caption:
        parts = caption.split(":", 1)
        candidate = parts[1].strip()
        if candidate:
            return candidate

    return caption.strip()


def parse_user_scene_and_dialogue(caption: str) -> tuple[str, str]:
    """Parse scene idea and dialogue from flexible user caption formats.

    Supported formats:
    - dialogue markers: "dialogue: ...", "speech: ...", "say: ..."
    - quoted dialogue: ... "this is spoken text"
    - separator format: "scene idea || spoken text"
    - fallback: whole caption is both scene idea and dialogue
    """
    raw = (caption or "").strip()
    if not raw:
        return "A person talking to camera", "Hello from Bengaluru."

    if "||" in raw:
        left, right = raw.split("||", 1)
        scene_text = left.strip() or "A person talking to camera"
        dialogue_text = right.strip()
        if dialogue_text:
            return scene_text, dialogue_text

    marker_dialogue = extract_spoken_text(raw)
    if marker_dialogue and marker_dialogue != raw:
        marker_pos = raw.lower().find("dialogue:")
        if marker_pos == -1:
            marker_pos = raw.lower().find("speech:")
        if marker_pos == -1:
            marker_pos = raw.lower().find("say:")
        scene_text = raw[:marker_pos].strip(" -|,") if marker_pos != -1 else ""
        if not scene_text:
            scene_text = raw
        return scene_text, marker_dialogue

    quote_match = re.search(r'"([^"]{3,})"', raw)
    if not quote_match:
        quote_match = re.search(r"“([^”]{3,})”", raw)
    if quote_match:
        dialogue_text = quote_match.group(1).strip()
        scene_text = (raw[:quote_match.start()] + " " + raw[quote_match.end():]).strip()
        if not scene_text:
            scene_text = dialogue_text
        return scene_text, dialogue_text

    return raw, raw


def sanitize_audio_dialogue(text: str) -> str:
    """Sanitize dialogue for TTS while preserving natural language across scripts."""
    raw = (text or "").strip()
    if not raw:
        return "Hello from Bengaluru."

    # Drop control chars, keep letters/numbers/spacing/punctuation from any language.
    cleaned_chars = []
    for char in raw:
        category = unicodedata.category(char)
        if category.startswith("C") and char not in {"\n", "\t", "\r"}:
            continue
        cleaned_chars.append(char)

    safe = "".join(cleaned_chars)
    safe = re.sub(r"\s+", " ", safe).strip()
    safe = re.sub(r"[\"'`]{2,}", '"', safe)
    safe = safe.strip(" -|,.;")

    # Keep it reasonably bounded for stable speech generation.
    return safe[:260] if safe else "Hello from Bengaluru."


def ensure_audio_dialogue_suffix(scene_prompt: str, fallback_text: str, force_fallback_dialogue: bool = False) -> str:
    """Guarantee prompt contains Audio Dialogue line in a meaningful way."""
    base_prompt = (scene_prompt or "").strip()
    fallback = sanitize_audio_dialogue(fallback_text)

    if force_fallback_dialogue and fallback:
        visual = base_prompt.strip() if base_prompt.strip() else "cinematic talking-head shot"
        visual = re.sub(r"\s*Audio\s*Dialogue\s*:\s*.*$", "", visual, flags=re.IGNORECASE).strip()
        return f"{visual}. Audio Dialogue: {fallback}".strip()

    if "audio dialogue:" in base_prompt.lower():
        prefix, _, tail = base_prompt.partition("Audio Dialogue:") if "Audio Dialogue:" in base_prompt else (base_prompt, "", "")
        dialogue_tail = sanitize_audio_dialogue(tail if tail else fallback)
        return f"{prefix.strip()} Audio Dialogue: {dialogue_tail}".strip()

    scene_text, extracted_dialogue = parse_user_scene_and_dialogue(base_prompt)
    dialogue = sanitize_audio_dialogue(extracted_dialogue if extracted_dialogue else fallback)
    visual = scene_text.strip() if scene_text.strip() else "cinematic talking-head shot"
    return f"{visual}. Audio Dialogue: {dialogue}"


def build_manual_scene_prompt(scene_visual: str, scene_dialogue: str) -> str:
    """Build manual-mode prompt with dialogue first to avoid truncation losing speech text."""
    dialogue = sanitize_audio_dialogue(scene_dialogue)
    visual = re.sub(r"\s+", " ", (scene_visual or "").strip())

    # Remove embedded quoted speech from visual block to avoid confusion/repetition.
    visual = re.sub(r'"[^"]{3,}"', "", visual)
    visual = re.sub(r"“[^”]{3,}”", "", visual)
    visual = re.sub(r"\s+", " ", visual).strip(" .")

    # Keep visual direction concise so dialogue remains within early tokens.
    if len(visual) > 260:
        visual = visual[:260].rsplit(" ", 1)[0].strip(" .")

    if not visual:
        visual = "cinematic talking-head framing with natural motion"

    return (
        f"Audio Dialogue: {dialogue}. "
        f"Visual Direction: {visual}. "
        "Single speaker only. Natural lip sync."
    )


def _split_script_into_dialogue_chunks(script_text: str, target_chunks: int) -> list[str]:
    """Split user script into contiguous scene-sized dialogue chunks preserving order."""
    cleaned = re.sub(r"\s+", " ", (script_text or "").strip())
    if not cleaned:
        return ["Hello from Bengaluru."] * max(1, target_chunks)

    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", cleaned) if s.strip()]
    if not sentences:
        sentences = [cleaned]

    target_chunks = max(1, target_chunks)

    # If sentence count is lower than required chunks (e.g., one long sentence),
    # split by words to avoid repeating identical dialogue across scenes.
    if len(sentences) < target_chunks:
        words = cleaned.split()
        if not words:
            return ["Hello from Bengaluru."] * target_chunks

        chunks: list[str] = []
        total_words = len(words)
        for index in range(target_chunks):
            start = int(index * total_words / target_chunks)
            end = int((index + 1) * total_words / target_chunks)
            if start >= end:
                end = min(total_words, start + 1)

            chunk_words = words[start:end]
            if not chunk_words:
                chunk_words = [words[min(start, total_words - 1)]]

            chunk = " ".join(chunk_words).strip()
            chunks.append(sanitize_audio_dialogue(chunk))

        return chunks

    chunks: list[str] = []
    total = len(sentences)

    for index in range(target_chunks):
        start = int(index * total / target_chunks)
        end = int((index + 1) * total / target_chunks)
        if start >= end:
            end = min(total, start + 1)
        chunk = " ".join(sentences[start:end]).strip()
        if not chunk:
            chunk = sentences[min(start, total - 1)]
        chunks.append(sanitize_audio_dialogue(chunk))

    return chunks


def _ensure_scene_dialogue_from_script(scenes: list[dict], script_text: str) -> list[dict]:
    """Ensure every scene has dialogue_text by filling missing values from user script chunks."""
    if not scenes:
        return scenes

    dialogue_chunks = _split_script_into_dialogue_chunks(script_text, len(scenes))

    for index, scene in enumerate(scenes):
        if not isinstance(scene, dict):
            scenes[index] = {
                "type": "dialogue",
                "visual_prompt": str(scene),
                "dialogue_text": dialogue_chunks[index],
            }
            continue

        scene_dialogue = (scene.get("dialogue_text") or "").strip()
        if not scene_dialogue:
            scene["dialogue_text"] = dialogue_chunks[index]
            scene["type"] = "dialogue"

    return scenes


def _split_manual_scene_blocks(script_text: str, target_scenes: int) -> list[str]:
    """Split manual script into scene blocks without rewriting user text.

    Priority:
    1) Explicit scene separators (`\n\n`, `---`, `||` on separate lines).
    2) Sentence-based contiguous chunking when explicit separators are absent.
    """
    raw = (script_text or "").strip()
    if not raw:
        return ["A person talking to camera."] * max(1, target_scenes)

    target_scenes = max(1, target_scenes)

    # Normalize separators for explicit scene block mode.
    normalized = raw.replace("\r\n", "\n")
    normalized = re.sub(r"\n\|\|\n", "\n\n", normalized)
    normalized = re.sub(r"\n---+\n", "\n\n", normalized)

    explicit_blocks = [block.strip() for block in re.split(r"\n\s*\n", normalized) if block.strip()]
    if len(explicit_blocks) >= target_scenes:
        # Merge extras into the last requested scene to preserve all user text.
        if len(explicit_blocks) > target_scenes:
            head = explicit_blocks[: target_scenes - 1]
            tail = "\n\n".join(explicit_blocks[target_scenes - 1 :])
            return head + [tail]
        return explicit_blocks

    # Fallback: contiguous sentence chunking (preserve words/order, no rewriting).
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", raw) if s.strip()]
    if not sentences:
        sentences = [raw]

    chunks: list[str] = []
    total = len(sentences)
    for index in range(target_scenes):
        start = int(index * total / target_scenes)
        end = int((index + 1) * total / target_scenes)
        if start >= end:
            end = min(total, start + 1)
        chunk = " ".join(sentences[start:end]).strip()
        if not chunk:
            chunk = sentences[min(start, total - 1)]
        chunks.append(chunk)

    return chunks


def _extract_manual_dialogue_track(script_text: str) -> str:
    """Extract intended spoken dialogue from manual script without rewriting content."""
    raw = (script_text or "").strip()
    if not raw:
        return ""

    # 1) Prefer quoted dialogue blocks, including smart quotes.
    quoted = re.findall(r'"([^"]{3,})"', raw)
    if not quoted:
        quoted = re.findall(r"“([^”]{3,})”", raw)
    if quoted:
        merged = " ".join(segment.strip() for segment in quoted if segment.strip())
        return merged.strip(" \"'“”`")

    # 2) Fallback to narration markers (say/says/dialogue/speech) until sentence end.
    marker_match = re.search(r"(?:dialogue|speech|say|says)\s*:\s*(.+)$", raw, flags=re.IGNORECASE)
    if marker_match:
        return marker_match.group(1).strip().strip(" \"'“”`")

    return ""


def _parse_manual_script_with_ollama(script_text: str, num_scenes: int, model: str) -> list[dict]:
    """Manual mode parser: split logically into scenes while preserving exact quoted dialogue text."""
    dialogue_track = _extract_manual_dialogue_track(script_text)
    system_prompt = (
        "You are a strict video script segmenter for MANUAL MODE. "
        "Split the provided script into EXACTLY the requested number of scenes. "
        "DO NOT be creative. DO NOT invent new story details. "
        "Keep visual prompts short and derived only from source text. "
        "CRITICAL DIALOGUE RULE: If quoted dialogue is provided, dialogue_text chunks must use the EXACT original words from that quoted text. "
        "Do not paraphrase, translate, rewrite, embellish, or add words. "
        "Distribute dialogue logically across scenes in contiguous order. "
        "Return ONLY valid JSON in this schema: "
        "{\"scenes\": [{\"visual_prompt\": \"...\", \"dialogue_text\": \"...\"}]}."
    )

    user_payload = {
        "target_scenes": num_scenes,
        "script": script_text,
        "quoted_dialogue": dialogue_track,
    }

    response = ollama.chat(
        model=model,
        format="json",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": json.dumps(user_payload, ensure_ascii=False)},
        ],
        options={"temperature": 0.0, "num_ctx": 8192, "num_predict": 4096},
    )

    content = response["message"]["content"]
    data = video_processor.safely_parse_json_with_control_chars(content)
    scenes = data.get("scenes", []) if isinstance(data, dict) else []
    if not isinstance(scenes, list) or not scenes:
        raise ValueError("Manual Ollama parser returned invalid scenes")

    if len(scenes) != num_scenes:
        if len(scenes) > num_scenes:
            scenes = scenes[:num_scenes]
        else:
            last_scene = scenes[-1] if scenes else {"visual_prompt": script_text, "dialogue_text": ""}
            while len(scenes) < num_scenes:
                scenes.append(dict(last_scene))

    normalized = []
    for scene in scenes:
        if not isinstance(scene, dict):
            normalized.append({"visual_prompt": str(scene), "dialogue_text": ""})
            continue
        normalized.append(
            {
                "visual_prompt": (scene.get("visual_prompt") or "").strip(),
                "dialogue_text": (scene.get("dialogue_text") or "").strip(),
            }
        )

    return normalized


def _normalize_for_dialogue_compare(text: str) -> str:
    value = (text or "").lower()
    value = re.sub(r"\s+", " ", value).strip()
    value = re.sub(r"[^\w\s]", "", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def _manual_dialogue_integrity_ok(source_dialogue: str, scene_dialogues: list[str]) -> bool:
    source_norm = _normalize_for_dialogue_compare(source_dialogue)
    if not source_norm:
        return True

    joined_norm = _normalize_for_dialogue_compare(" ".join(scene_dialogues))
    if not joined_norm:
        return False

    return joined_norm in source_norm or source_norm in joined_norm


async def build_dynamic_prompt(caption: str, status_msg, image_path: str | None = None) -> str:
    await status_msg.edit_text("🧠 Expanding prompt with Ollama...")
    scene_seed, user_dialogue_raw = parse_user_scene_and_dialogue(caption)
    sanitized_caption = sanitize_prompt(scene_seed)
    spoken_text = sanitize_audio_dialogue(user_dialogue_raw)
    if len(spoken_text) < 4:
        spoken_text = sanitize_audio_dialogue(scene_seed)
    logging.info(f"[Prompt] User caption (raw): {caption}")
    logging.info(f"[Prompt] Scene seed parsed: {scene_seed}")
    logging.info(f"[Prompt] Dialogue parsed (raw): {user_dialogue_raw}")
    logging.info(f"[Prompt] User caption (sanitized): {sanitized_caption}")
    logging.info(f"[Prompt] Dialogue extracted/locked: {spoken_text}")

    system_prompt = (
        "You are an expert prompt engineer for image-to-video generation. "
        "Create one concise cinematic scene prompt that preserves the exact person identity from the reference image. "
        "Never replace subject with another person. Keep face, age, gender, hair, skin tone, and body type consistent with reference image. "
        "Describe natural talking-head motion, subtle expression changes, realistic lip sync context, and stable background. "
        "Do not output markdown, bullets, or labels. Return only plain prompt sentence(s)."
    )

    try:
        user_prompt = (
            f"Scene intent: {sanitized_caption}. "
            f"Dialogue that must be spoken verbatim: {spoken_text}. "
            "Generate visual prompt only."
        )

        if image_path and os.path.exists(image_path):
            logging.info(f"[Prompt] Using Ollama vision model with image: {image_path}")
            response = ollama.chat(
                model=OLLAMA_VISION_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt, "images": [image_path]},
                ],
                options={"temperature": 0.4},
            )
        else:
            logging.info("[Prompt] Using text-only Ollama model (no image passed)")
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                options={"temperature": 0.5},
            )

        generated_prompt = response["message"]["content"].strip()
        logging.info(f"[Prompt] Ollama expanded prompt (raw): {generated_prompt}")
        scene_only = generated_prompt.split("Audio Dialogue:", 1)[0].strip()
        scene_only = re.sub(r"\s+", " ", scene_only).strip()
        if not scene_only:
            scene_only = "cinematic video, natural talking to camera, expressive but realistic motion"

        speech_guard = (
            "Single speaker only. Clear natural English pronunciation. "
            "Steady pacing. No mumbling. No extra words. "
            "Speak the Audio Dialogue text exactly verbatim. Preserve exact identity from reference image."
        )
        final_prompt = f"{scene_only}. {speech_guard} Audio Dialogue: {spoken_text}"
        logging.info(f"[Prompt] Final prompt to ComfyUI: {final_prompt}")
        return final_prompt
    except Exception as error:
        logging.error(f"Ollama prompt generation failed: {error}")
        await status_msg.edit_text("⚠️ Ollama unavailable, using fallback prompt.")
        return (
            "cinematic video, natural talking to camera, expressive but realistic motion. "
            "Single speaker only. Clear natural English pronunciation. Steady pacing. "
            "No mumbling. No extra words. Speak the Audio Dialogue text exactly verbatim. "
            f"Audio Dialogue: {spoken_text}"
        )


async def build_hybrid_prompt(caption: str, status_msg, image_path: str | None = None) -> str:
    """Hybrid mode: Ollama generates scene from image+intent, dialogue stays user-locked."""
    await status_msg.edit_text("🧠 Hybrid mode: generating scene with Ollama, locking your dialogue...")

    scene_seed, user_dialogue_raw = parse_user_scene_and_dialogue(caption)
    sanitized_scene_seed = sanitize_prompt(scene_seed)
    locked_dialogue = sanitize_audio_dialogue(user_dialogue_raw)
    if len(locked_dialogue) < 4:
        locked_dialogue = sanitize_audio_dialogue(scene_seed)

    logging.info(f"[Prompt] Hybrid scene seed: {scene_seed}")
    logging.info(f"[Prompt] Hybrid dialogue locked: {locked_dialogue}")

    system_prompt = (
        "You are an expert prompt engineer for image-to-video generation. "
        "Create one concise cinematic scene prompt that preserves exact identity from the reference image. "
        "Do not invent a different person. Keep face and appearance consistent with the uploaded image. "
        "Return only visual scene prompt text, no labels and no dialogue line."
    )

    user_prompt = (
        f"Scene intent: {sanitized_scene_seed}. "
        "Generate visual prompt only."
    )

    try:
        if image_path and os.path.exists(image_path):
            response = ollama.chat(
                model=OLLAMA_VISION_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt, "images": [image_path]},
                ],
                options={"temperature": 0.4},
            )
        else:
            response = ollama.chat(
                model=OLLAMA_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                options={"temperature": 0.5},
            )

        generated_scene = response["message"]["content"].strip()
        generated_scene = generated_scene.split("Audio Dialogue:", 1)[0].strip()
        generated_scene = re.sub(r"\s+", " ", generated_scene).strip()
        if not generated_scene:
            generated_scene = "cinematic video, natural talking to camera, expressive but realistic motion"

        speech_guard = (
            "Single speaker only. Clear natural English pronunciation. "
            "Steady pacing. No mumbling. No extra words. "
            "Speak the Audio Dialogue text exactly verbatim. Preserve exact identity from reference image."
        )
        final_prompt = f"{generated_scene}. {speech_guard} Audio Dialogue: {locked_dialogue}"
        logging.info(f"[Prompt] Hybrid final prompt to ComfyUI: {final_prompt}")
        return final_prompt
    except Exception as error:
        logging.error(f"Hybrid prompt generation failed: {error}")
        await status_msg.edit_text("⚠️ Hybrid generation fallback: using direct scene text + locked dialogue.")
        return (
            f"{sanitized_scene_seed}. "
            "Single speaker only. Clear natural English pronunciation. Steady pacing. "
            "No mumbling. No extra words. Speak the Audio Dialogue text exactly verbatim. "
            f"Audio Dialogue: {locked_dialogue}"
        )


def reset_wizard_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data["wizard_active"] = False
    context.user_data.pop("wizard_step", None)
    context.user_data.pop("wizard_data", None)


def get_workflow_path(context: ContextTypes.DEFAULT_TYPE) -> tuple[str, str]:
    workflow_filename = context.user_data.get("workflow_file", "video_ltx2_3_i2v.json")
    workflow_path = os.path.join(WORKFLOW_API_PATH, workflow_filename)
    return workflow_filename, workflow_path


def extract_last_frame_to_input(video_path: str, output_image_path: str) -> bool:
    try:
        probe = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                video_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        duration_text = (probe.stdout or "").strip()
        duration = float(duration_text) if duration_text else 0.0
        if duration <= 0:
            return False

        seek_time = max(0.0, duration - 0.2)
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-ss",
                f"{seek_time}",
                "-i",
                video_path,
                "-frames:v",
                "1",
                output_image_path,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return os.path.exists(output_image_path)
    except Exception:
        return False


def _apply_scene_duration_to_workflow(workflow: dict, scene_duration_seconds: int) -> None:
    """Set workflow frame length to keep each generated scene close to target seconds."""
    if not isinstance(workflow, dict):
        return

    target_seconds = max(1, int(scene_duration_seconds))
    fps = 24

    # Preferred known fps node for current LTX workflow.
    if "267:260" in workflow:
        try:
            fps_candidate = workflow["267:260"].get("inputs", {}).get("value")
            if isinstance(fps_candidate, (int, float)) and fps_candidate > 0:
                fps = int(round(fps_candidate))
        except Exception:
            pass

    # Generic fallback: discover frame-rate primitive by title.
    if fps <= 0:
        for _, node in workflow.items():
            if not isinstance(node, dict):
                continue
            if node.get("class_type") != "PrimitiveInt":
                continue
            title = str(node.get("_meta", {}).get("title", "")).lower()
            if "frame rate" in title or title == "fps":
                value = node.get("inputs", {}).get("value")
                if isinstance(value, (int, float)) and value > 0:
                    fps = int(round(value))
                    break

    frames = max(1, int(round(fps * target_seconds)))

    # Preferred known length node for current LTX workflow.
    if "267:225" in workflow and isinstance(workflow["267:225"], dict):
        workflow["267:225"].setdefault("inputs", {})["value"] = frames

    # Generic fallback: update length-like PrimitiveInt nodes by title.
    for _, node in workflow.items():
        if not isinstance(node, dict):
            continue
        if node.get("class_type") != "PrimitiveInt":
            continue
        title = str(node.get("_meta", {}).get("title", "")).lower()
        if title in {"length", "frames", "frame count", "frames number"}:
            node.setdefault("inputs", {})["value"] = frames


async def queue_and_wait_video(
    prompt_text: str,
    image_name: str,
    workflow_path: str,
    status_msg,
    progress_label: str,
    scene_duration_seconds: int = 12,
) -> str | None:
    try:
        with open(workflow_path, "r", encoding="utf-8") as workflow_file:
            workflow = json.load(workflow_file)

        _apply_scene_duration_to_workflow(workflow, scene_duration_seconds)

        workflow["269"]["inputs"]["image"] = image_name
        workflow["267:266"]["inputs"]["value"] = prompt_text
        if "267:240" in workflow and "inputs" in workflow["267:240"]:
            workflow["267:240"]["inputs"]["text"] = prompt_text
        if "267:274" in workflow and "inputs" in workflow["267:274"]:
            workflow["267:274"]["inputs"]["sampling_mode"] = "off"

        response = requests.post(f"{COMFY_URL}/prompt", data=json.dumps({"prompt": workflow}), timeout=30)
        response.raise_for_status()
        payload = response.json()
        prompt_id = payload.get("prompt_id")
        if not prompt_id:
            return None

        ACTIVE_PROMPT_IDS.add(prompt_id)
        history_url = f"{COMFY_URL}/history/{prompt_id}"
        await status_msg.edit_text(f"{progress_label}\n🚀 Scene queued: `{prompt_id[:8]}`", parse_mode="Markdown")

        for _ in range(160):
            await asyncio.sleep(10)
            history_payload = requests.get(history_url, timeout=15).json()
            if prompt_id not in history_payload:
                continue

            outputs = history_payload[prompt_id].get("outputs", {})
            video_filename = None
            for _, output_data in outputs.items():
                key = "gifs" if "gifs" in output_data else "images" if "images" in output_data else None
                if key and output_data[key]:
                    video_filename = output_data[key][0].get("filename")
                    break

            ACTIVE_PROMPT_IDS.discard(prompt_id)
            if not video_filename:
                return None
            return os.path.join(COMFY_OUTPUT_PATH, video_filename)

        ACTIVE_PROMPT_IDS.discard(prompt_id)
        return None
    except Exception as error:
        logging.error(f"[Wizard] Scene render failed: {error}", exc_info=True)
        return None


async def run_story_generation(update: Update, context: ContextTypes.DEFAULT_TYPE, status_msg):
    wizard_data = context.user_data.get("wizard_data", {})
    prompt_text = wizard_data.get("prompt", "").strip()
    mode = wizard_data.get("mode", "auto")
    manual_strict = bool(context.user_data.get("manual_strict", False))
    duration = int(wizard_data.get("duration", 12))
    quality = str(wizard_data.get("quality", "no")).lower().strip()
    upscale_resolution = UPSCALE_PRESETS.get(quality, None)
    base_image_name = wizard_data.get("image_name")
    pipeline_id = wizard_data.get("pipeline_id") or f"pl_{update.effective_user.id}_{int(time.time())}"
    wizard_data["pipeline_id"] = pipeline_id
    pipeline_dir = os.path.join(video_processor.APP_OUTPUT_DIR, "pipelines", pipeline_id)
    os.makedirs(pipeline_dir, exist_ok=True)

    workflow_filename, workflow_path = get_workflow_path(context)
    if not os.path.exists(workflow_path):
        await status_msg.edit_text(f"❌ Workflow not found: `{workflow_filename}`", parse_mode="Markdown")
        return

    if not base_image_name:
        await status_msg.edit_text("❌ Missing image. Please restart with /begin")
        return

    target_scene_seconds = 12
    num_scenes = max(1, math.ceil(duration / target_scene_seconds))
    if mode == "manual":
        await status_msg.edit_text(f"🧩 Manual mode: using Ollama for strict scene split ({num_scenes} scene(s), no creativity)...")
        source_dialogue_track = _extract_manual_dialogue_track(prompt_text)
        if not source_dialogue_track:
            source_dialogue_track = prompt_text
        canonical_dialogue_chunks = _split_script_into_dialogue_chunks(source_dialogue_track, num_scenes)

        try:
            parsed_manual_scenes = _parse_manual_script_with_ollama(prompt_text, num_scenes, OLLAMA_MODEL)
            scenes = [
                {
                    "type": "manual",
                    "manual_text": (item.get("visual_prompt") or "").strip() or prompt_text,
                    "manual_dialogue": canonical_dialogue_chunks[index],
                }
                for index, item in enumerate(parsed_manual_scenes)
            ]

            generated_dialogues = [(scene.get("manual_dialogue") or "").strip() for scene in scenes]
            integrity_ok = _manual_dialogue_integrity_ok(source_dialogue_track, generated_dialogues)
            if not integrity_ok:
                if manual_strict:
                    await status_msg.edit_text(
                        "❌ Manual strict mode: Ollama altered dialogue. Please retry with clearer quoted dialogue."
                    )
                    return

                logging.warning("[ManualGuard] Dialogue mismatch detected. Applying deterministic exact-chunk fallback.")
                for idx, scene in enumerate(scenes):
                    scene["manual_dialogue"] = canonical_dialogue_chunks[idx]
        except Exception as manual_error:
            logging.warning(f"[ManualOllama] Strict split failed, using deterministic fallback: {manual_error}")
            manual_blocks = _split_manual_scene_blocks(prompt_text, num_scenes)
            manual_dialogue_chunks = _split_script_into_dialogue_chunks(source_dialogue_track, len(manual_blocks))

            scenes = [
                {
                    "type": "manual",
                    "manual_text": block,
                    "manual_dialogue": manual_dialogue_chunks[index],
                }
                for index, block in enumerate(manual_blocks)
            ]

        # Ensure every scene has dialogue in manual mode.
        if any(not (scene.get("manual_dialogue") or "").strip() for scene in scenes):
            refill = _split_script_into_dialogue_chunks(
                source_dialogue_track,
                len(scenes),
            )
            for index, scene in enumerate(scenes):
                if not (scene.get("manual_dialogue") or "").strip():
                    scene["manual_dialogue"] = refill[index]

        for idx, scene in enumerate(scenes, start=1):
            preview = (scene.get("manual_text") or "")[:140]
            logging.info(f"[ManualScene] scene={idx} text='{preview}'")
            dialogue_preview = (scene.get("manual_dialogue") or "")[:120]
            logging.info(f"[ManualDialogue] scene={idx} dialogue='{dialogue_preview}'")
    else:
        await status_msg.edit_text(f"🧠 Parsing script into {num_scenes} scene(s)...")

        try:
            parsed = video_processor.parse_detailed_script_with_ollama(
                detailed_script=prompt_text,
                model=OLLAMA_MODEL,
                num_scenes=num_scenes,
                force_dialogue=False,
            )
        except Exception as error:
            await status_msg.edit_text(f"❌ Failed to parse story: {error}")
            return

        scenes = parsed.get("scenes", []) if isinstance(parsed, dict) else []
        if not scenes:
            await status_msg.edit_text("❌ No scenes were generated from your script.")
            return

        scenes = _ensure_scene_dialogue_from_script(scenes, prompt_text)
        for idx, scene in enumerate(scenes, start=1):
            preview = (scene.get("dialogue_text") or "")[:120]
            logging.info(f"[SceneDialogue] scene={idx} dialogue='{preview}'")

    generated_videos = []
    current_image_name = base_image_name

    for index, scene in enumerate(scenes, start=1):
        progress_label = f"🎬 Scene {index}/{len(scenes)} | Mode: {mode}"
        current_image_path = os.path.join(COMFY_INPUT_PATH, current_image_name)

        if mode == "manual":
            scene_prompt = (scene.get("manual_text") or "").strip()
            if not scene_prompt:
                scene_prompt = prompt_text
            manual_dialogue = (scene.get("manual_dialogue") or "").strip()
            scene_prompt = build_manual_scene_prompt(scene_prompt, manual_dialogue or scene_prompt)
        elif mode == "hybrid":
            scene_visual = (scene.get("visual_prompt") or scene.get("description") or prompt_text).strip()
            scene_dialogue = (scene.get("dialogue_text") or "").strip()
            scene_seed_text = f"{scene_visual} Dialogue: {scene_dialogue}" if scene_dialogue else scene_visual
            scene_prompt = await build_hybrid_prompt(scene_seed_text, status_msg, image_path=current_image_path)
            scene_prompt = ensure_audio_dialogue_suffix(scene_prompt, scene_dialogue or scene_seed_text)
        else:
            scene_visual = (scene.get("visual_prompt") or scene.get("description") or prompt_text).strip()
            scene_dialogue = (scene.get("dialogue_text") or "").strip()
            scene_seed_text = f"{scene_visual} Dialogue: {scene_dialogue}" if scene_dialogue else scene_visual
            scene_prompt = await build_dynamic_prompt(scene_seed_text, status_msg, image_path=current_image_path)
            scene_prompt = ensure_audio_dialogue_suffix(scene_prompt, scene_dialogue or scene_seed_text)

        logging.info(f"[ScenePrompt] scene={index} prompt_preview='{scene_prompt[:180]}'")

        video_path = await queue_and_wait_video(
            prompt_text=scene_prompt,
            image_name=current_image_name,
            workflow_path=workflow_path,
            status_msg=status_msg,
            progress_label=progress_label,
            scene_duration_seconds=target_scene_seconds,
        )

        if not video_path or not os.path.exists(video_path):
            await status_msg.edit_text(f"❌ Scene {index} failed. Stopping story generation.")
            return

        scene_clip_name = f"{pipeline_id}_scene_{index:03d}.mp4"
        scene_clip_path = os.path.join(pipeline_dir, scene_clip_name)
        try:
            shutil.copy2(video_path, scene_clip_path)
        except Exception as copy_error:
            logging.error(f"[Pipeline] Failed to archive scene clip | pipeline_id={pipeline_id} | scene={index} | {copy_error}")
            await status_msg.edit_text(f"❌ Failed to save scene {index} clip for stitching.")
            return

        logging.info(
            f"[Pipeline] Scene archived | pipeline_id={pipeline_id} | scene={index} | src={video_path} | dst={scene_clip_path}"
        )
        generated_videos.append(scene_clip_path)

        if index < len(scenes):
            next_image_name = f"scene_{update.effective_user.id}_{int(time.time())}_{index}.png"
            next_image_path = os.path.join(COMFY_INPUT_PATH, next_image_name)
            frame_ok = extract_last_frame_to_input(video_path, next_image_path)
            if frame_ok:
                current_image_name = next_image_name

    quality_label = "No Upscale" if upscale_resolution is None else ("2K" if upscale_resolution == 1440 else "4K")
    await status_msg.edit_text(f"🧩 Stitching scene videos... ({quality_label})")
    final_output = os.path.join(pipeline_dir, f"{pipeline_id}_final.mp4")
    stitched = video_processor.stitch_videos(generated_videos, final_output, upscale_resolution=upscale_resolution)
    if not stitched or not os.path.exists(stitched):
        await status_msg.edit_text("❌ Stitching failed.")
        return

    await status_msg.edit_text("✅ Final video ready. Uploading...")
    with open(stitched, "rb") as video_file:
        await update.message.reply_video(
            video=video_file,
            caption=f"Done! Your long-form video is ready. Quality: {quality_label}\nPipeline ID: {pipeline_id}"
        )


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Welcome!\n"
        "Use /begin to start interactive video creation.\n"
        "I will ask for image → prompt → mode → duration → quality, then generate your final video.\n"
        "Use /pipeline <pipeline_id> to resend a finished video.\n"
        "Use /manualstrict on|off to control manual dialogue integrity behavior."
    )


async def begin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    pipeline_id = f"pl_{update.effective_user.id}_{int(time.time())}"
    context.user_data["wizard_active"] = True
    context.user_data["wizard_step"] = WIZARD_STEPS["image"]
    context.user_data["wizard_data"] = {"pipeline_id": pipeline_id}
    await update.message.reply_text(
        "🎬 Wizard started.\n"
        f"Pipeline ID: `{pipeline_id}`\n"
        "Step 1/5: Please upload the reference image.",
        parse_mode="Markdown",
    )


async def cancel_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    reset_wizard_state(context)
    await update.message.reply_text("🛑 Wizard canceled. Use /begin to start again.")


async def handle_story(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Use /begin for the interactive flow.\n"
        "I’ll collect image, prompt, mode, duration, and quality step by step."
    )


async def pipeline_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Usage: /pipeline <pipeline_id>")
        return

    pipeline_id = context.args[0].strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]+", pipeline_id):
        await update.message.reply_text("❌ Invalid pipeline ID format.")
        return

    pipeline_dir = os.path.join(video_processor.APP_OUTPUT_DIR, "pipelines", pipeline_id)
    if not os.path.isdir(pipeline_dir):
        await update.message.reply_text(f"❌ Pipeline not found: `{pipeline_id}`", parse_mode="Markdown")
        return

    exact_final = os.path.join(pipeline_dir, f"{pipeline_id}_final.mp4")
    candidates = []
    if os.path.exists(exact_final):
        candidates.append(exact_final)
    candidates.extend(glob.glob(os.path.join(pipeline_dir, f"{pipeline_id}_final_*.mp4")))

    if not candidates:
        await update.message.reply_text(
            f"❌ No final video file found for pipeline `{pipeline_id}`",
            parse_mode="Markdown",
        )
        return

    final_video_path = max(candidates, key=os.path.getmtime)
    await update.message.reply_text(f"📦 Resending pipeline `{pipeline_id}`...", parse_mode="Markdown")

    try:
        with open(final_video_path, "rb") as video_file:
            await update.message.reply_video(
                video=video_file,
                caption=f"Resent from pipeline: {pipeline_id}",
                read_timeout=300,
                write_timeout=300,
                connect_timeout=60,
            )
    except Exception as error:
        logging.error(f"[Pipeline] Resend failed | pipeline_id={pipeline_id} | {error}", exc_info=True)
        await update.message.reply_text("⚠️ Could not resend video right now.")


async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.message.text:
        return

    if not context.user_data.get("wizard_active"):
        await update.message.reply_text("Use /begin to start interactive generation.")
        return

    step = context.user_data.get("wizard_step")
    wizard_data = context.user_data.setdefault("wizard_data", {})
    text = update.message.text.strip()

    if step == WIZARD_STEPS["prompt"]:
        wizard_data["prompt"] = text
        context.user_data["wizard_step"] = WIZARD_STEPS["mode"]
        await update.message.reply_text(
            "Step 3/5: Choose mode by sending one word: `auto`, `manual`, or `hybrid`",
            parse_mode="Markdown",
        )
        return

    if step == WIZARD_STEPS["mode"]:
        selected_mode = text.lower()
        if selected_mode not in {"auto", "manual", "hybrid"}:
            await update.message.reply_text("Invalid mode. Please send: auto / manual / hybrid")
            return
        wizard_data["mode"] = selected_mode
        context.user_data["wizard_step"] = WIZARD_STEPS["duration"]
        await update.message.reply_text("Step 4/5: Enter video duration in seconds (e.g., 36, 60, 120).")
        return

    if step == WIZARD_STEPS["duration"]:
        if not text.isdigit():
            await update.message.reply_text("Please send duration as a number in seconds.")
            return
        duration = int(text)
        if duration < 12 or duration > 600:
            await update.message.reply_text("Please choose duration between 12 and 600 seconds.")
            return

        wizard_data["duration"] = duration
        context.user_data["wizard_step"] = WIZARD_STEPS["quality"]
        await update.message.reply_text(
            "Step 5/5: Choose output quality: `no` (no upscale), `2k`, or `4k`",
            parse_mode="Markdown",
        )
        return

    if step == WIZARD_STEPS["quality"]:
        selected_quality = text.lower()
        if selected_quality not in UPSCALE_PRESETS:
            await update.message.reply_text("Invalid quality. Please send: no / 2k / 4k")
            return

        wizard_data["quality"] = selected_quality
        status_msg = await update.message.reply_text("⚙️ Starting long-form generation...")
        await run_story_generation(update, context, status_msg)
        reset_wizard_state(context)
        return

    await update.message.reply_text("Step 2/5: Send your story/script prompt text.")


# --- 4. COMMAND HANDLERS ---
async def workflows_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        files = sorted([name for name in os.listdir(WORKFLOW_API_PATH) if name.endswith(".json")])
        if not files:
            await update.message.reply_text("No workflows found in workflow_api.")
            return

        keyboard = [[InlineKeyboardButton(os.path.splitext(name)[0], callback_data=f"workflow_{name}")] for name in files]
        reply_markup = InlineKeyboardMarkup(keyboard)
        current_workflow = context.user_data.get("workflow_file", "video_ltx2_3_i2v.json")
        await update.message.reply_text(
            f"🔀 Select a Workflow\nCurrent: `{current_workflow}`",
            reply_markup=reply_markup,
            parse_mode="Markdown",
        )
    except Exception as error:
        await update.message.reply_text(f"❌ Error listing workflows: {error}")


async def workflow_button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    workflow_filename = query.data.split("workflow_", 1)[1]
    context.user_data["workflow_file"] = workflow_filename
    await query.edit_message_text(text=f"✅ Workflow selected: `{workflow_filename}`", parse_mode="Markdown")


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        response = requests.get(f"{COMFY_URL}/queue", timeout=5).json()
        running = len(response.get("queue_running", []))
        pending = len(response.get("queue_pending", []))
        msg = (
            f"🖥️ **ComfyUI Status**\n\n"
            f"⚡ **Currently Rendering:** {running}\n"
            f"📝 **Waiting in Queue:** {pending}\n\n"
        )
        msg += "⏳ System Busy" if running > 0 else "✅ Ready"
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as error:
        await update.message.reply_text(f"❌ Error reaching ComfyUI: {error}")


async def mode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Set or view prompt mode: auto (Ollama) or manual (caption as-is)."""
    current_mode = context.user_data.get("prompt_mode", "auto")

    if not context.args:
        await update.message.reply_text(
            f"⚙️ Current mode: *{current_mode}*\n"
            "Use `/mode auto` for Ollama prompt generation\n"
            "Use `/mode manual` to pass your caption as-is\n"
            "Use `/mode hybrid` for Ollama visual prompt + locked user dialogue",
            parse_mode="Markdown",
        )
        return

    requested_mode = context.args[0].strip().lower()
    if requested_mode not in {"auto", "manual", "hybrid"}:
        await update.message.reply_text(
            "❌ Invalid mode. Use `/mode auto`, `/mode manual`, or `/mode hybrid`",
            parse_mode="Markdown",
        )
        return

    context.user_data["prompt_mode"] = requested_mode
    await update.message.reply_text(f"✅ Prompt mode set to *{requested_mode}*", parse_mode="Markdown")


async def manual_strict_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Toggle strict dialogue-integrity guard for manual mode."""
    current = bool(context.user_data.get("manual_strict", False))

    if not context.args:
        await update.message.reply_text(
            f"🔒 Manual strict mode is currently: *{'ON' if current else 'OFF'}*\n"
            "Use `/manualstrict on` to fail when Ollama alters dialogue.\n"
            "Use `/manualstrict off` for soft-guard auto-correction.",
            parse_mode="Markdown",
        )
        return

    value = context.args[0].strip().lower()
    if value not in {"on", "off"}:
        await update.message.reply_text("❌ Use `/manualstrict on` or `/manualstrict off`", parse_mode="Markdown")
        return

    context.user_data["manual_strict"] = value == "on"
    await update.message.reply_text(
        f"✅ Manual strict mode set to *{'ON' if value == 'on' else 'OFF'}*",
        parse_mode="Markdown",
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        nvmlInit()
        handle = nvmlDeviceGetHandleByIndex(0)
        gpu_name = nvmlDeviceGetName(handle)
        gpu_temp = nvmlDeviceGetTemperature(handle, NVML_TEMPERATURE_GPU)
        power_draw = nvmlDeviceGetPowerUsage(handle) / 1000
        util_rates = nvmlDeviceGetUtilizationRates(handle)
        mem_info = nvmlDeviceGetMemoryInfo(handle)
        nvmlShutdown()

        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_freq = psutil.cpu_freq()
        cpu_cores = psutil.cpu_count(logical=True)
        ram = psutil.virtual_memory()
        disk_c = psutil.disk_usage("C:/")
        disk_d = psutil.disk_usage("D:/")

        msg = (
            f"💻 **System Vitals**\n\n"
            f"🔥 **GPU ({gpu_name})**\n"
            f"- Temp: {gpu_temp}°C\n"
            f"- Usage: {util_rates.gpu}%\n"
            f"- Power: {power_draw:.1f}W\n"
            f"- VRAM: {mem_info.used / 1024**3:.1f}GB / {mem_info.total / 1024**3:.1f}GB\n\n"
            f"🧠 **CPU ({cpu_cores} cores)**\n"
            f"- Usage: {cpu_percent}%\n"
            f"- Frequency: {cpu_freq.current:.0f} MHz\n\n"
            f"💾 **RAM**\n"
            f"- Usage: {ram.percent}%\n"
            f"- Used: {ram.used / 1024**3:.1f}GB / {ram.total / 1024**3:.1f}GB\n\n"
            f"💽 **Disk**\n"
            f"- C: {disk_c.percent}% used ({disk_c.free / 1024**3:.1f}GB free)\n"
            f"- D: {disk_d.percent}% used ({disk_d.free / 1024**3:.1f}GB free)"
        )
        await update.message.reply_text(msg, parse_mode="Markdown")
    except Exception as error:
        await update.message.reply_text(f"❌ Error fetching vitals: {error}")


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message.photo:
        await update.message.reply_text("📸 Please send a photo with a caption.")
        return

    if context.user_data.get("wizard_active"):
        step = context.user_data.get("wizard_step")
        if step != WIZARD_STEPS["image"]:
            await update.message.reply_text("Image already received. Please continue with the next text step.")
            return

        user_id = update.message.from_user.id
        image_name = f"wizard_{user_id}_{int(time.time())}.png"
        image_path = os.path.join(COMFY_INPUT_PATH, image_name)
        try:
            photo_file = await update.message.photo[-1].get_file()
            await photo_file.download_to_drive(image_path)
            wizard_data = context.user_data.setdefault("wizard_data", {})
            wizard_data["image_name"] = image_name
            context.user_data["wizard_step"] = WIZARD_STEPS["prompt"]
            await update.message.reply_text(
                "✅ Image received.\n"
                "Step 2/5: Send your full story prompt/dialogue text."
            )
        except Exception as error:
            await update.message.reply_text(f"❌ Failed to save image: {error}")
        return

    user_id = update.message.from_user.id
    workflow_filename = context.user_data.get("workflow_file", "video_ltx2_3_i2v.json")
    workflow_path = os.path.join(WORKFLOW_API_PATH, workflow_filename)

    if not os.path.exists(workflow_path):
        await update.message.reply_text(f"❌ Workflow file not found: `{workflow_filename}`", parse_mode="Markdown")
        return

    raw_caption = update.message.caption if update.message.caption else "A person talking to camera."
    status_msg = await update.message.reply_text(f"⏳ Using `{workflow_filename}`...", parse_mode="Markdown")
    prompt_mode = context.user_data.get("prompt_mode", "auto")

    try:
        photo_file = await update.message.photo[-1].get_file()
        image_name = f"tg_{user_id}.png"
        image_path = os.path.join(COMFY_INPUT_PATH, image_name)
        await photo_file.download_to_drive(image_path)
        logging.info(f"[Telegram] Image downloaded: {image_path}")
    except Exception as error:
        await status_msg.edit_text(f"❌ Download failed: {error}")
        return

    logging.info(f"[Telegram] Request received | user_id={user_id} | workflow={workflow_filename}")
    logging.info(f"[Telegram] Caption received: {raw_caption}")
    logging.info(f"[Prompt] Mode selected: {prompt_mode}")

    if prompt_mode == "manual":
        final_complex_prompt = raw_caption.strip() if raw_caption.strip() else "A person talking to camera."
        await status_msg.edit_text("📝 Manual mode: using your caption as-is")
        logging.info(f"[Prompt] Manual mode prompt (as-is): {final_complex_prompt}")
    elif prompt_mode == "hybrid":
        final_complex_prompt = await build_hybrid_prompt(raw_caption, status_msg, image_path=image_path)
    else:
        final_complex_prompt = await build_dynamic_prompt(raw_caption, status_msg, image_path=image_path)

    logging.info(f"[Prompt] Prompt selected for node 267:266: {final_complex_prompt}")

    prompt_preview = final_complex_prompt
    if len(prompt_preview) > 1400:
        prompt_preview = prompt_preview[:1400] + " ...[truncated]"
    await status_msg.edit_text(
        f"📝 Final prompt ({prompt_mode} mode):\n\n{prompt_preview}\n\n⏳ Queuing render..."
    )

    try:
        with open(workflow_path, "r", encoding="utf-8") as workflow_file:
            workflow = json.load(workflow_file)

        workflow["269"]["inputs"]["image"] = image_name
        workflow["267:266"]["inputs"]["value"] = final_complex_prompt
        # Critical: preserve exact dialogue by bypassing TextGenerateLTX2Prompt rewrite stage.
        # Feed final prompt directly to CLIP Text Encode so Audio Dialogue remains verbatim.
        if "267:240" in workflow and "inputs" in workflow["267:240"]:
            workflow["267:240"]["inputs"]["text"] = final_complex_prompt
            logging.info("[ComfyUI] Dialogue route: direct text injection to node 267:240 (rewrite bypass enabled)")
        # Keep 267:266 updated for compatibility/preview nodes in the workflow.
        if "267:274" in workflow and "inputs" in workflow["267:274"]:
            workflow["267:274"]["inputs"]["sampling_mode"] = "off"
            logging.info("[ComfyUI] TextGenerateLTX2Prompt sampling_mode forced to off")
        logging.info(
            f"[ComfyUI] Injected workflow inputs | image_node=269 image={image_name} | text_node=267:266 text={final_complex_prompt}"
        )

        response = requests.post(f"{COMFY_URL}/prompt", data=json.dumps({"prompt": workflow}), timeout=30)
        response.raise_for_status()
        payload = response.json()
        prompt_id = payload.get("prompt_id")

        if not prompt_id:
            await status_msg.edit_text(f"❌ Error queuing prompt: `{payload}`", parse_mode="Markdown")
            return

        ACTIVE_PROMPT_IDS.add(prompt_id)
        logging.info(f"[ComfyUI] Queued prompt | prompt_id={prompt_id}")
        await status_msg.edit_text(f"🚀 Render ID: `{prompt_id[:8]}` started...", parse_mode="Markdown")
        await poll_for_completion(prompt_id, update, status_msg)
    except Exception as error:
        logging.error(f"[ComfyUI] Queue/trigger failure: {error}", exc_info=True)
        await status_msg.edit_text(f"❌ System Error: `{error}`", parse_mode="Markdown")


async def poll_for_completion(prompt_id, update, status_msg):
    history_url = f"{COMFY_URL}/history/{prompt_id}"
    user_id = update.message.from_user.id
    logging.info(f"[ComfyUI] Polling started | prompt_id={prompt_id}")

    for attempt in range(120):
        await asyncio.sleep(15)
        try:
            response = requests.get(history_url, timeout=15).json()
            if prompt_id in response:
                outputs = response[prompt_id].get("outputs", {})
                video_filename = None

                for _, output_data in outputs.items():
                    key = "gifs" if "gifs" in output_data else "images" if "images" in output_data else None
                    if key and output_data[key]:
                        video_filename = output_data[key][0].get("filename")
                        break

                if not video_filename:
                    await status_msg.edit_text("⚠️ Render finished but no output file found.")
                    ACTIVE_PROMPT_IDS.discard(prompt_id)
                    return

                video_path = os.path.join(COMFY_OUTPUT_PATH, video_filename)
                logging.info(f"[ComfyUI] Output ready: {video_path}")
                ACTIVE_PROMPT_IDS.discard(prompt_id)
                try:
                    try:
                        await status_msg.edit_text("✅ Render finished. Uploading video...")
                    except BadRequest as edit_error:
                        if "Message is not modified" not in str(edit_error):
                            raise

                    with open(video_path, "rb") as video_file:
                        await update.message.reply_video(
                            video=video_file,
                            caption="Done! Your generation is ready.",
                            read_timeout=300,
                            write_timeout=300,
                            connect_timeout=60,
                        )

                    try:
                        await status_msg.delete()
                    except Exception:
                        pass

                    logging.info(f"[Telegram] Response delivered | user_id={user_id} | prompt_id={prompt_id}")
                except (TimedOut, NetworkError) as send_error:
                    logging.warning(f"[Telegram] Video upload timeout/network issue | prompt_id={prompt_id} | {send_error}")
                    await update.message.reply_text(
                        "⚠️ Render is finished, but upload timed out. Please try `/status` and request resend."
                    )
                except Exception as send_error:
                    logging.error(f"[Telegram] Video upload failed | prompt_id={prompt_id} | {send_error}", exc_info=True)
                    await update.message.reply_text(
                        "⚠️ Render completed, but I could not upload the video automatically."
                    )
                return

            logging.debug(f"[ComfyUI] Waiting... poll={attempt + 1}/120 | prompt_id={prompt_id}")
        except Exception as error:
            logging.warning(f"[ComfyUI] Poll error | prompt_id={prompt_id} | {error}")

    ACTIVE_PROMPT_IDS.discard(prompt_id)
    await status_msg.edit_text("⚠️ Timeout: Render taking too long.")
    logging.warning(f"[ComfyUI] Poll timeout | prompt_id={prompt_id}")


# --- 5. BACKGROUND ALERTS ---
async def check_vitals_and_alert(context: ContextTypes.DEFAULT_TYPE):
    if not ADMIN_CHAT_ID:
        return

    try:
        nvmlInit()
        handle = nvmlDeviceGetHandleByIndex(0)
        gpu_temp = nvmlDeviceGetTemperature(handle, NVML_TEMPERATURE_GPU)
        nvmlShutdown()

        vitals = {
            "gpu_temp": gpu_temp,
            "cpu_usage": psutil.cpu_percent(interval=1),
            "ram_usage": psutil.virtual_memory().percent,
            "disk_usage_c": psutil.disk_usage("C:/").percent,
            "disk_usage_d": psutil.disk_usage("D:/").percent,
        }

        for key, value in vitals.items():
            if value >= THRESHOLDS[key] and not alert_state[key]:
                await context.bot.send_message(
                    chat_id=ADMIN_CHAT_ID,
                    text=(
                        "🚨 **High Usage Alert**\n"
                        f"Metric: **{key.replace('_', ' ').title()}**\n"
                        f"Current: **{value}%**\n"
                        f"Threshold: **{THRESHOLDS[key]}%**"
                    ),
                    parse_mode="Markdown",
                )
                alert_state[key] = True
            elif value < THRESHOLDS[key] and alert_state[key]:
                await context.bot.send_message(
                    chat_id=ADMIN_CHAT_ID,
                    text=f"✅ **Alert Resolved**\n{key.replace('_', ' ').title()} back to normal.",
                    parse_mode="Markdown",
                )
                alert_state[key] = False
    except Exception as error:
        logging.warning(f"[Alerts] Check failed: {error}")


# --- 6. MAIN ---
if __name__ == "__main__":
    logging.info("🤖 OpenClaw Bridge is starting...")

    if not BOT_TOKEN:
        raise RuntimeError("Missing BOT_TOKEN environment variable. Set BOT_TOKEN before running telegram_gate.py")

    ws_thread = threading.Thread(target=listen_to_comfyui_websocket, daemon=True)
    ws_thread.start()
    logging.info("[ComfyUI-WS] Listener thread started")

    app = ApplicationBuilder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("begin", begin_command))
    app.add_handler(CommandHandler("cancel", cancel_command))
    app.add_handler(CommandHandler("pipeline", pipeline_command))
    app.add_handler(CommandHandler("workflows", workflows_command))
    app.add_handler(CommandHandler("mode", mode_command))
    app.add_handler(CommandHandler("manualstrict", manual_strict_command))
    app.add_handler(CommandHandler("stats", stats_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("story", handle_story))
    app.add_handler(CallbackQueryHandler(workflow_button_callback, pattern="^workflow_"))
    app.add_handler(MessageHandler(filters.PHOTO, handle_message))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    if app.job_queue:
        app.job_queue.run_repeating(check_vitals_and_alert, interval=ALERT_INTERVAL_SECONDS, first=10)

    logging.info("[Telegram] Bot is live")
    app.run_polling()