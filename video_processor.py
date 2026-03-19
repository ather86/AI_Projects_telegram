"""
This module contains the core logic for processing and generating long-form videos
based on a script. It is adapted from the LTXI2V project and is designed to be
used as a backend service for the Telegram bot.
"""
import os
import json
import time
import uuid
import websocket
import subprocess
import copy
import sys
import shutil
import random
import logging
import re
import requests
import ollama
from uuid import uuid4

# --- Configuration (to be aligned with telegram_gate.py) ---
COMFYUI_URL = "http://127.0.0.1:8188"
COMFYUI_WS_URL = f"ws://{COMFYUI_URL.split('//')[1]}/ws"
COMFYUI_REAL_INPUT_DIR = "D:\\StabilityMatrix-win-x64\\Data\\Packages\\ComfyUI\\input"
COMFYUI_REAL_OUTPUT_DIR = "D:\\StabilityMatrix-win-x64\\Data\\Packages\\ComfyUI\\output"
APP_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

def recover_malformed_scenes(scenes_list):
    """
    Recovers scenes when the LLM returns a mix of dicts and raw strings in the scenes array.
    e.g. [{"type":"dialogue",...}, "some dialogue text", "visual_prompt: description"]
    Merges stray strings into the nearest valid scene object.
    Returns a list of valid scene dicts.
    """
    if not isinstance(scenes_list, list):
        return scenes_list

    recovered = []
    last_valid = None
    for idx, s in enumerate(scenes_list):
        if isinstance(s, dict):
            if "type" not in s:
                s["type"] = "visual_segment"
                logging.warning(f"Scene {idx} missing 'type' field, defaulting to 'visual_segment'")
            last_valid = s
            recovered.append(s)
        elif isinstance(s, str) and s.strip():
            text = s.strip()
            logging.warning(f"Scene {idx} is a raw string, recovering: '{text[:100]}...'")

            if text.lower().startswith("visual_prompt:") or text.lower().startswith("visual_prompt :"):
                visual_text = text.split(":", 1)[1].strip()
                if last_valid and not last_valid.get("visual_prompt"):
                    last_valid["visual_prompt"] = visual_text
                    logging.info(f"  -> Attached as visual_prompt to previous scene")
                else:
                    new_scene = {"type": "visual_segment", "visual_prompt": visual_text, "description": visual_text}
                    recovered.append(new_scene)
                    last_valid = new_scene
                    logging.info(f"  -> Created new visual_segment from visual_prompt string")
            elif last_valid and last_valid.get("type") == "dialogue" and not last_valid.get("dialogue_text"):
                last_valid["dialogue_text"] = text
                logging.info(f"  -> Attached as dialogue_text to previous dialogue scene")
            elif last_valid:
                existing = last_valid.get("description", "")
                last_valid["description"] = (existing + " " + text).strip() if existing else text
                logging.info(f"  -> Appended to previous scene's description")
            else:
                new_scene = {"type": "visual_segment", "visual_prompt": text, "description": text}
                recovered.append(new_scene)
                last_valid = new_scene
                logging.info(f"  -> Created new visual_segment from stray text")
        else:
            logging.warning(f"Scene {idx}: Skipping unrecognizable entry of type {type(s)}: {s}")

    if len(recovered) != len(scenes_list):
        logging.info(f"Scene recovery: {len(scenes_list)} raw entries -> {len(recovered)} valid scenes")

    return recovered


def safely_parse_json_with_control_chars(json_str):
    """
    Parse JSON string while properly handling control characters (newlines, tabs) that
    can appear in LLM responses.
    """
    sanitized = json_str.replace('\\n', ' ').replace('\\r', ' ').replace('\\t', ' ')
    sanitized = re.sub(r'\\s+', ' ', sanitized)

    try:
        return json.loads(sanitized)
    except json.JSONDecodeError as e:
        error_pos = e.pos
        context_start = max(0, error_pos - 100)
        context_end = min(len(sanitized), error_pos + 100)
        error_context = sanitized[context_start:context_end]
        logging.error(f"JSON parsing failed at position {error_pos}: {e.msg}")
        logging.error(f"Context around error: ...{error_context}...")
        raise ValueError(f"Failed to parse JSON response: {e}") from e


def parse_detailed_script_with_ollama(detailed_script, model, num_scenes=1, force_dialogue=False):
    """
    Uses Ollama to parse a detailed narrative script into a structured JSON format
    for sequential video generation.
    """
    logging.info(f"Parsing detailed script with Ollama using model: {model}, target scenes: {num_scenes}")

    system_prompt = (
        "You are an expert script parser. Your task is to take a detailed video script "
        "and extract its components into a structured JSON format. The script may describe a story, a tutorial, an advertisement, or another concept. "
        "Identify a 'global_visual_description' that sets the overall style (e.g., '3D animation', 'live-action cooking show', 'cinematic product ad'). "
        "If specific, recurring characters are defined, list them in a 'characters' array. If no characters are defined, this key can be omitted. "
        f"CRITICAL SCENE COUNT: You MUST split the script into EXACTLY {num_scenes} scenes in the 'scenes' array. Each scene represents a 12-second video segment. "
        f"If the script has fewer natural breaks, split dialogue and action evenly across {num_scenes} scenes. If the script is short, create variations/continuations to fill all {num_scenes} scenes. "
        "Break the script into a 'scenes' array. Each scene MUST be a JSON object with 'type' and other fields. "
        "For each scene, provide a 'visual_prompt' that summarizes the key visual elements for the AI to generate. "
        "CRITICAL - DIALOGUE PRESERVATION: If a scene contains dialogue, ALWAYS include 'character' and 'dialogue_text' fields WITHIN THE SAME SCENE OBJECT. "
        "DIALOGUE TEXT MUST BE COPIED EXACTLY AS WRITTEN IN THE SOURCE SCRIPT — WORD FOR WORD, CHARACTER FOR CHARACTER. "
        "DO NOT translate, paraphrase, rewrite, edit, rephrase, or 'improve' ANY dialogue. "
        "If the user wrote dialogue in Hinglish (Hindi words in English/Roman script like 'Arre tum itna lamba muh kaise bana lete ho'), "
        "you MUST copy that EXACT Hinglish text into dialogue_text. Do NOT convert it to English or formal Hindi. "
        "If the user wrote dialogue in any language using Roman script, preserve it exactly as-is. "
        "When dialogue has special characters, quotes, or non-English text, ensure proper JSON escaping. "
        "ABSOLUTE RULE - TYPE FIELD: ANY scene that contains 'dialogue_text' MUST have its 'type' set to 'dialogue'. "
        "NEVER use 'visual_segment' for a scene that has dialogue_text. This is the #1 most important rule. "
        "If a character speaks in a scene, that scene's type MUST be 'dialogue', period. "
        "CRITICAL - LANGUAGE & ACCENT CONSISTENCY: Detect the language style from the script. "
        "If the script contains Hinglish (Hindi words in Roman script like 'kya', 'hai', 'yaar', 'bhai', 'arre', 'accha'), "
        "set language to 'Hinglish' and accent to 'native Hindi desi'. "
        "If the script is in pure Hindi (Devanagari), set language to 'Hindi' and accent to 'native Hindi desi'. "
        "If the script is in English, set language to 'English' and accent to 'neutral American'. "
        "Add top-level 'language' and 'accent' fields to the JSON output. "
        "CRITICAL - VISUAL PROMPT MUST NOT CONTAIN SILENCE MARKERS FOR DIALOGUE SCENES: "
        "If a scene has 'dialogue_text', the 'visual_prompt' field MUST NOT contain any phrases like "
        "'No speech', 'No dialogue', 'No voiceover', 'No talking', 'Silent scene', or 'No human voice'. "
        "These silence instructions destroy the dialogue during video generation. Only describe the visual action in 'visual_prompt'. "
        "CRITICAL - SCENE UNIQUENESS: Each scene MUST have a visually DISTINCT 'visual_prompt'. "
        "Do NOT repeat or closely paraphrase the same visual description across multiple scenes. "
        "Each scene must describe a DIFFERENT camera angle, character action, setting detail, or moment in the story. "
        "If two scenes involve the same characters talking, differentiate them by: camera angle (close-up vs wide), character positioning, expressions, gestures, background elements, or lighting changes. "
        "If a scene contains on-screen text or titles, use a 'title_card' type with a 'text' field. "
        "IMPORTANT: Every scene object must have this structure: {\"type\": \"...\", \"character\": \"...\", \"dialogue_text\": \"...\", \"visual_prompt\": \"...\"} "
        "The top-level JSON must include: {\"global_visual_description\": \"...\", \"language\": \"...\", \"accent\": \"...\", \"characters\": [...], \"scenes\": [...]} "
        "ENSURE THE FINAL OUTPUT IS VALID, PROPERLY FORMATTED JSON. Each array element is a complete object. "
        "Your entire response must be ONLY the JSON object. Do not include any conversational text, introductions, or explanations. Your response must start with `{` and end with `}`."
    )

    if force_dialogue:
        system_prompt += (
            "CRITICAL - EVERY SCENE MUST HAVE DIALOGUE: The user requires that ALL scenes contain dialogue. "
            "Distribute the script's dialogue evenly across ALL scenes. Do NOT leave any scene without dialogue_text. "
            "If the script has limited dialogue, split existing dialogue into smaller parts so every scene has some speech. "
            "Every single scene object MUST have a non-empty 'dialogue_text' field and type='dialogue'. Zero silent scenes allowed. "
        )

    try:
        response = ollama.chat(
            model=model,
            format='json',
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': detailed_script}
            ],
            options={
                'temperature': 0.5,
                'num_ctx': 8192,
                'num_predict': 4096
            }
        )
        
        content = response['message']['content']
        logging.info(f"Ollama response received (partial): {content[:500]}...")

        json_start = content.find('{')
        json_end = content.rfind('}') + 1
        if json_start == -1 or json_end == -1 or json_end <= json_start:
            raise ValueError("No JSON object found in Ollama's response.")
        
        json_str = content[json_start:json_end]
        parsed_data = safely_parse_json_with_control_chars(json_str)

        if "global_visual_description" not in parsed_data or "scenes" not in parsed_data:
            raise ValueError("Parsed JSON is missing required top-level keys.")

        parsed_data["scenes"] = recover_malformed_scenes(parsed_data["scenes"])
        if not parsed_data["scenes"]:
            raise ValueError("No valid scenes could be recovered from the Ollama response.")

        logging.info(f"Successfully parsed script into {len(parsed_data['scenes'])} events.")
        return parsed_data

    except Exception as e:
        logging.error(f"Failed during Ollama script parsing: {e}", exc_info=True)
        raise

def stitch_videos(video_files, output_path, upscale_resolution=None):
    """
    Stitches multiple video files into one and optionally upscales it using ffmpeg.
    """
    if not video_files:
        logging.error("No video files to stitch.")
        return None

    logging.info(f"Stitching {len(video_files)} video files into {output_path}")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Create a temporary file list for ffmpeg
    list_file_path = os.path.join(APP_OUTPUT_DIR, f"stitch_list_{uuid4().hex}.txt")
    with open(list_file_path, "w", encoding="utf-8", newline="\n") as f:
        for video_file in video_files:
            normalized_path = os.path.abspath(video_file).replace("\\", "/")
            normalized_path = normalized_path.replace("'", "'\\''")
            f.write(f"file '{normalized_path}'\n")

    # Base ffmpeg command for concatenation
    ffmpeg_cmd = [
        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file_path,
        "-c", "copy",
        output_path
    ]

    ffmpeg_fallback_cmd = [
        "ffmpeg",
        "-y",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file_path,
        "-c:v", "libx264",
        "-preset", "medium",
        "-crf", "20",
        "-c:a", "aac",
        "-b:a", "192k",
        output_path,
    ]

    try:
        # Run concatenation
        try:
            subprocess.run(ffmpeg_cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as error:
            logging.warning(f"Stream-copy concat failed, retrying with re-encode. Reason: {error}")
            subprocess.run(ffmpeg_fallback_cmd, check=True, capture_output=True, text=True)
        logging.info(f"Successfully stitched video to {output_path}")

        # Optional Upscaling
        if upscale_resolution:
            upscaled_output_path = output_path.replace(".mp4", f"_{upscale_resolution}.mp4")
            logging.info(f"Upscaling video to {upscale_resolution} at {upscaled_output_path}")
            
            # ffmpeg upscale command
            upscale_cmd = [
                "ffmpeg",
                "-i", output_path,
                "-vf", f"scale=-1:{upscale_resolution}",
                "-c:v", "libx264",
                "-preset", "slow",
                "-crf", "22",
                "-c:a", "aac",
                "-b:a", "192k",
                upscaled_output_path
            ]
            subprocess.run(upscale_cmd, check=True, capture_output=True, text=True)
            logging.info(f"Successfully upscaled video to {upscaled_output_path}")
            os.remove(output_path) # remove the non-upscaled version
            return upscaled_output_path

        return output_path

    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg failed. Command: {' '.join(e.cmd)}")
        logging.error(f"FFmpeg stderr: {e.stderr}")
        return None
    except FileNotFoundError:
        logging.error("FFmpeg not found. Please ensure ffmpeg is installed and in your system's PATH.")
        return None
    finally:
        # Clean up the temporary list file
        if os.path.exists(list_file_path):
            os.remove(list_file_path)
