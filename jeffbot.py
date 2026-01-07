import discord
import openai
import asyncio
import aiofiles
import aiohttp
import os
import json
import time
import sys
import math
import random
import copy
import secrets, string
from dotenv import load_dotenv
from discord.ext import commands, tasks
from discord import ButtonStyle, app_commands, Interaction
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from datetime import time as dtime  # for midnight
from openai import AsyncOpenAI
from urllib.parse import quote_plus, quote  # NEW: for URL‐encoding summoner names
from typing import Optional
from flask import Flask, request
from flask_cors import CORS
from threading import Thread
from playwright.async_api import async_playwright
import tiktoken
import re
import random

load_dotenv()
_opgg_refresh_lock = asyncio.Lock()
client = AsyncOpenAI(api_key=os.getenv("OPENAI_API_KEY"))
DISCORD_TOKEN = os.getenv('DISCORD_TOKEN')

# The ID of the user whose messages you want to collect
TARGET_USER_ID = 184481785172721665  # e.g., 123456789012345678

# Maximum number of messages to collect from the user
MAX_USER_MESSAGES = 8000

SHOP_COST = 10000  # Adjust this to 10000 if you want a higher cost

TEST_MODE = os.getenv('TEST_MODE')

OP_GG_REGION = "na" 
DRAFTLOL_BASE_URL = "https://draftlol.dawe.gg"

FLEX_LEADERBOARD_HISTORY_FILE = "flex_leaderboard_history.json"


# List of channel IDs to collect messages from
TARGET_CHANNEL_IDS = [753959443263389737, 781309198855438336]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Update the file paths to use absolute paths
MESSAGES_FILE = os.path.join(SCRIPT_DIR, 'user_messages.json')
SUMMARY_FILE = os.path.join(SCRIPT_DIR, 'user_summary.txt')
BALANCES_FILE = os.path.join(SCRIPT_DIR, 'user_balances.json')
DAILY_COOLDOWN_FILE = os.path.join(SCRIPT_DIR, 'daily_cooldowns.json')
FLEX_RANK_SNAPSHOTS_FILE = os.path.join(SCRIPT_DIR, "flex_rank_snapshots.json")

# Persistent Flex leaderboard message pointers (so the bot can edit 1 message instead of spamming new ones)
FLEX_PERSISTENT_MESSAGES_FILE = os.path.join(SCRIPT_DIR, "flex_persistent_messages.json")

JOSH_GUILD_ID = 753949534387961877
JOSH_USER_ID = 187737483088232449

# Josh's SoloQ is on NA1 (platform routing for summoner/league endpoints)
JOSH_LOL_PLATFORM = "na1"

# Persist last seen LP so restarts don't spam edits
JOSH_SOLOQ_STATE_FILE = "josh_soloq_lp_state.json"

USE_SUMMARY_FOR_CONTEXT = False
JEFF = True

# Initialize the bot client with intents
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
intents.guilds = True

bot = commands.Bot(command_prefix='!!', intents=intents)

# A set to store message IDs to avoid duplicates
collected_message_ids = set()

# A list to store the target user's messages
user_messages = []

# Progress counters
total_messages_processed = 0
user_messages_collected = 0

LOCAL_TIMEZONE = timezone(timedelta(hours=-4))

# Track voice clients per guild
voice_clients: dict[int, discord.VoiceClient] = {}



# Dictionary to store user balances
user_balances = {}

active_games = set()

IGNORE_FILE = os.path.join(SCRIPT_DIR, 'ignore_list.json')
SPAM_FILE = os.path.join(SCRIPT_DIR, 'spam_state.json')

DISABLE_FILE = os.path.join(SCRIPT_DIR, 'disable_state.json')
ALLOWED_DISABLE_USERS = {187737483088232449, 133017322800545792, 137776800234209281}

_disable_state = {"until": 0}  # epoch seconds


# Dictionary to map Discord user IDs to op.gg URLs for League of Legends
id_to_opgg = {

    187737483088232449: "Trombone#NA1"

}

# Dictionary to store active custom game lobbies
custom_lobbies = {}  # key: message.id of lobby message, value: LobbyData object

# Data class to track lobby state
class LobbyData:
    def __init__(self, creator_id, max_players: int = 10):
        self.creator_id = creator_id
        self.max_players = max_players 
        self.players = []  # list of discord.Member
        self.message = None  # discord.Message for the lobby embed
        self.guild = None
        self.captains_selected = False
        self.captains = []  # list of discord.Member
        self.current_picker_index = 0  # index in captains list
        self.teams = {0: [], 1: []}  # teams 0=blue, 1=red
        self.side_selected = None  # 0 for blue, 1 for red
        self.draft_phase = 'awaiting_players'

# Additional functionality configuration
RANDOM_RESPONSE_CHANCE = 200  # 1 means 1% chance (adjust this variable as needed)
RANDOM_RESPONSE_CHANNEL_ID = [1310101027550400565, 1330337673684193303]  # Channel where random responses will be sent

SPECIAL_USER_ID = 329843089214537729  # Specific user for "Lenny 😋" response
SPECIAL_USER_RESPONSE_CHANCE = 50  # 10% chance
SPECIAL_USER_RESPONSE = "Lenny 😋"

SPAM_WINDOWS = [
    (5, 6 * 60),    # 5 in 5 minutes
    (7, 15 * 60),   # 7 in 15 minutes
]


USER_ID_MAPPING = {
    133017322800545792: ["Parky", "Parker"],
    379521006764556291: ["Cameron"],
    197414593566343168: ["Oqi"],
    295293382811582467: ["Ash"],
    343220246787915778: ["Reid"],
    806382485276983296: ["Trent"],
    280132607423807489: ["Caleb"],
    329843089214537729: ["Lenny", "Bi", "Zerox", "Lengy"],
    387688894746984448: ["Liam"],
    435963643721547786: ["Cody"],
    187737483088232449: ["Blake"],
    148907426442248193: ["Cylainius", "Marcus"],
    438103809399455745: ["Josh"],
    424431225764184085: ["Willy"],
    241746019765714945: ["Micheal", "Michael"],
    1286079484830945341: ["Jeff Bot"],
    191765702670024715: ["Missy"],
    184481785172721665: ["Jeff"],
    139938273698119680: ["Chip", "Keon"],
    1011380418131529769: ["Trey"],
    308424467971964930: ["Alec"],
    192506069849735169: ["Madi"]
}

MENTION_SEP = "::"

DPM_FLEX_PROFILES = {
    "Blake": {
        "puuid": "mV8WBqdnPXtD_grbs_nXqZXCSMfEJhnb1pW11vJXbJO7p6JqzAfbZgCYcND0DVk0R8l5gDX3AxzMOQ",
        "opgg_url": "https://op.gg/lol/summoners/na/Schmort-bone?queue_type=FLEXRANKED",
    },
    "Parky": {
        "puuid": "c1OusXt1PnpBHUcPhYB5tVGxaaJiHtllltrNp_d8PcsXQn-YjhiBuqsziEe6ThzDCtCkebYVV-hIsQ",
        "opgg_url": "https://op.gg/lol/summoners/na/Parky-NA1?queue_type=FLEXRANKED",
    },
    "Josh": {
        "puuid": "UjiUcaCRKmCMYGQ8i_9u1hVzE5GvwloxPz3vG7jR2mUgSXvItkufi4LJXGZ_54lgHB23evgqnlvNJw",
        "opgg_url": "https://op.gg/lol/summoners/na/DrkCloak-NA1?queue_type=FLEXRANKED",
    },
    "Bi": {
        "puuid": "pW0cJ9DcXsb3uj0xHwXjDsMHGPIbNEBmESbb8P8wsWli5CgmwjgM4TFPHwpd9HvcwXB7z9FzC06NSw",
        "opgg_url": "https://op.gg/lol/summoners/na/White%20Swan-4242?queue_type=FLEXRANKED",
    },
    "Cody": {
        "puuid": "GWB6JRRVtuAKYufv6JSR8uY6w--dfpVM45XcM6-PuRqw-IDkamubG7KurtNTP8_jqeLWePFVmDTKuw",
        "opgg_url": "https://op.gg/lol/summoners/na/Cody-1414?queue_type=FLEXRANKED",
    },
    "Ash": {
        "puuid": "AgW0-64FECbeS4PPlm5dRcKYsbHUFeozTqvhWYyyNcRUXaRmWRG_wZ9LDuhdkxk7sLxNwf6aUuFEEg",
        "opgg_url": "https://op.gg/lol/summoners/na/Ash-uoplw?queue_type=FLEXRANKED",
    },
    "Oqi": {
        "puuid": "I-ZkjIqVkqj64P5pUo5GGC11G2sVwh0ObHmob3MLPYr1ecaAXL5SZ1eZLNfeI4jhgJgp-HA-zAqadA",
        "opgg_url": "https://op.gg/lol/summoners/na/Oqi-NA1?queue_type=FLEXRANKED",
    },
    "Michael": {
        "puuid": "WpJkq7RYZU3FE01ehuu-oQhxL_QIJ3MilpVmVvMn9nIjdiRmXCCCPLabOGy70MS4tIL2Fq5133S_3w",
        "opgg_url": "https://op.gg/lol/summoners/na/Madi%20Hales-NA1?queue_type=FLEXRANKED",
    },
    "Jeff": {
        "puuid": "oGvNLx9Ie3c6WupoSfiIZJfkwTBBgUBvyQk7JzQQrvZOSRUCMOpt5TvXM8oxHQoZZxo2id4iD-MW0g",
        "opgg_url": "https://op.gg/lol/summoners/na/Tacoboy7777-NA1?queue_type=FLEXRANKED",
    },
    "Marcus": {
        "puuid": "lBH-OgFrJw_duwOBue7X_F8G25gdMapoKPyhKihNQOyeqUZqE2N14-IBp6frvIiQ6LHGqGH2uj-XoQ",
        "opgg_url": "https://op.gg/lol/summoners/na/Cylainius-NOXUS?queue_type=FLEXRANKED",
    },
    # "Parker": { ... },
    # etc.
}

DPM_FLEX_PROFILES_OLD = {
    "Blake": {
        "puuid": "mV8WBqdnPXtD_grbs_nXqZXCSMfEJhnb1pW11vJXbJO7p6JqzAfbZgCYcND0DVk0R8l5gDX3AxzMOQ",
        "endpoint": "https://dpm.lol/v1/players/mV8WBqdnPXtD_grbs_nXqZXCSMfEJhnb1pW11vJXbJO7p6JqzAfbZgCYcND0DVk0R8l5gDX3AxzMOQ/match-history?queue=flex",
    },
    "Parky": {
        "puuid": "c1OusXt1PnpBHUcPhYB5tVGxaaJiHtllltrNp_d8PcsXQn-YjhiBuqsziEe6ThzDCtCkebYVV-hIsQ",
        "endpoint": "https://dpm.lol/v1/players/c1OusXt1PnpBHUcPhYB5tVGxaaJiHtllltrNp_d8PcsXQn-YjhiBuqsziEe6ThzDCtCkebYVV-hIsQ/match-history?queue=flex",
    },
    "Josh": {
        "puuid": "UjiUcaCRKmCMYGQ8i_9u1hVzE5GvwloxPz3vG7jR2mUgSXvItkufi4LJXGZ_54lgHB23evgqnlvNJw",
        "endpoint": "https://dpm.lol/v1/players/UjiUcaCRKmCMYGQ8i_9u1hVzE5GvwloxPz3vG7jR2mUgSXvItkufi4LJXGZ_54lgHB23evgqnlvNJw/match-history?queue=flex",
    },
    "Bi": {
        "puuid": "pW0cJ9DcXsb3uj0xHwXjDsMHGPIbNEBmESbb8P8wsWli5CgmwjgM4TFPHwpd9HvcwXB7z9FzC06NSw",
        "endpoint": "https://dpm.lol/v1/players/pW0cJ9DcXsb3uj0xHwXjDsMHGPIbNEBmESbb8P8wsWli5CgmwjgM4TFPHwpd9HvcwXB7z9FzC06NSw/match-history?queue=flex",
    },
    "Cody": {
        "puuid": "GWB6JRRVtuAKYufv6JSR8uY6w--dfpVM45XcM6-PuRqw-IDkamubG7KurtNTP8_jqeLWePFVmDTKuw",
        "endpoint": "https://dpm.lol/v1/players/GWB6JRRVtuAKYufv6JSR8uY6w--dfpVM45XcM6-PuRqw-IDkamubG7KurtNTP8_jqeLWePFVmDTKuw/match-history?queue=flex",
    },
    "Ash": {
        "puuid": "AgW0-64FECbeS4PPlm5dRcKYsbHUFeozTqvhWYyyNcRUXaRmWRG_wZ9LDuhdkxk7sLxNwf6aUuFEEg",
        "endpoint": "https://dpm.lol/v1/players/AgW0-64FECbeS4PPlm5dRcKYsbHUFeozTqvhWYyyNcRUXaRmWRG_wZ9LDuhdkxk7sLxNwf6aUuFEEg/match-history?queue=flex",
    },
    "Oqi": {
        "puuid": "I-ZkjIqVkqj64P5pUo5GGC11G2sVwh0ObHmob3MLPYr1ecaAXL5SZ1eZLNfeI4jhgJgp-HA-zAqadA",
        "endpoint": "https://dpm.lol/v1/players/I-ZkjIqVkqj64P5pUo5GGC11G2sVwh0ObHmob3MLPYr1ecaAXL5SZ1eZLNfeI4jhgJgp-HA-zAqadA/match-history?queue=flex",
    },
    "Michael": {
        "puuid": "WpJkq7RYZU3FE01ehuu-oQhxL_QIJ3MilpVmVvMn9nIjdiRmXCCCPLabOGy70MS4tIL2Fq5133S_3w",
        "endpoint": "https://dpm.lol/v1/players/WpJkq7RYZU3FE01ehuu-oQhxL_QIJ3MilpVmVvMn9nIjdiRmXCCCPLabOGy70MS4tIL2Fq5133S_3w/match-history?queue=flex",
    },
    "Jeff": {
        "puuid": "oGvNLx9Ie3c6WupoSfiIZJfkwTBBgUBvyQk7JzQQrvZOSRUCMOpt5TvXM8oxHQoZZxo2id4iD-MW0g",
        "endpoint": "https://dpm.lol/v1/players/oGvNLx9Ie3c6WupoSfiIZJfkwTBBgUBvyQk7JzQQrvZOSRUCMOpt5TvXM8oxHQoZZxo2id4iD-MW0g/match-history?queue=flex",
    },
    "Marcus": {
        "puuid": "lBH-OgFrJw_duwOBue7X_F8G25gdMapoKPyhKihNQOyeqUZqE2N14-IBp6frvIiQ6LHGqGH2uj-XoQ",
        "endpoint": "https://dpm.lol/v1/players/lBH-OgFrJw_duwOBue7X_F8G25gdMapoKPyhKihNQOyeqUZqE2N14-IBp6frvIiQ6LHGqGH2uj-XoQ/match-history?queue=flex",
    },
    # "Parker": { ... },
    # etc.
}
# ---------- DPM Flex leaderboard config ----------

# Map a short display name -> DPM profile info.
# You fill these in with the correct PUUID and match-history URL.
# NOTE: Append ?queue=flex (or whatever DPM uses) to limit to Flex only.


FLEX_LEADERBOARD_CHANNEL_ID = 753959443263389737
MIN_FLEX_GAMES_PER_WEEK = 5  # only rank players with at least this many games this week
FLEX_QUEUE_ID = 440          # DPM uses 440 for Ranked Flex
MIN_GROUP_PLAYERS_IN_GAME = 5
dpm_latest_matches_by_profile: dict[str, list[dict]] = {}

# ---------- Flask app to receive DPM data from Tampermonkey ----------

flask_app = Flask(__name__)
CORS(flask_app, resources={r"/update_dpm": {"origins": "*"}})


@flask_app.route("/update_dpm", methods=["POST", "OPTIONS"])
def update_dpm():
    """
    Endpoint that Tampermonkey posts to with DPM match-history data.
    Expected JSON shape:

      {
        "profile": "Blake",
        "puuid": "....",
        "matches": [ { gameId, gameCreation, queueId, participants: [...], ... }, ... ],
        "totalCount": 123
      }

    We store matches keyed by `profile`.
    """
    global dpm_latest_matches_by_profile

    if request.method == "OPTIONS":
        # CORS preflight
        return ("", 204)

    data = request.get_json(silent=True) or {}
    profile = data.get("profile") or "UNKNOWN"
    matches = data.get("matches") or []

    # Store latest batch for this profile
    dpm_latest_matches_by_profile[profile] = matches
    print(f"[DPM Relay] Received {len(matches)} matches for profile={profile}")

    return ("", 204)


def _run_flask():
    # Run on localhost:5000 so Tampermonkey can hit it.
    flask_app.run(host="127.0.0.1", port=5000, debug=False, use_reloader=False)


# Start Flask server in a background thread immediately when this module is imported.
flask_thread = Thread(target=_run_flask, daemon=True)
flask_thread.start()
print("[DPM Relay] Flask listener started on http://127.0.0.1:5000/update_dpm")




# ------------- Riot API / League score helpers -------------

RIOT_ROUTING_REGION = "americas"   # cluster for NA match/account endpoints
RIOT_PLATFORM_REGION = "na1"       # kept for future use (summoner-v4 etc.)

# Hard-wired for now: Schmort#bone (NA)
RIOT_GAME_NAME = "Schmort"
RIOT_TAGLINE = "bone"

QUEUE_ID_TO_NAME = {
    420: "Ranked Solo/Duo",
    440: "Ranked Flex 5v5",
    400: "Normal Draft",
    430: "Normal Blind",
    450: "ARAM",
}

ROLE_EXPECTED_DAMAGE_SHARE = {
    "TOP": 0.22,
    "JUNGLE": 0.18,
    "MIDDLE": 0.24,
    "BOTTOM": 0.28,
    "UTILITY": 0.08,
}

ROLE_EXPECTED_CS_PM = {
    "TOP": 6.5,
    "JUNGLE": 5.5,
    "MIDDLE": 7.0,
    "BOTTOM": 7.5,
    "UTILITY": 1.5,
}

EXPECTED_KP = 0.55  # rough "good" kill participation baseline


_ignore_state = {"ignored": [], "cooldowns": {}}  # {"ignored":[str(user_id),...], "cooldowns": {str(user_id): last_toggle_epoch}}
_spam_state = {"punished_until": {}, "recent": {}}  # punished_until: {str(user_id): epoch}, recent: {str(user_id): [epochs]}

JOSH_RIOT_GAME_NAME = "DrkCloak"
JOSH_RIOT_TAG_LINE = "NA1"
JOSH_ACCOUNT_ROUTING = "americas"  # for NA riot-id lookups

async def _get_puuid_by_riot_id(session: aiohttp.ClientSession, api_key: str, game_name: str, tag_line: str) -> str:
    url = (
        f"https://{JOSH_ACCOUNT_ROUTING}.api.riotgames.com"
        f"/riot/account/v1/accounts/by-riot-id/{quote(game_name)}/{quote(tag_line)}"
    )
    data = await _riot_get_json(session, url, api_key)
    puuid = data.get("puuid")
    if not puuid:
        raise RuntimeError(f"[JoshLP] account-v1 missing puuid: {data}")
    return puuid

def _load_josh_soloq_state() -> dict:
    # shape: {"version": 1, "last_lp": int|None, "last_tier": str|None}
    base = {"version": 1, "last_lp": None, "last_tier": None}
    try:
        if not os.path.exists(JOSH_SOLOQ_STATE_FILE):
            return base
        with open(JOSH_SOLOQ_STATE_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
        base["last_lp"] = raw.get("last_lp")
        base["last_tier"] = raw.get("last_tier")
        return base
    except Exception as e:
        print(f"[JoshLP] Error loading state: {e}")
        return base


def _save_josh_soloq_state(last_lp: int | None, last_tier: str | None) -> None:
    try:
        payload = {"version": 1, "last_lp": last_lp, "last_tier": last_tier}
        with open(JOSH_SOLOQ_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
    except Exception as e:
        print(f"[JoshLP] Error saving state: {e}")


async def _get_josh_soloq_lp_and_tier(session: aiohttp.ClientSession, api_key: str) -> tuple[int | None, str | None]:
    """
    Returns (lp, tier) for RANKED_SOLO_5x5, or (None, None) if no SoloQ entry.
    """
    puuid = await _get_puuid_by_riot_id(session, api_key, JOSH_RIOT_GAME_NAME, JOSH_RIOT_TAG_LINE)

    # IMPORTANT: quote the puuid to avoid URL issues
    url = f"https://{JOSH_LOL_PLATFORM}.api.riotgames.com/lol/league/v4/entries/by-puuid/{quote(puuid, safe='')}"
    entries = await _riot_get_json(session, url, api_key)

    if not isinstance(entries, list):
        raise RuntimeError(f"[JoshLP] Unexpected league response: {entries}")

    for e in entries:
        if isinstance(e, dict) and e.get("queueType") == "RANKED_SOLO_5x5":
            lp = e.get("leaguePoints")
            tier = e.get("tier")
            try:
                lp = int(lp)
            except Exception:
                lp = None
            tier = str(tier).upper() if tier else None
            return lp, tier

    return None, None



def _format_josh_nick(lp: int | None, tier: str | None) -> str:
    # Exact format requested: "JOSH X/570 LP MASTER"
    # If unranked/unknown, keep something sensible
    if lp is None or tier is None:
        return "JOSH —/580 LP UNRANKED"
    return f"JOSH {lp}/580 LP {tier}"


def _now_epoch() -> float:
    return time.time()  # (already defined above; leave your original)

async def load_disable_state():
    global _disable_state
    if os.path.exists(DISABLE_FILE):
        try:
            async with aiofiles.open(DISABLE_FILE, 'r') as f:
                _disable_state = json.loads(await f.read()) or {"until": 0}
        except Exception:
            _disable_state = {"until": 0}
    else:
        _disable_state = {"until": 0}

async def save_disable_state():
    async with aiofiles.open(DISABLE_FILE, 'w') as f:
        await f.write(json.dumps(_disable_state))

def is_globally_disabled() -> bool:
    return _now_epoch() < _disable_state.get("until", 0)

def disable_remaining_seconds() -> int:
    return max(0, int(_disable_state.get("until", 0) - _now_epoch()))

async def set_disabled_for(seconds: int):
    _disable_state["until"] = int(_now_epoch() + max(0, seconds))
    await save_disable_state()

async def clear_disabled():
    _disable_state["until"] = 0
    await save_disable_state()

def _parse_duration_to_seconds(s: str | None) -> int:
    """Accepts '1h', '2hr', '30m', '90min'. Defaults to 1h on bad input."""
    if not s:
        return 3600
    s = s.strip().lower()
    m = re.match(r"^\s*(\d+)\s*(h|hr|hrs|hour|hours|m|min|mins|minute|minutes)?\s*$", s)
    if not m:
        return 3600
    qty = int(m.group(1))
    unit = (m.group(2) or "h").lower()
    if unit.startswith('h'):
        return qty * 3600
    return qty * 60

def _now_epoch() -> float:
    return time.time()

async def load_ignore_and_spam_state():
    global _ignore_state, _spam_state
    # ignore
    if os.path.exists(IGNORE_FILE):
        async with aiofiles.open(IGNORE_FILE, 'r') as f:
            try:
                _ignore_state = json.loads(await f.read()) or {"ignored": [], "cooldowns": {}}
            except Exception:
                _ignore_state = {"ignored": [], "cooldowns": {}}
    else:
        _ignore_state = {"ignored": [], "cooldowns": {}}
    # spam
    if os.path.exists(SPAM_FILE):
        async with aiofiles.open(SPAM_FILE, 'r') as f:
            try:
                _spam_state = json.loads(await f.read()) or {"punished_until": {}, "recent": {}}
            except Exception:
                _spam_state = {"punished_until": {}, "recent": {}}
    else:
        _spam_state = {"punished_until": {}, "recent": {}}

async def save_ignore_state():
    async with aiofiles.open(IGNORE_FILE, 'w') as f:
        await f.write(json.dumps(_ignore_state))

async def save_spam_state():
    async with aiofiles.open(SPAM_FILE, 'w') as f:
        await f.write(json.dumps(_spam_state))

def is_ignored(user_id: int) -> bool:
    return str(user_id) in _ignore_state.get("ignored", [])

def ignore_cooldown_remaining(user_id: int) -> float:
    last = _ignore_state.get("cooldowns", {}).get(str(user_id), 0)
    elapsed = _now_epoch() - last
    cooldown = 24 * 3600
    return max(0.0, cooldown - elapsed)

async def set_ignored(user_id: int, value: bool):
    uid = str(user_id)
    ignored = set(_ignore_state.get("ignored", []))
    if value:
        ignored.add(uid)
    else:
        if uid in ignored:
            ignored.remove(uid)
    _ignore_state["ignored"] = list(ignored)
    if "cooldowns" not in _ignore_state:
        _ignore_state["cooldowns"] = {}
    _ignore_state["cooldowns"][uid] = _now_epoch()
    await save_ignore_state()

def is_punished(user_id: int) -> bool:
    until = _spam_state.get("punished_until", {}).get(str(user_id), 0)
    return _now_epoch() < until

async def record_ask_and_check_punish(user_id: int) -> bool:
    """
    Returns True if the user is currently punished (so you should react instead of replying),
    otherwise False. Also updates punishment if they hit the spam threshold.
    """
    uid = str(user_id)
    now = _now_epoch()

    # If already punished, keep it (same behavior as before)
    if is_punished(user_id):
        return True

    # Track per-user recent "ask" timestamps (mentions, replies, /ask, !!ask)
    recent = _spam_state.setdefault("recent", {}).setdefault(uid, [])

    # Record this ask
    recent.append(now)

    # Prune to the longest window we care about so the list stays small
    max_window = max(w for _, w in SPAM_WINDOWS)
    cutoff = now - max_window
    recent[:] = [t for t in recent if t >= cutoff]

    # Trip on the first window that’s met or exceeded
    for limit, window in SPAM_WINDOWS:
        hits = [t for t in recent if t >= now - window]
        if len(hits) >= limit:                     # note: >= so "exactly 5" triggers
            _spam_state.setdefault("punished_until", {})[uid] = now + 60 * 60 * 24  # keep your 1h punish
            await save_spam_state()
            return True

    # Not punished
    await save_spam_state()
    return False

def _canonical_name(user_id: int) -> str:
    """Map a Discord user id to a readable canonical name, falling back to 'User-<id>'."""
    names = USER_ID_MAPPING.get(user_id)
    if names and len(names) > 0:
        return names[0]
    return f"User-{user_id}"

def normalize_mentions_raw(text: str) -> str:
    """
    Replace any raw <@123> or <@!123> mentions with @Name using USER_ID_MAPPING.
    This prevents GPT from seeing ID mentions and from thinking @Jeff Bot means 'talk about yourself'.
    """
    if not text:
        return text

    def _repl(m):
        uid = int(m.group("id"))
        return "@" + _canonical_name(uid)

    # <@123> or <@!123>
    text = re.sub(r"<@!? (?P<id>\d+) >".replace(" ", ""), _repl, text)
    return text

def normalize_visible_ats(text: str) -> str:
    text = re.sub(r"(?s)BEGIN_[A-Z0-9_]+.*?END_[A-Z0-9_]+\s*", "", text).strip()
    # 2) If the model starts like "Jeff Bot :: ..." or "[Name] :: ..."
    text = re.sub(r"^\s*(?:Jeff\s*Bot|[\[\(]?[^\]\):]+[\]\)]?)\s*::\s*", "", text, flags=re.IGNORECASE)
    return text


async def _riot_get_json(session: aiohttp.ClientSession, url: str, api_key: str) -> dict:
    """Tiny helper that does a Riot GET with basic error handling."""
    headers = {"X-Riot-Token": api_key}
    async with session.get(url, headers=headers) as resp:
        if resp.status != 200:
            text = await resp.text()
            raise RuntimeError(f"Riot API error {resp.status} for {url}: {text[:200]}")
        return await resp.json()
    
def _detroit_day_str_from_utc(dt_utc: datetime) -> str:
    # dt_utc should be aware UTC
    local = dt_utc.astimezone(DETROIT_TZ)
    return local.date().isoformat()

def _load_flex_rank_snapshots() -> dict:
    """
    Shape:
      { "version": 1, "snapshots": [ { "day": "YYYY-MM-DD", "ts": "ISO_UTC", "ranks": [names...] }, ... ] }
    """
    base = {"version": 1, "snapshots": []}
    if not os.path.exists(FLEX_RANK_SNAPSHOTS_FILE):
        return base
    try:
        with open(FLEX_RANK_SNAPSHOTS_FILE, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
        snaps = raw.get("snapshots") or []
        if isinstance(snaps, list):
            base["snapshots"] = snaps
        return base
    except Exception as e:
        print(f"[FlexLB] Error loading rank snapshots: {e}")
        return base

def _save_flex_rank_snapshots(data: dict) -> None:
    try:
        with open(FLEX_RANK_SNAPSHOTS_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[FlexLB] Error saving rank snapshots: {e}")

def _get_latest_snapshot_for_day(day_str: str) -> list[str] | None:
    data = _load_flex_rank_snapshots()
    best_ts = None
    best_ranks = None
    for s in data.get("snapshots", []) or []:
        if not isinstance(s, dict):
            continue
        if s.get("day") != day_str:
            continue
        ts = s.get("ts")
        ranks = s.get("ranks")
        if not isinstance(ts, str) or not isinstance(ranks, list):
            continue
        # ISO UTC strings sort correctly lexicographically if they’re real ISO datetimes
        if best_ts is None or ts > best_ts:
            best_ts = ts
            best_ranks = [str(x) for x in ranks]
    return best_ranks

def _upsert_latest_snapshot_for_today(entries: list[dict], now_utc: datetime) -> None:
    """
    Stores ONE snapshot per Detroit day: the latest call that day wins.
    """
    day_str = _detroit_day_str_from_utc(now_utc)
    ts = now_utc.astimezone(timezone.utc).isoformat()

    ranks = [e["name"] for e in entries]  # already sorted best->worst
    data = _load_flex_rank_snapshots()

    # Remove existing snapshot(s) for this day; we keep only the latest for that day.
    snaps = [s for s in (data.get("snapshots") or []) if not (isinstance(s, dict) and s.get("day") == day_str)]
    snaps.append({"day": day_str, "ts": ts, "ranks": ranks})

    # Optional pruning: keep last 45 days only (prevents file growth)
    cutoff_day = (now_utc.astimezone(DETROIT_TZ).date() - timedelta(days=45)).isoformat()
    pruned = []
    for s in snaps:
        if isinstance(s, dict) and isinstance(s.get("day"), str) and s["day"] >= cutoff_day:
            pruned.append(s)

    data["snapshots"] = pruned
    _save_flex_rank_snapshots(data)



# -------------------------------
# Lenny counter persistence
# -------------------------------
LENNY_TARGET_USER_ID = 184481785172721665
LENNY_CHANNEL_ID = 753959443263389737  # optional filter (you can remove if you want all channels)

LENNY_STATS_FILE = os.path.join(SCRIPT_DIR, "lenny_stats.json")
_lenny_stats = {"total": 0, "by_day": {}}  # day is YYYY-MM-DD (America/Detroit)
_lenny_lock = asyncio.Lock()

# whole-word match for "lenny" or "lennert" (any caps)
_LENNY_RE = re.compile(r"\b(lenny|lennert)\b", re.IGNORECASE)

async def load_lenny_stats():
    global _lenny_stats
    if os.path.exists(LENNY_STATS_FILE):
        try:
            async with aiofiles.open(LENNY_STATS_FILE, "r") as f:
                _lenny_stats = json.loads(await f.read()) or {"total": 0, "by_day": {}}
        except Exception:
            _lenny_stats = {"total": 0, "by_day": {}}
    else:
        _lenny_stats = {"total": 0, "by_day": {}}

async def save_lenny_stats():
    async with aiofiles.open(LENNY_STATS_FILE, "w") as f:
        await f.write(json.dumps(_lenny_stats, indent=2))

def _detroit_day_str(dt_aware_utc: datetime) -> str:
    # message.created_at is aware UTC in discord.py
    local = dt_aware_utc.astimezone(DETROIT_TZ)
    return local.date().isoformat()

async def record_lenny_if_needed(message: discord.Message):
    # Only count target user
    if message.author.id != LENNY_TARGET_USER_ID:
        return
    # Optional: only count in one channel
    if LENNY_CHANNEL_ID and message.channel.id != LENNY_CHANNEL_ID:
        return
    content = message.content or ""
    if not content:
        return
    hits = len(_LENNY_RE.findall(content))
    if hits <= 0:
        return
    day = _detroit_day_str(message.created_at)

    async with _lenny_lock:
        _lenny_stats["total"] = int(_lenny_stats.get("total", 0)) + hits
        by_day = _lenny_stats.setdefault("by_day", {})
        by_day[day] = int(by_day.get(day, 0)) + hits
        await save_lenny_stats()


# ---------- DPM Flex Weekly Leaderboard Helpers (using local Tampermonkey data) ----------

def _snapshot_dpm_matches() -> dict[str, list[dict]]:
    """
    Take a shallow snapshot of the global matches dict so we don't
    get weirdness if Flask writes while we're iterating.
    """
    global dpm_latest_matches_by_profile
    snap: dict[str, list[dict]] = {}
    for k, v in dpm_latest_matches_by_profile.items():
        snap[k] = list(v)  # shallow copy of list; match dicts reused
    return snap

def get_last_completed_week_window():
    """
    Returns (week_start_utc, week_end_utc, display_start_date, display_end_date)

    Week is defined as:
      - Start: previous Monday 00:00 local
      - End: this Monday 00:00 local
    So it covers Monday-Sunday, and the 'Sunday night at midnight' run
    is exactly at week_end.
    """
    now_local = datetime.now(LOCAL_TIMEZONE)

    today = now_local.date()
    # Monday = 0 ... Sunday = 6
    days_since_monday = today.weekday()

    # This Monday 00:00
    this_monday_local = datetime.combine(
        today - timedelta(days=days_since_monday),
        dtime(0, 0),
        tzinfo=LOCAL_TIMEZONE,
    )

    # If we're somehow before Monday 00:00 (we shouldn't be), go one week back
    if now_local < this_monday_local:
        this_monday_local -= timedelta(days=7)

    week_end_local = this_monday_local
    week_start_local = week_end_local - timedelta(days=7)

    # Convert to UTC for comparing to gameCreation timestamps
    week_start_utc = week_start_local.astimezone(timezone.utc)
    week_end_utc = week_end_local.astimezone(timezone.utc)

    # For display, we show start_date → end_date - 1 day (Mon–Sun)
    display_start_date = week_start_local.date()
    display_end_date = (week_end_local - timedelta(days=1)).date()

    return week_start_utc, week_end_utc, display_start_date, display_end_date


def load_flex_leaderboard_history() -> dict:
    """
    Returns a dict mapping week_key -> list of entries:
      { "YYYY-MM-DD": [ { "name": str, "games": int, "avg": float }, ... ], ... }
    """
    if not os.path.exists(FLEX_LEADERBOARD_HISTORY_FILE):
        return {}
    try:
        with open(FLEX_LEADERBOARD_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_flex_leaderboard_history(history: dict) -> None:
    try:
        with open(FLEX_LEADERBOARD_HISTORY_FILE, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)
    except Exception as e:
        print(f"[FlexLB] Error saving history: {e}")


async def compute_weekly_flex_leaderboard_from_local() -> tuple[list[dict], datetime.date, datetime.date]:
    """
    Compute the weekly Flex leaderboard using the data that the browser
    (via Tampermonkey) has pushed into dpm_latest_matches_by_profile.

    Week window comes from get_last_completed_week_window().

    A game counts for a player if:
      - queueId == FLEX_QUEUE_ID (Flex)
      - gameCreation is within the week window
      - that game's gameId appears for at least MIN_GROUP_PLAYERS_IN_GAME profiles
        in DPM_FLEX_PROFILES (i.e., ≥4 of your tracked accounts played that game).
    """
    if not DPM_FLEX_PROFILES:
        week_start_utc, week_end_utc, display_start, display_end = get_last_completed_week_window()
        return [], display_start, display_end

    week_start_utc, week_end_utc, display_start, display_end = get_last_completed_week_window()
    week_start_ts = week_start_utc.timestamp()
    week_end_ts = week_end_utc.timestamp()

    snap = _snapshot_dpm_matches()  # { profile_name: [matches...] }

    # 1) Build gameId -> set of tracked profile names who have that game in this week
    game_to_profiles: dict[int, set[str]] = {}

    for name, matches in snap.items():
        if name not in DPM_FLEX_PROFILES:
            continue

        for m in matches or []:
            if m.get("queueId") != FLEX_QUEUE_ID:
                continue

            gc_ms = m.get("gameCreation") or 0
            gc_ts = gc_ms / 1000.0
            if not (week_start_ts <= gc_ts < week_end_ts):
                continue

            game_id = m.get("gameId")
            if game_id is None:
                continue

            game_to_profiles.setdefault(game_id, set()).add(name)

    # 2) For each player, collect DPM scores only from games where
    #    ≥ MIN_GROUP_PLAYERS_IN_GAME tracked profiles share that gameId.
    entries: list[dict] = []

    for name in DPM_FLEX_PROFILES.keys():
        matches = snap.get(name, []) or []
        scores: list[float] = []

        for m in matches:
            if m.get("queueId") != FLEX_QUEUE_ID:
                continue

            gc_ms = m.get("gameCreation") or 0
            gc_ts = gc_ms / 1000.0
            if not (week_start_ts <= gc_ts < week_end_ts):
                continue

            game_id = m.get("gameId")
            if game_id is None:
                continue

            profiles_in_game = game_to_profiles.get(game_id, set())
            if len(profiles_in_game) < MIN_GROUP_PLAYERS_IN_GAME:
                continue

            # This match is a valid "group flex" game for this player.
            # match-history gives this player's stats at participants[0].
            part_list = m.get("participants") or []
            if not part_list:
                continue
            dpm_score = part_list[0].get("dpmScore")

            if isinstance(dpm_score, (int, float)):
                scores.append(float(dpm_score))

        if len(scores) >= MIN_FLEX_GAMES_PER_WEEK:
            avg = sum(scores) / len(scores) if scores else 0.0
            entries.append(
                {
                    "name": name,
                    "games": len(scores),
                    "avg": avg,
                }
            )

    # 3) Sort best → worst
    entries.sort(key=lambda e: e["avg"], reverse=True)

    # 4) Attach deltas vs last week and save history
    history = load_flex_leaderboard_history()
    this_week_key = display_start.isoformat()  # e.g. "2025-12-01"

    # Find latest prior week key < this_week_key
    prev_key = None
    for k in sorted(history.keys()):
        if k < this_week_key:
            prev_key = k
    prev_entries = history.get(prev_key, []) if prev_key else []
    prev_positions = {e["name"]: idx + 1 for idx, e in enumerate(prev_entries)}

    for idx, e in enumerate(entries):
        new_pos = idx + 1
        old_pos = prev_positions.get(e["name"])
        if old_pos is None:
            e["delta"] = None
        else:
            e["delta"] = old_pos - new_pos  # positive = moved up

    # Save this week's ranking for next comparison
    history[this_week_key] = [
        {"name": e["name"], "games": e["games"], "avg": e["avg"]} for e in entries
    ]
    save_flex_leaderboard_history(history)

    return entries, display_start, display_end


async def compute_recent_flex_leaderboard_from_local(hours: int = 18) -> tuple[list[dict], datetime, datetime]:
    """
    Compute a temporary Flex leaderboard over the last `hours` (default 18h).

    A game counts for a player if:
      - queueId == FLEX_QUEUE_ID (Flex)
      - gameCreation is within [now - hours, now]
      - that game's gameId appears for at least MIN_GROUP_PLAYERS_IN_GAME profiles
        in DPM_FLEX_PROFILES (i.e., ≥4 of your tracked accounts played that game).

    Unlike the weekly leaderboard:
      - There is NO minimum games requirement (as long as player has ≥1 qualifying game).
      - No history or delta is saved/used.
    """
    if not DPM_FLEX_PROFILES:
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc - timedelta(hours=hours)
        return [], start_utc, now_utc

    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=hours)
    start_ts = start_utc.timestamp()
    end_ts = now_utc.timestamp()

    snap = _snapshot_dpm_matches()  # { profile_name: [matches...] }

    # 1) Build gameId -> set of tracked profile names who have that game in this window
    game_to_profiles: dict[int, set[str]] = {}

    for name, matches in snap.items():
        if name not in DPM_FLEX_PROFILES:
            continue

        for m in matches or []:
            if m.get("queueId") != FLEX_QUEUE_ID:
                continue

            gc_ms = m.get("gameCreation") or 0
            gc_ts = gc_ms / 1000.0
            if not (start_ts <= gc_ts <= end_ts):
                continue

            game_id = m.get("gameId")
            if game_id is None:
                continue

            game_to_profiles.setdefault(game_id, set()).add(name)

    # 2) Per-player scores from valid group games (≥4 tracked profiles per gameId)
    entries: list[dict] = []

    for name in DPM_FLEX_PROFILES.keys():
        matches = snap.get(name, []) or []
        scores: list[float] = []

        for m in matches:
            if m.get("queueId") != FLEX_QUEUE_ID:
                continue

            gc_ms = m.get("gameCreation") or 0
            gc_ts = gc_ms / 1000.0
            if not (start_ts <= gc_ts <= end_ts):
                continue

            game_id = m.get("gameId")
            if game_id is None:
                continue

            profiles_in_game = game_to_profiles.get(game_id, set())
            if len(profiles_in_game) < MIN_GROUP_PLAYERS_IN_GAME:
                continue

            part_list = m.get("participants") or []
            if not part_list:
                continue
            dpm_score = part_list[0].get("dpmScore")

            if isinstance(dpm_score, (int, float)):
                scores.append(float(dpm_score))

        # 🔥 NO minimum game requirement beyond at least 1 qualifying game
        if not scores:
            continue

        avg = sum(scores) / len(scores)
        entries.append(
            {
                "name": name,
                "games": len(scores),
                "avg": avg,
            }
        )

    entries.sort(key=lambda e: e["avg"], reverse=True)
    return entries, start_utc, now_utc

def format_recent_flex_leaderboard(entries: list[dict], start_utc: datetime, end_utc: datetime) -> str:
    MEDALS = ["🥇", "🥈", "🥉"]

    # Convert to local time for display
    start_local = start_utc.astimezone(LOCAL_TIMEZONE)
    end_local = end_utc.astimezone(LOCAL_TIMEZONE)


    header_end = end_local.strftime("%m-%d")

    if not entries:
        return (
            f"**⏱️ LAST FLEX SESSION LEADERBOARD** ({header_end})\n"
            "```text\n"
            "No qualifying Flex games in the last 18 hours.\n"
            f"- Still require ≥{MIN_GROUP_PLAYERS_IN_GAME}/5 tracked members in the same game\n"
            "```"
        )

    # Precompute string columns
    rank_col = [str(i) for i in range(1, len(entries) + 1)]
    name_col = [e["name"] for e in entries]
    score_col = [f"{e['avg']:.2f}" for e in entries]
    kda_col = ["--" if e.get("avg_kda") is None else f"{float(e['avg_kda']):.2f}" for e in entries]
    lane_col = ["--" if e.get("avg_lane") is None else f"{float(e['avg_lane']):.2f}" for e in entries]
    games_col = [str(e["games"]) for e in entries]

    # Column widths
    rank_w = max(len("RK"), max(len(r) for r in rank_col))
    name_w = max(len("Player"), max(len(n) for n in name_col))
    score_w = max(len("Score"), max(len(s) for s in score_col))
    kda_w = max(len("KDA"), max(len(s) for s in kda_col))
    lane_w = max(len("Lane"), max(len(s) for s in lane_col))
    games_w = max(len("Games"), max(len(g) for g in games_col))

    lines: list[str] = []
    lines.append(f"**⏱️ LAST FLEX SESSION LEADERBOARD** ({header_end})")
    lines.append("```text")

    # Header
    header = (
        f"{'RK'.rjust(rank_w)}  "
        f"{'Player'.ljust(name_w)}   "
        f"{'Score'.rjust(score_w)} "
        f"{'KDA'.rjust(kda_w)}  "
        f"{'Lane'.rjust(lane_w)}    "
        f"{'Games'.rjust(games_w)}  "
    )
    lines.append(header)
    lines.append("-" * (len(header) + 1))

    # Rows (with medal swap for 1/2/3, same trick as your weekly)
    for idx, (r, n, s, k, l, g) in enumerate(zip(rank_col, name_col, score_col, kda_col, lane_col, games_col), start=1):
        # Replace ranks 1,2,3 with medal emoji (no padding)
        if idx == 1:
            rk_field = "🥇"
        elif idx == 2:
            rk_field = "🥈"
        elif idx == 3:
            rk_field = "🥉"
        else:
            rk_field = r.rjust(rank_w)

        line = (
            f"{rk_field}  "
            f"{n.ljust(name_w)}  "
            f"{s.rjust(score_w)}  "
            f"{k.rjust(kda_w)}  "
            f"{l.rjust(lane_w)}  "
            f"{g.rjust(games_w)}"
        )
        lines.append(line)

    lines.append("```")
    return "\n".join(lines)

def _parse_opgg_rsc_payload(text: str):
    """
    Returns either:
      - dict (usually with key "data"), OR
      - list (sometimes the parsed value *is* the list of matches)
    """
    for line in (text or "").splitlines():
        if line.startswith("1:"):
            return json.loads(line[2:].strip())
    raise ValueError("No '1:' JSON line found in OP.GG RSC payload")



def _find_me_in_opgg_match(match: dict) -> dict | None:
    """Find the participant dict corresponding to match['puuid']."""
    puuid = match.get("puuid")
    if not isinstance(puuid, str) or not puuid:
        return None

    for team_key in ("team_blue", "team_red"):
        team = match.get(team_key)
        if not isinstance(team, list):
            continue
        for p in team:
            if not isinstance(p, dict):
                continue
            summ = p.get("summoner")
            if isinstance(summ, dict) and summ.get("puuid") == puuid:
                return p
    return None


def _opgg_try_float(v) -> float | None:
    """Best-effort: coerce OP.GG stat values into a float."""
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        # common formats: "3.25", "3.25:1", "3.25%" (we strip non-numeric suffixes)
        # Keep leading sign, digits, dot.
        m = re.match(r"^\s*([+-]?[0-9]*\.?[0-9]+)", s)
        if not m:
            return None
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def _opgg_get_stat_float(stats: dict, keys: list[str]) -> float | None:
    """Try multiple keys and return the first parseable float."""
    if not isinstance(stats, dict):
        return None
    for k in keys:
        if k in stats:
            v = _opgg_try_float(stats.get(k))
            if v is not None:
                return v
    return None


def _opgg_compute_kda_ratio_from_stats(stats: dict) -> float | None:
    """Fallback KDA ratio from kills/deaths/assists if OP.GG doesn't provide kda_ratio."""
    if not isinstance(stats, dict):
        return None

    # OP.GG uses singular keys: kill/death/assist (keep plural fallbacks too)
    kills = _opgg_try_float(stats.get("kills") if stats.get("kills") is not None else stats.get("kill"))
    deaths = _opgg_try_float(stats.get("deaths") if stats.get("deaths") is not None else stats.get("death"))
    assists = _opgg_try_float(stats.get("assists") if stats.get("assists") is not None else stats.get("assist"))

    if kills is None or deaths is None or assists is None:
        return None

    return (kills + assists) / max(1.0, deaths)






async def _fetch_opgg_flex_matches_from_url(context, opgg_url: str) -> list[dict]:
    """
    Returns: [{
        "match_id": str,
        "created_at": datetime|None,
        "op_score": float|None,
        "kda_ratio": float|None,
        "laning_score": float|None,
    }, ...]
    Grabs the first *large* RSC payload and returns immediately (no long scroll loops).
    """
    page = await context.new_page()

    best_body: str | None = None
    got_payload = asyncio.Event()

    async def on_response(resp):
        nonlocal best_body
        try:
            if "op.gg" not in (resp.url or ""):
                return
            ctype = (resp.headers.get("content-type") or "").lower()
            if "text/x-component" not in ctype:
                return
            body = await resp.text()
            if not body:
                return
            # Your log spam is fine; keep it if you want:
            print(f"[OPGG] RSC resp {resp.status} size={len(body)} url={resp.url}")

            # Heuristic: the real payload is large and contains a "1:" JSON line
            # (the tiny 53/215/693 ones are fragments)
            if len(body) > 50_000 and "\n1:" in body:
                # keep the biggest one we’ve seen
                if best_body is None or len(body) > len(best_body):
                    best_body = body
                got_payload.set()
        except Exception:
            pass

    page.on("response", on_response)

    try:
        cache_bust = int(time.time())
        sep = "&" if "?" in opgg_url else "?"
        url = f"{opgg_url}{sep}t={cache_bust}"
        await page.goto(url, wait_until="domcontentloaded", timeout=60_000)

        # Give the page time to kick off its RSC requests.
        # OP.GG can be slow / bursty; a slightly longer settle helps a lot.
        try:
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            # networkidle isn't always achievable on SPAs; that's fine.
            pass

        await page.wait_for_timeout(1500)

        # Wait up to 25s for the big payload
        await asyncio.wait_for(got_payload.wait(), timeout=40)

    except Exception as e:
        # If we never got a big payload, bail cleanly
        # (This prevents your gather wrapper from swallowing everything.)
        return []
    finally:
        try:
            await page.close()
        except Exception:
            pass

    if not best_body:
        return []

    # Parse the biggest captured payload
    payload = _parse_opgg_rsc_payload(best_body)

    # OP.GG sometimes returns dict {"data":[...]} and sometimes returns the list directly.
    if isinstance(payload, dict):
        data = payload.get("data")
    elif isinstance(payload, list):
        data = payload
    else:
        return []

    if not isinstance(data, list):
        return []


    out: list[dict] = []
    for m in data:
        if not isinstance(m, dict):
            continue

        match_id = m.get("id")
        created_at_s = m.get("created_at")

        if not isinstance(match_id, str) or not match_id:
            continue

        created_dt = None
        if isinstance(created_at_s, str) and created_at_s and not created_at_s.startswith("$"):
            try:
                created_dt = datetime.fromisoformat(created_at_s)
            except Exception:
                created_dt = None

        me = _find_me_in_opgg_match(m)
        op_score = None
        kda_ratio = None
        laning_score = None
        if isinstance(me, dict):
            stats = me.get("stats")
            #if isinstance(stats, dict):
            #    print(
            #        "[OPGG] sample K/D/A:",
            #        stats.get("kill"), stats.get("death"), stats.get("assist"),
            #        "(plural fallback):",
            #        stats.get("kills"), stats.get("deaths"), stats.get("assists")
            #    )


            if isinstance(stats, dict):
                op_score = _opgg_get_stat_float(stats, ["op_score", "opScore", "opscore"])

                # OP.GG sometimes uses slightly different keys; try a few.
                kda_ratio = _opgg_get_stat_float(stats, ["kda_ratio", "kdaRatio", "kda"])

                # OP.GG does not provide KDA for flex → compute it ourselves
                if kda_ratio is None:
                    kda_ratio = _opgg_compute_kda_ratio_from_stats(stats)

                # "laning" / "lane" score is exposed alongside op_score in the same stats dict.
                laning_score = _opgg_get_stat_float(
                    stats,
                    [
                        "laning_phase_score",
                        "laningPhaseScore",
                        "laning_score",
                        "laningScore",
                        "lane_score",
                        "laneScore",
                        "lane_phase_score",
                        "lanePhaseScore",
                    ],
                )

        out.append(
            {
                "match_id": match_id,
                "created_at": created_dt,
                "op_score": op_score,
                "kda_ratio": kda_ratio,
                "laning_score": laning_score,
            }
        )

    return out



# =========================
# OP.GG Flex persistent cache
# =========================

FLEX_OPGG_CACHE_FILE = os.path.join(os.path.dirname(__file__), "opgg_flex_cache.json")

_OPGG_FLEX_CACHE: dict | None = None
_OPGG_FLEX_CACHE_LAST_REFRESH_UTC: datetime | None = None

# Timezone helpers (keep internal math in UTC; interpret naive datetimes as America/Detroit for safety)
def _get_detroit_tz():
    """Return a tzinfo for America/Detroit.

    On some Windows installs, Python's zoneinfo database is unavailable unless the third-party
    'tzdata' package is installed. We try ZoneInfo first, then fall back to dateutil (if present),
    and finally to a fixed -05:00 offset (no DST awareness) as a last resort.
    """
    try:
        return ZoneInfo("America/Detroit")
    except Exception:
        try:
            from dateutil import tz as dateutil_tz  # type: ignore
            tzinfo = dateutil_tz.gettz("America/Detroit")
            if tzinfo is not None:
                return tzinfo
        except Exception:
            pass
        return timezone(timedelta(hours=-5))

DETROIT_TZ = _get_detroit_tz()

# =========================
# Persistent Flex leaderboard messages (1 message per guild, edited in-place)
# =========================

_FLEX_PERSISTENT_STATE: dict | None = None


def _load_flex_persistent_state() -> dict:
    """Load persistent message pointers from disk (best effort)."""
    global _FLEX_PERSISTENT_STATE
    if _FLEX_PERSISTENT_STATE is not None:
        return _FLEX_PERSISTENT_STATE

    base = {"weekly": {}, "session": {}}
    try:
        if os.path.exists(FLEX_PERSISTENT_MESSAGES_FILE):
            with open(FLEX_PERSISTENT_MESSAGES_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
            if isinstance(raw.get("weekly"), dict):
                base["weekly"] = raw.get("weekly")
            if isinstance(raw.get("session"), dict):
                base["session"] = raw.get("session")
    except Exception as e:
        print(f"[FlexPersist] failed to load: {type(e).__name__}: {e}")

    _FLEX_PERSISTENT_STATE = base
    return _FLEX_PERSISTENT_STATE


def _save_flex_persistent_state(state: dict) -> None:
    """Persist persistent message pointers to disk (best effort)."""
    global _FLEX_PERSISTENT_STATE
    _FLEX_PERSISTENT_STATE = state
    try:
        tmp_path = FLEX_PERSISTENT_MESSAGES_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)
        os.replace(tmp_path, FLEX_PERSISTENT_MESSAGES_FILE)
    except Exception as e:
        print(f"[FlexPersist] failed to save: {type(e).__name__}: {e}")


def _format_last_updated_header(now_utc: datetime) -> str:
    now_detroit = now_utc.astimezone(DETROIT_TZ)
    return f"**Last updated:** {now_detroit.strftime('%m-%d %I:%M %p')}"


def _get_persistent_pointer(kind: str, guild_id: int) -> dict | None:
    state = _load_flex_persistent_state()
    bucket = state.get(kind) or {}
    ptr = bucket.get(str(guild_id))
    return ptr if isinstance(ptr, dict) else None


def _set_persistent_pointer(kind: str, guild_id: int, *, channel_id: int, message_id: int) -> None:
    state = _load_flex_persistent_state()
    bucket = state.setdefault(kind, {})
    bucket[str(guild_id)] = {"channel_id": int(channel_id), "message_id": int(message_id)}
    _save_flex_persistent_state(state)


def _delete_persistent_pointer(kind: str, guild_id: int) -> None:
    state = _load_flex_persistent_state()
    bucket = state.get(kind) or {}
    bucket.pop(str(guild_id), None)
    _save_flex_persistent_state(state)


async def _upsert_persistent_message(
    *,
    bot: commands.Bot,
    kind: str,
    guild_id: int,
    channel_id: int,
    content: str,
) -> None:
    """Create or edit the persistent message for (kind, guild_id)."""
    try:
        channel = bot.get_channel(channel_id) or await bot.fetch_channel(channel_id)
    except Exception:
        return

    if channel is None:
        return

    ptr = _get_persistent_pointer(kind, guild_id)
    msg_obj: discord.Message | None = None

    if ptr:
        try:
            msg_obj = await channel.fetch_message(int(ptr.get("message_id")))
        except Exception:
            msg_obj = None

    if msg_obj is None:
        try:
            msg_obj = await channel.send(content)
            _set_persistent_pointer(kind, guild_id, channel_id=channel_id, message_id=msg_obj.id)
            return
        except Exception:
            return

    # Only edit if content actually changed (reduces rate-limit pressure)
    try:
        if (msg_obj.content or "") != (content or ""):
            await msg_obj.edit(content=content)
    except Exception:
        # If we can't prove the message is editable anymore, forget the pointer so the next call can recreate.
        _delete_persistent_pointer(kind, guild_id)


def _latest_qualifying_flex_match_id_from_cache(min_group_size: int = 5) -> str | None:
    """Return the newest match_id that appears in the cache for >= min_group_size tracked members."""
    tracked = list(DPM_FLEX_PROFILES.keys())
    if not tracked:
        return None

    # match_id -> {"count": int, "best_created_at": datetime|None}
    agg: dict[str, dict] = {}

    for name in tracked:
        for m in (_iter_cached_matches(name) or []):
            mid = m.get("match_id")
            if not mid:
                continue
            created = m.get("created_at")
            created_utc = _coerce_dt_to_utc(created) if isinstance(created, datetime) else None

            bucket = agg.get(mid)
            if bucket is None:
                agg[mid] = {"count": 1, "best_created_at": created_utc}
            else:
                bucket["count"] = int(bucket.get("count", 0)) + 1
                bt = bucket.get("best_created_at")
                if bt is None or (created_utc is not None and created_utc > bt):
                    bucket["best_created_at"] = created_utc

    best_mid = None
    best_t = None
    for mid, info in agg.items():
        if int(info.get("count", 0)) < int(min_group_size):
            continue
        t = info.get("best_created_at")
        if best_t is None or (t is not None and t > best_t):
            best_t = t
            best_mid = mid

    return best_mid

def _latest_qualifying_flex_match_info_from_cache(min_group_size: int = 5) -> tuple[str | None, datetime | None]:
    """Return (match_id, created_at_utc) for newest qualifying match in cache.

    Qualifying means the same match_id appears for >= min_group_size tracked members.
    """
    tracked = list(DPM_FLEX_PROFILES.keys())
    if not tracked:
        return None, None

    # match_id -> {"count": int, "best_created_at": datetime|None}
    agg: dict[str, dict] = {}

    for name in tracked:
        for m in (_iter_cached_matches(name) or []):
            mid = m.get("match_id")
            if not mid:
                continue
            created = m.get("created_at")
            created_utc = _coerce_dt_to_utc(created) if isinstance(created, datetime) else None

            bucket = agg.get(mid)
            if bucket is None:
                agg[mid] = {"count": 1, "best_created_at": created_utc}
            else:
                bucket["count"] = int(bucket.get("count", 0)) + 1
                bt = bucket.get("best_created_at")
                if bt is None or (created_utc is not None and created_utc > bt):
                    bucket["best_created_at"] = created_utc

    best_mid = None
    best_t = None
    for mid, info in agg.items():
        if int(info.get("count", 0)) < int(min_group_size):
            continue
        t = info.get("best_created_at")
        if t is None:
            continue
        if best_t is None or t > best_t:
            best_t = t
            best_mid = mid

    return best_mid, best_t


def _format_dt_et_short(dt_utc: datetime | None) -> str:
    """Format UTC datetime into ET without year/timezone (MM-DD H:MM AM/PM)."""
    if not isinstance(dt_utc, datetime):
        return "—"
    dt_et = dt_utc.astimezone(DETROIT_TZ)
    # e.g. 12-31 4:07 PM
    s = dt_et.strftime("%m-%d %I:%M %p")
    # remove leading 0s like 04:07 -> 4:07 and 01-02 -> 1-02? keep month/day two-digit for clarity
    s = s.replace(" 0", " ")
    return s



async def _update_all_persistent_flex_messages(bot: commands.Bot, *, reason: str = "refresh") -> None:
    """If persistent messages exist, update them to latest computed leaderboards."""
    state = _load_flex_persistent_state()
    now_utc = datetime.now(timezone.utc)
    header = _format_last_updated_header(now_utc)

    # Weekly
    for guild_id_str, ptr in (state.get("weekly") or {}).items():
        if not isinstance(ptr, dict):
            continue
        try:
            guild_id = int(guild_id_str)
            channel_id = int(ptr.get("channel_id"))
        except Exception:
            continue

        try:
            entries, week_start, now = await compute_weekly_flex_leaderboard_from_opgg_cache()
            body = format_flex_leaderboard(entries, week_start, now)
            await _upsert_persistent_message(
                bot=bot,
                kind="weekly",
                guild_id=guild_id,
                channel_id=channel_id,
                content=header + "\n" + body,
            )
        except Exception as e:
            print(f"[FlexPersist] weekly update failed ({reason}): {type(e).__name__}: {e}")

    # Session (18h)
    for guild_id_str, ptr in (state.get("session") or {}).items():
        if not isinstance(ptr, dict):
            continue
        try:
            guild_id = int(guild_id_str)
            channel_id = int(ptr.get("channel_id"))
        except Exception:
            continue

        try:
            entries, start_utc, end_utc = await compute_recent_flex_leaderboard_from_opgg_cache(hours=18)
            body = format_recent_flex_leaderboard(entries, start_utc, end_utc)
            await _upsert_persistent_message(
                bot=bot,
                kind="session",
                guild_id=guild_id,
                channel_id=channel_id,
                content=header + "\n" + body,
            )
        except Exception as e:
            print(f"[FlexPersist] session update failed ({reason}): {type(e).__name__}: {e}")


def _coerce_dt_to_utc(dt: datetime) -> datetime:
    """Return an aware UTC datetime.

    - If dt is tz-aware: convert to UTC.
    - If dt is naive: assume America/Detroit (matches how users reason about timestamps) then convert to UTC.
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=DETROIT_TZ)
    return dt.astimezone(timezone.utc)


def _load_opgg_flex_cache() -> dict:
    """Load cache from disk (best effort)."""
    global _OPGG_FLEX_CACHE
    if _OPGG_FLEX_CACHE is not None:
        return _OPGG_FLEX_CACHE

    base = {"version": 1, "updated_at": None, "profiles": {}}
    try:
        if os.path.exists(FLEX_OPGG_CACHE_FILE):
            with open(FLEX_OPGG_CACHE_FILE, "r", encoding="utf-8") as f:
                raw = json.load(f) or {}
            # normalize structure
            profiles = raw.get("profiles") or {}
            base["profiles"] = profiles
            base["updated_at"] = raw.get("updated_at")
    except Exception as e:
        print(f"[OPGG][cache] failed to load cache: {type(e).__name__}: {e}")

    _OPGG_FLEX_CACHE = base
    return _OPGG_FLEX_CACHE


def _save_opgg_flex_cache(cache: dict) -> None:
    """Persist cache to disk (best effort)."""
    try:
        tmp_path = FLEX_OPGG_CACHE_FILE + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2, default=str)
        os.replace(tmp_path, FLEX_OPGG_CACHE_FILE)
    except Exception as e:
        print(f"[OPGG][cache] failed to save cache: {type(e).__name__}: {e}")


def _cache_upsert_matches(player_name: str, matches: list[dict]) -> int:
    """Insert new matches into cache for a player. Returns count inserted."""
    cache = _load_opgg_flex_cache()
    profiles = cache.setdefault("profiles", {})
    player_bucket = profiles.setdefault(player_name, {})  # match_id -> payload

    inserted = 0
    for m in matches or []:
        match_id = m.get("match_id")
        if not match_id:
            continue

        if match_id in player_bucket:
            # Already have it — but backfill any missing fields (older cache versions had only op_score).
            existing = player_bucket.get(match_id)
            if isinstance(existing, dict):
                # Prefer new values when the existing entry is missing them (None / key absent).
                if existing.get("created_at") in (None, "") and m.get("created_at") is not None:
                    existing["created_at"] = m.get("created_at")
                if existing.get("op_score") is None and m.get("op_score") is not None:
                    existing["op_score"] = m.get("op_score")
                if existing.get("kda_ratio") is None and m.get("kda_ratio") is not None:
                    existing["kda_ratio"] = m.get("kda_ratio")
                if existing.get("laning_score") is None and m.get("laning_score") is not None:
                    existing["laning_score"] = m.get("laning_score")
            continue

        created = m.get("created_at")
        created_iso = None
        if isinstance(created, datetime):
            created_iso = created.isoformat()
        elif isinstance(created, str):
            created_iso = created

        player_bucket[match_id] = {
            "match_id": match_id,
            "created_at": created_iso,
            "op_score": m.get("op_score"),
            # New: used for extra leaderboard columns.
            "kda_ratio": m.get("kda_ratio"),
            "laning_score": m.get("laning_score"),
        }
        inserted += 1

    cache["updated_at"] = datetime.now(timezone.utc).isoformat()
    _save_opgg_flex_cache(cache)
    return inserted


def _iter_cached_matches(player_name: str) -> list[dict]:
    cache = _load_opgg_flex_cache()
    bucket = (cache.get("profiles") or {}).get(player_name, {}) or {}
    out: list[dict] = []
    for match_id, payload in bucket.items():
        created_iso = payload.get("created_at")
        created_dt = None
        if isinstance(created_iso, str) and created_iso:
            try:
                created_dt = datetime.fromisoformat(created_iso)
            except Exception:
                created_dt = None

        out.append(
            {
                "match_id": payload.get("match_id") or match_id,
                "created_at": created_dt,
                "op_score": payload.get("op_score"),
                "kda_ratio": payload.get("kda_ratio"),
                "laning_score": payload.get("laning_score"),
            }
        )
    return out


async def refresh_opgg_flex_cache_best_effort(reason: str = "manual", *, force: bool = False) -> dict:
    """
    Refresh the local OP.GG flex cache by scraping the most recent FLEXRANKED matches
    for each tracked profile prove.

    Best-effort: a failure for one profile won't fail the whole refresh.

    Args:
        reason: log tag
        force: if True, bypass the 20s anti-storm throttle and wait for the refresh lock
               (instead of bailing) so repeated refresh attempts can be run in sequence.

    Returns:
        {"total_inserted": int, "inserted_by_name": {name: int}}
    """
    global _OPGG_FLEX_CACHE_LAST_REFRESH_UTC

    now_utc = datetime.now(timezone.utc)

    # Avoid refresh storms: skip if we refreshed very recently (unless forced).
    if (not force) and _OPGG_FLEX_CACHE_LAST_REFRESH_UTC and (now_utc - _OPGG_FLEX_CACHE_LAST_REFRESH_UTC).total_seconds() < 20:
        return {"total_inserted": 0, "inserted_by_name": {}}

    # If another refresh is running, either bail (normal) or wait our turn (force=True).
    if _opgg_refresh_lock.locked() and not force:
        return {"total_inserted": 0, "inserted_by_name": {}}

    async with _opgg_refresh_lock:
        # Re-check convince: if we waited for the lock, another refresh may have just happened.
        now_utc = datetime.now(timezone.utc)
        if (not force) and _OPGG_FLEX_CACHE_LAST_REFRESH_UTC and (now_utc - _OPGG_FLEX_CACHE_LAST_REFRESH_UTC).total_seconds() < 20:
            return {"total_inserted": 0, "inserted_by_name": {}}

        _OPGG_FLEX_CACHE_LAST_REFRESH_UTC = now_utc
        _load_opgg_flex_cache()

        if not DPM_FLEX_PROFILES:
            return {"total_inserted": 0, "inserted_by_name": {}}

        try:
            from playwright.async_api import async_playwright
        except Exception as e:
            print(f"[OPGG][cache] playwright import failed: {type(e).__name__}: {e}")
            return {"total_inserted": 0, "inserted_by_name": {}}

        total_inserted = 0
        inserted_by_name: dict[str, int] = {}

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(locale="en-US")

            async def fetch_one(name: str, opgg_url: str) -> tuple[str, list[dict] | None]:
                try:
                    ms = await asyncio.wait_for(
                        _fetch_opgg_flex_matches_from_url(context, opgg_url),
                        timeout=90,
                    )
                    return name, (ms or [])
                except Exception as e:
                    print(f"[OPGG][cache] scrape failed for {name}: {type(e).__name__}: {e}")
                    return name, None

            tasks = []
            for name, prof in DPM_FLEX_PROFILES.items():
                opgg_url = prof.get("opgg_url")
                if not opgg_url:
                    inserted_by_name[name] = 0
                    continue
                tasks.append(fetch_one(name, opgg_url))

            results = await asyncio.gather(*tasks, return_exceptions=False)

            for name, ms in results:
                if not ms:
                    inserted_by_name[name] = 0
                    continue
                inserted = _cache_upsert_matches(name, ms)
                inserted_by_name[name] = inserted
                total_inserted += inserted

            await context.close()
            await browser.close()

        if total_inserted > 0:
            _save_opgg_flex_cache(_OPGG_FLEX_CACHE)

        print(f"[OPGG][cache] refresh done: inserted={total_inserted} reason={reason}")
        return {"total_inserted": total_inserted, "inserted_by_name": inserted_by_name}


async def compute_recent_flex_leaderboard_from_opgg_cache(hours: int = 18) -> tuple[list[dict], datetime, datetime]:
    """Compute a temporary leaderboard from cached OP.GG matches in the last `hours`."""
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(hours=hours)

    # Build match_id -> set(profiles) for matches within window
    match_to_profiles: dict[str, set[str]] = {}

    per_player_matches: dict[str, list[dict]] = {}
    for name in DPM_FLEX_PROFILES.keys():
        ms = _iter_cached_matches(name)
        per_player_matches[name] = ms
        for m in ms:
            created = m.get("created_at")
            if not isinstance(created, datetime):
                continue
            created_utc = _coerce_dt_to_utc(created)
            if not (start_utc <= created_utc <= now_utc):
                continue
            match_id = m.get("match_id")
            if match_id:
                match_to_profiles.setdefault(match_id, set()).add(name)

    entries: list[dict] = []
    for name in DPM_FLEX_PROFILES.keys():
        scores: list[float] = []
        kdas: list[float] = []
        lanes: list[float] = []
        for m in per_player_matches.get(name, []) or []:
            match_id = m.get("match_id")
            if not match_id:
                continue

            created = m.get("created_at")
            if not isinstance(created, datetime):
                continue
            created_utc = _coerce_dt_to_utc(created)

            if not (start_utc <= created_utc <= now_utc):
                continue

            if len(match_to_profiles.get(match_id, set())) < MIN_GROUP_PLAYERS_IN_GAME:
                continue

            op_score = m.get("op_score")
            if isinstance(op_score, (int, float)):
                scores.append(float(op_score))

            kda_ratio = m.get("kda_ratio")
            if isinstance(kda_ratio, (int, float)):
                kdas.append(float(kda_ratio))

            laning_score = m.get("laning_score")
            if isinstance(laning_score, (int, float)):
                lanes.append(float(laning_score))

        if scores:
            entries.append(
                {
                    "name": name,
                    "games": len(scores),
                    "avg": sum(scores) / len(scores),
                    "avg_kda": (sum(kdas) / len(kdas)) if kdas else None,
                    "avg_lane": (sum(lanes) / len(lanes)) if lanes else None,
                }
            )

    entries.sort(key=lambda e: e["avg"], reverse=True)
    return entries, start_utc, now_utc




async def compute_weekly_flex_leaderboard_from_opgg_cache(days: int = 7) -> tuple[list[dict], datetime, datetime]:
    """Rolling window leaderboard from cache.

    By default, looks back the last `days` days from *right now* (UTC).
    This is intentionally NOT Sunday-to-Sunday so you can test cache behavior easily.
    """
    now_utc = datetime.now(timezone.utc)
    start_utc = now_utc - timedelta(days=days)

    per_player_matches: dict[str, list[dict]] = {}
    match_to_profiles: dict[str, set[str]] = {}

    # Build mapping of match_id -> {profiles} for group-size filtering within the rolling window.
    for name in DPM_FLEX_PROFILES.keys():
        ms = _iter_cached_matches(name)
        per_player_matches[name] = ms
        for m in ms:
            created = m.get("created_at")
            if not isinstance(created, datetime):
                continue
            created_utc = _coerce_dt_to_utc(created)

            if not (start_utc <= created_utc <= now_utc):
                continue

            match_id = m.get("match_id")
            if not match_id:
                continue
            match_to_profiles.setdefault(match_id, set()).add(name)

    entries: list[dict] = []
    for name in DPM_FLEX_PROFILES.keys():
        scores: list[float] = []
        kdas: list[float] = []
        lanes: list[float] = []
        for m in per_player_matches.get(name, []) or []:
            match_id = m.get("match_id")
            if not match_id:
                continue

            created = m.get("created_at")
            if not isinstance(created, datetime):
                continue
            created_utc = _coerce_dt_to_utc(created)

            if not (start_utc <= created_utc <= now_utc):
                continue

            if len(match_to_profiles.get(match_id, set())) < MIN_GROUP_PLAYERS_IN_GAME:
                continue

            op_score = m.get("op_score")
            if isinstance(op_score, (int, float)):
                scores.append(float(op_score))

            kda_ratio = m.get("kda_ratio")
            if isinstance(kda_ratio, (int, float)):
                kdas.append(float(kda_ratio))

            laning_score = m.get("laning_score")
            if isinstance(laning_score, (int, float)):
                lanes.append(float(laning_score))

        # Keep weekly minimums for your "real" weekly command;
        # for testing you can temporarily set MIN_FLEX_GAMES_PER_WEEK=0 if you want.
        if len(scores) >= MIN_FLEX_GAMES_PER_WEEK:
            entries.append(
                {
                    "name": name,
                    "games": len(scores),
                    "avg": sum(scores) / len(scores),
                    "avg_kda": (sum(kdas) / len(kdas)) if kdas else None,
                    "avg_lane": (sum(lanes) / len(lanes)) if lanes else None,
                }
            )

    entries.sort(key=lambda e: e["avg"], reverse=True)

    # --- CHANGE COLUMN: compare to latest snapshot from Detroit date exactly 7 days ago ---
    today_detroit = now_utc.astimezone(DETROIT_TZ).date()
    target_day = (today_detroit - timedelta(days=7)).isoformat()

    old_ranks = _get_latest_snapshot_for_day(target_day)
    old_positions = {name: idx + 1 for idx, name in enumerate(old_ranks or [])}

    for idx, e in enumerate(entries):
        new_pos = idx + 1
        old_pos = old_positions.get(e["name"])
        if old_pos is None:
            e["delta"] = None   # formatter will show NEW (we’ll change formatter next)
        else:
            e["delta"] = old_pos - new_pos  # positive = moved up

    # Save today's snapshot (latest call of the day wins)
    _upsert_latest_snapshot_for_today(entries, now_utc)

    return entries, start_utc, now_utc

async def compute_weekly_flex_leaderboard_from_opgg() -> tuple[list[dict], datetime.date, datetime.date]:
    """
    Replacement for compute_weekly_flex_leaderboard_from_local().
    Uses OP.GG 'OP score' for FLEXRANKED match list.
    Enforces:
      - match is within last completed week window
      - match_id shared by at least MIN_GROUP_PLAYERS_IN_GAME tracked profiles
      - min games per player: MIN_FLEX_GAMES_PER_WEEK
    """
    if not DPM_FLEX_PROFILES:
        week_start_utc, week_end_utc, display_start, display_end = get_last_completed_week_window()
        return [], display_start, display_end

    week_start_utc, week_end_utc, display_start, display_end = get_last_completed_week_window()

    # 1) Pull match lists for each profile
    results_by_name: dict[str, list[dict]] = {}

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(locale="en-US")

        async def fetch_one(name: str, url: str) -> list[dict]:
            # hard cap each profile scrape to 25s
            return await asyncio.wait_for(
                _fetch_opgg_flex_matches_from_url(context, url),
                timeout=90,
            )

        tasks = []
        names = []
        for name, prof in DPM_FLEX_PROFILES.items():
            opgg_url = prof.get("opgg_url")
            if not opgg_url:
                results_by_name[name] = []
                continue
            names.append(name)
            tasks.append(fetch_one(name, opgg_url))

        # Run all profile scrapes in parallel
        done = await asyncio.gather(*tasks, return_exceptions=True)

        for name, res in zip(names, done):
            if isinstance(res, Exception):
                print(f"[OPGG] scrape failed for {name}: {type(res).__name__}: {res}")
                results_by_name[name] = []
            else:
                results_by_name[name] = res
        for name, ms in results_by_name.items():
            print(f"[OPGG] {name}: matches scraped = {len(ms or [])}")


        await context.close()
        await browser.close()

    # 2) Build match_id -> set(names) for matches in the week window
    match_to_profiles: dict[str, set[str]] = {}

    for name, matches in results_by_name.items():
        for m in matches or []:
            created = m.get("created_at")
            if not isinstance(created, datetime):
                continue

            # Convert to UTC for comparison (created may have +09:00)
            created_utc = _coerce_dt_to_utc(created)

            if not (week_start_utc <= created_utc < week_end_utc):
                continue

            match_id = m.get("match_id")
            if isinstance(match_id, str) and match_id:
                match_to_profiles.setdefault(match_id, set()).add(name)

    # 3) Compute per-player averages using only group games (>=4 tracked in same match_id)
    entries: list[dict] = []

    for name in DPM_FLEX_PROFILES.keys():
        matches = results_by_name.get(name, []) or []
        scores: list[float] = []

        for m in matches:
            created = m.get("created_at")
            if not isinstance(created, datetime):
                continue

            created_utc = _coerce_dt_to_utc(created)

            if not (week_start_utc <= created_utc < week_end_utc):
                continue

            match_id = m.get("match_id")
            if not isinstance(match_id, str) or not match_id:
                continue

            if len(match_to_profiles.get(match_id, set())) < MIN_GROUP_PLAYERS_IN_GAME:
                continue

            op_score = m.get("op_score")
            if isinstance(op_score, (int, float)):
                scores.append(float(op_score))

        if len(scores) >= MIN_FLEX_GAMES_PER_WEEK:
            avg = sum(scores) / len(scores)
            entries.append({"name": name, "games": len(scores), "avg": avg})

    entries.sort(key=lambda e: e["avg"], reverse=True)

    # reuse your existing history/delta logic by leaving the rest of your pipeline as-is
    return entries, display_start, display_end


def format_flex_leaderboard(entries: list[dict], display_start, display_end) -> str:
    MEDALS = ["🥇", "🥈", "🥉"]

    def _fmt_window(x):
        return x.strftime("%m-%d")

    start_str = _fmt_window(display_start)
    end_str = _fmt_window(display_end)


    if not entries:
        return (
            f"**🏆 WEEKLY FLEX LEADERBOARD** ({start_str} → {end_str})\n"
            "```text\n"
            "No qualifying Flex games found for last week.\n"
            f"- Need ≥{MIN_FLEX_GAMES_PER_WEEK} Flex games per player\n"
            f"- Only counting games where ≥{MIN_GROUP_PLAYERS_IN_GAME}/5 tracked members played together\n"
            "```"
        )

    lines: list[str] = []
    start_str = display_start.strftime("%m-%d")
    end_str = display_end.strftime("%m-%d")
    lines.append(f"**🏆 WEEKLY FLEX LEADERBOARD** ({start_str} → {end_str})")


    

    # ----- Boxed table (ASCII only so alignment stays perfect) -----

    # Precompute string versions for table columns
    rank_col = [str(i) for i in range(1, len(entries) + 1)]
    name_col = [e["name"] for e in entries]
    score_col = [f"{e['avg']:.2f}" for e in entries]
    kda_col = ["--" if e.get("avg_kda") is None else f"{float(e['avg_kda']):.2f}" for e in entries]
    lane_col = ["--" if e.get("avg_lane") is None else f"{float(e['avg_lane']):.2f}" for e in entries]
    games_col = [str(e["games"]) for e in entries]

    change_col = []
    for e in entries:
        delta = e.get("delta")
        if delta is None:
            change_col.append("--")
        elif delta == 0:
            change_col.append("-")
        elif delta > 0:
            change_col.append(f"+{delta}")
        else:
            change_col.append(str(delta))

    # Column widths
    rank_w = max(len("RK"), max(len(r) for r in rank_col))
    name_w = max(len("Player"), max(len(n) for n in name_col))
    score_w = max(len("Score"), max(len(s) for s in score_col))
    kda_w = max(len("KDA"), max(len(s) for s in kda_col))
    lane_w = max(len("Lane"), max(len(s) for s in lane_col))
    games_w = max(len("Games"), max(len(g) for g in games_col))
    change_w = max(len("Change"), max(len(c) for c in change_col))

    # Start code block
    lines.append("```text")

    # Header
    header = (
        f"{'RK'.rjust(rank_w)}  "
        f"{'Player'.ljust(name_w)}   "
        f"{'Score'.rjust(score_w)} "
        f"{'KDA'.rjust(kda_w)}  "
        f"{'Lane'.rjust(lane_w)}    "
        f"{'Games'.rjust(games_w)}  "
        f"{'Change'.rjust(change_w)}"
    )
    lines.append(header)
    lines.append("-" * (len(header) + 1))

    # Rows
    for idx, (r, n, s, k, l, g, c) in enumerate(zip(rank_col, name_col, score_col, kda_col, lane_col, games_col, change_col), start=1):

        # --- Replace ranks 1,2,3 with medal emoji (no padding!) ---
        if idx == 1:
            rk = "🥇"
        elif idx == 2:
            rk = "🥈"
        elif idx == 3:
            rk = "🥉"
        else:
            rk = r.rjust(rank_w)  # numeric ranks stay aligned

        # For emoji rows → no rjust (prevents whitespace)
        if idx <= 3:
            rk_field = rk   # raw, no spacing at all
        else:
            rk_field = rk   # already right-justified for numeric

        line = (
            f"{rk_field}  "
            f"{n.ljust(name_w)}  "
            f"{s.rjust(score_w)}  "
            f"{k.rjust(kda_w)}  "
            f"{l.rjust(lane_w)}  "
            f"{g.rjust(games_w)}  "
            f"{c.rjust(change_w)}"
        )
        lines.append(line)


    lines.append("```")

    return "\n".join(lines)




async def fetch_dpm_matches_for_player(
    session: aiohttp.ClientSession,
    name: str,
    profile: dict,
    week_start_ts: float,
    tracked_puuids: set[str],
    min_group_size: int,
) -> list[float]:
    """
    Fetch weekly Flex games for a single profile and return the list of dpmScore values
    for games that:
      - are Flex queue
      - were created after week_start_ts
      - contain at least `min_group_size` tracked players
    """
    endpoint = profile["endpoint"]
    puuid = profile["puuid"]

    scores: list[float] = []

    try:
        async with session.get(endpoint) as resp:
            if resp.status != 200:
                text = await resp.text()
                print(f"[FlexLB] {name}: HTTP {resp.status} from DPM: {text[:200]}")
                return scores

            data = await resp.json()

    except Exception as e:
        print(f"[FlexLB] {name}: error fetching DPM data: {e}")
        return scores

    matches = data.get("matches", []) or []
    for m in matches:
        # 1) Ensure Flex queue
        if m.get("queueId") != FLEX_QUEUE_ID:
            continue

        # 2) Ensure within last week
        game_creation_ms = m.get("gameCreation") or 0
        game_ts = game_creation_ms / 1000.0  # DPM uses ms since epoch
        if game_ts < week_start_ts:
            continue

        participants = m.get("participants", []) or []

        # 3) How many tracked players were in this game?
        present = 0
        for p in participants:
            p_puuid = p.get("puuid")
            if p_puuid in tracked_puuids:
                present += 1

        if present < min_group_size:
            # Not enough people from our group in this game
            continue

        # 4) Find this player's participant row
        player_part = None
        for p in participants:
            if p.get("puuid") == puuid:
                player_part = p
                break

        if not player_part:
            continue

        dpm_score = player_part.get("dpmScore")
        if isinstance(dpm_score, (int, float)):
            scores.append(float(dpm_score))

    return scores


async def compute_weekly_flex_leaderboard() -> tuple[list[dict], datetime, datetime]:
    """
    Compute the weekly Flex leaderboard.

    Returns:
        (entries, week_start_dt, now_dt)
        where entries is a list of dicts:
            { "name": str, "games": int, "avg": float }
        sorted by avg descending.
    """
    if not DPM_FLEX_PROFILES:
        return [], datetime.now(timezone.utc), datetime.now(timezone.utc)

    now = datetime.now(timezone.utc)
    week_start = now - timedelta(days=7)
    week_start_ts = week_start.timestamp()

    # All tracked PUUIDs (for 'how many people from the list are in this game')
    tracked_puuids: set[str] = {p["puuid"] for p in DPM_FLEX_PROFILES.values()}

    # "At least 4/5 of the people from the list are in the game"
    # Generalized to ceil(0.8 * N) so it still works if you ever add/remove people.
    import math as _math
    min_group_size = max(1, _math.ceil(1 * len(tracked_puuids)))

    entries: list[dict] = []

    async with aiohttp.ClientSession() as session:
        tasks_list = []
        names = list(DPM_FLEX_PROFILES.keys())

        for name in names:
            profile = DPM_FLEX_PROFILES[name]
            tasks_list.append(
                fetch_dpm_matches_for_player(
                    session,
                    name,
                    profile,
                    week_start_ts,
                    tracked_puuids,
                    min_group_size,
                )
            )

        results = await asyncio.gather(*tasks_list, return_exceptions=True)

    for name, scores_or_exc in zip(DPM_FLEX_PROFILES.keys(), results):
        if isinstance(scores_or_exc, Exception):
            print(f"[FlexLB] {name}: exception {scores_or_exc}")
            continue

        scores: list[float] = scores_or_exc

        # Only consider players with at least MIN_FLEX_GAMES_PER_WEEK eligible games
        if len(scores) < MIN_FLEX_GAMES_PER_WEEK:
            continue

        avg = sum(scores) / len(scores)
        entries.append({"name": name, "games": len(scores), "avg": avg})

    # Sort highest to lowest
    entries.sort(key=lambda e: e["avg"], reverse=True)

    return entries, week_start, now







async def fetch_schmort_match_basic(n: int = 1) -> tuple[str, dict, str]:
    """
    Fetch Schmort#bone's (NA) n-th most recent match.
    Returns (match_id, match_data, puuid).

    This is independent of get_match_for_schmort and does NOT fetch timeline.
    """
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        raise RuntimeError("RIOT_API_KEY is not set in the environment.")

    if n < 1:
        n = 1

    async with aiohttp.ClientSession() as session:
        # 1) Account info
        account_url = (
            f"https://{RIOT_ROUTING_REGION}.api.riotgames.com/riot/account/v1/"
            f"accounts/by-riot-id/{quote(RIOT_GAME_NAME)}/{quote(RIOT_TAGLINE)}"
        )
        account_data = await _riot_get_json(session, account_url, api_key)
        puuid = account_data["puuid"]

        # 2) Match list
        count = max(n, 1)
        matches_url = (
            f"https://{RIOT_ROUTING_REGION}.api.riotgames.com/lol/match/v5/"
            f"matches/by-puuid/{puuid}/ids?start=0&count={count}"
        )
        match_ids = await _riot_get_json(session, matches_url, api_key)
        if len(match_ids) < n:
            raise RuntimeError(f"Player only has {len(match_ids)} matches available.")
        match_id = match_ids[n - 1]

        # 3) Match data
        match_url = (
            f"https://{RIOT_ROUTING_REGION}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        )
        match_data = await _riot_get_json(session, match_url, api_key)

    return match_id, match_data, puuid

# ---------- DPM-style scoring (independent of old scoring) ----------

# Per-role KDA weights (approx from your DPM exports)
DPM_KDA_WEIGHTS = {
    "TOP":     {"kills": 0.80, "deaths": -1.50, "assists": 0.80},
    "JUNGLE":  {"kills": 0.75, "deaths": -1.50, "assists": 0.75},
    "MIDDLE":  {"kills": 0.75, "deaths": -1.50, "assists": 0.75},
    "BOTTOM":  {"kills": 0.75, "deaths": -1.50, "assists": 0.75},
    "UTILITY": {"kills": 0.85, "deaths": -1.25, "assists": 0.90},
}

# Vision score / min → contribution, by role
DPM_VSPM_WEIGHTS = {
    "TOP":     {"a": 7.10, "b": -3.55},
    "JUNGLE":  {"a": 5.64, "b": -3.69},
    "MIDDLE":  {"a": 7.00, "b": -3.85},
    "BOTTOM":  {"a": 7.00, "b": -3.85},
    "UTILITY": {"a": 5.20, "b": -7.54},
}

def _dpm_get_role(player: dict) -> str:
    raw = (
        player.get("teamPosition")
        or player.get("individualPosition")
        or player.get("lane")
        or ""
    ).upper()
    if raw in ("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"):
        return raw
    # If Riot gives us weird stuff (e.g. "NONE"), treat as ADC-ish
    return "BOTTOM"


def _dpm_global(player: dict, game_minutes: float) -> tuple[float, dict]:
    """Global section: KDA, CS/min, gold/min, damage/min, vision/min, first blood."""
    role = _dpm_get_role(player)
    ch = player.get("challenges", {}) or {}

    w_kda = DPM_KDA_WEIGHTS.get(role, DPM_KDA_WEIGHTS["BOTTOM"])
    w_vspm = DPM_VSPM_WEIGHTS.get(role, DPM_VSPM_WEIGHTS["BOTTOM"])

    kills = player.get("kills", 0)
    deaths = player.get("deaths", 0)
    assists = player.get("assists", 0)

    total_cs = player.get("totalMinionsKilled", 0) + player.get("neutralMinionsKilled", 0)
    cs_pm = total_cs / game_minutes if game_minutes > 0 else 0.0

    gold = player.get("goldEarned", 0)
    gpm = ch.get("goldPerMinute")
    if gpm is None:
        gpm = gold / game_minutes if game_minutes > 0 else 0.0

    dmg = player.get("totalDamageDealtToChampions", 0)
    dpm = ch.get("damagePerMinute")
    if dpm is None:
        dpm = dmg / game_minutes if game_minutes > 0 else 0.0

    vision = player.get("visionScore", 0)
    vspm = ch.get("visionScorePerMinute")
    if vspm is None:
        vspm = vision / game_minutes if game_minutes > 0 else 0.0

    got_fb = bool(player.get("firstBloodKill") or player.get("firstBloodAssist"))

    # KDA contributions
    kills_c = w_kda["kills"] * kills
    deaths_c = w_kda["deaths"] * deaths
    assists_c = w_kda["assists"] * assists

    # Per-minute stat contributions (same across roles except vspm)
    cs_c = 0.65 * cs_pm - 2.4      # approximate from exports
    gpm_c = 0.03 * gpm - 8.5
    dpm_c = 0.0067 * dpm + 0.07
    vspm_c = w_vspm["a"] * vspm + w_vspm["b"]

    fb_c = 5.0 if got_fb else 0.0

    total = kills_c + deaths_c + assists_c + cs_c + gpm_c + dpm_c + vspm_c + fb_c
    details = {
        "kills": kills_c,
        "deaths": deaths_c,
        "assists": assists_c,
        "csm": cs_c,
        "goldPerMinute": gpm_c,
        "damagePerMinute": dpm_c,
        "visionScorePerMinute": vspm_c,
        "firstBlood": fb_c,
    }
    return total, details


def _dpm_objectives(player: dict, team_participants: list[dict]) -> tuple[float, dict]:
    """Objectives section: dragons, heralds, barons, horde, objective damage, steals."""
    ch = player.get("challenges", {}) or {}

    dragons = ch.get("dragonTakedowns", 0.0)
    heralds = ch.get("riftHeraldTakedowns", 0.0)
    barons = ch.get("baronTakedowns", 0.0)
    # DPM uses "horde" (void grubs / atakhan); approximate via any void epic stat if present
    horde = ch.get("voidMonsterKills", 0.0)
    steals = ch.get("epicMonsterSteals", 0.0)

    obj_damage = player.get("damageDealtToObjectives", 0.0) or 0.0

    # Coefficients approximated from DPM exports
    dragon_c = 2.0 * dragons
    baron_c = 2.0 * barons
    herald_c = 3.0 * heralds
    horde_c = 0.5 * horde
    dmg_c = 6.15e-05 * obj_damage
    steals_c = 3.5 * steals

    total = dragon_c + baron_c + herald_c + horde_c + dmg_c + steals_c
    details = {
        "dragon": dragon_c,
        "baron": baron_c,
        "riftHerald": herald_c,
        "horde": horde_c,
        "damageDealtToObjectives": dmg_c,
        "epicMonsterSteals": steals_c,
    }
    return total, details


def _dpm_team(player: dict, team_participants: list[dict]) -> tuple[float, dict]:
    """
    Team section: kill participation, damage share, damage taken share.
    Using KP%, teamDamage% and damageTaken% from challenges (0..1) mapped via linear fits.
    """
    ch = player.get("challenges", {}) or {}

    kp_frac = ch.get("killParticipation")
    team_dmg_frac = ch.get("teamDamagePercentage")
    taken_frac = ch.get("damageTakenOnTeamPercentage")

    # Fallbacks if challenges missing (just in case)
    team_kills = sum(tp.get("kills", 0) for tp in team_participants) or 1
    if kp_frac is None:
        kp_frac = float(player.get("kills", 0) + player.get("assists", 0)) / team_kills

    if team_dmg_frac is None:
        team_total_dmg = sum(tp.get("totalDamageDealtToChampions", 0) for tp in team_participants) or 1
        team_dmg_frac = float(player.get("totalDamageDealtToChampions", 0)) / team_total_dmg

    if taken_frac is None:
        team_total_taken = sum(tp.get("totalDamageTaken", 0) for tp in team_participants) or 1
        taken_frac = float(player.get("totalDamageTaken", 0)) / team_total_taken

    # Convert to % to match the scale we fitted on
    kp_pct = kp_frac * 100.0
    dmg_pct = team_dmg_frac * 100.0
    taken_pct = taken_frac * 100.0

    kp_c = 0.1280 * kp_pct - 3.221
    taken_c = 0.0693 * taken_pct - 0.434
    dmg_c = 0.0898 * dmg_pct - 0.317

    total = kp_c + dmg_c + taken_c
    details = {
        "killParticipation": kp_c,
        "teamDamagePercentage": dmg_c,
        "damageTakenOnTeamPercentage": taken_c,
    }
    return total, details


def _dpm_role_section(player: dict) -> tuple[float, dict]:
    """
    Role-specific micro section: uses challenges fields.
    This does NOT touch your old lane scoring.
    """
    role = _dpm_get_role(player)
    ch = player.get("challenges", {}) or {}

    details = {}
    total = 0.0

    if role in ("TOP", "MIDDLE", "BOTTOM"):
        lm10 = ch.get("laneMinionsFirst10Minutes", 0.0)
        solo = ch.get("soloKills", player.get("soloKills", 0.0))
        plates = ch.get("turretPlatesTaken", 0.0)
        turrets = player.get("turretTakedowns", 0.0)
        first_tower = 5.0 if (player.get("firstTowerKill") or player.get("firstTowerAssist")) else 0.0

        if role == "TOP":
            lm_a, lm_b = 0.35, -18.9
            solo_w = 0.75
            turret_w = 0.85
        elif role == "MIDDLE":
            lm_a, lm_b = 0.35, -18.55
            solo_w = 0.85
            turret_w = 0.75
        else:  # BOTTOM
            lm_a, lm_b = 0.37, -18.87
            solo_w = 1.50
            turret_w = 0.75

        lm_c = lm_a * lm10 + lm_b
        solo_c = solo_w * solo
        plates_c = 0.75 * plates
        turrets_c = turret_w * turrets

        total = lm_c + solo_c + plates_c + turrets_c + first_tower
        details = {
            "laneMinionsFirst10Minutes": lm_c,
            "soloKills": solo_c,
            "turretPlatesTaken": plates_c,
            "turretTakedowns": turrets_c,
            "firstTurretKilled": first_tower,
        }

    elif role == "JUNGLE":
        init_crabs = ch.get("initialCrabCount", 0.0)
        scuttles = ch.get("scuttleCrabKills", 0.0)
        jg_cs10 = ch.get("jungleCsBefore10Minutes", 0.0)
        enemy_jg = ch.get("enemyJungleMonsterKills", 0.0)
        buffs = ch.get("buffsStolen", 0.0)
        picks = ch.get("pickKillWithAlly", 0.0)

        init_c = 1.5 * init_crabs
        scuttle_c = 1.0 * scuttles
        jg_cs_c = 0.10 * jg_cs10
        enemy_jg_c = 1.0 * enemy_jg
        buffs_c = 1.5 * buffs
        picks_c = 1.0 * picks

        total = init_c + scuttle_c + jg_cs_c + enemy_jg_c + buffs_c + picks_c
        details = {
            "initialCrabCount": init_c,
            "scuttleCrabKills": scuttle_c,
            "jungleCsBefore10Minutes": jg_cs_c,
            "enemyJungleMonsterKills": enemy_jg_c,
            "buffsStolen": buffs_c,
            "pickKillWithAlly": picks_c,
        }

    elif role == "UTILITY":
        cw_time = ch.get("controlWardTimeCoverageInRiverOrEnemyHalf", 0.0) or 0.0
        cw_placed = ch.get("controlWardsPlaced", 0.0)
        ward_takedowns = ch.get("wardTakedowns", 0.0)
        stealth = ch.get("stealthWardsPlaced", 0.0)
        picks = ch.get("pickKillWithAlly", 0.0)
        saves = ch.get("saveAllyFromDeath", 0.0)

        cw_time_c = 4.0 * cw_time
        cw_placed_c = 1.0 * cw_placed
        ward_takedowns_c = 1.0 * ward_takedowns
        stealth_c = 0.5 * stealth
        picks_c = 1.0 * picks
        saves_c = 1.0 * saves

        total = cw_time_c + cw_placed_c + ward_takedowns_c + stealth_c + picks_c + saves_c
        details = {
            "controlWardTimeCoverageInRiverOrEnemyHalf": cw_time_c,
            "controlWardsPlaced": cw_placed_c,
            "wardTakedowns": ward_takedowns_c,
            "stealthWardsPlaced": stealth_c,
            "pickKillWithAlly": picks_c,
            "saveAllyFromDeath": saves_c,
        }

    return total, details


def compute_dpm_score(match_data: dict, puuid: str) -> tuple[float, dict, dict]:
    """
    Compute a DPM-style score for the given player in a match.

    Returns:
        (final_score_0_100, breakdown_dict, player_participant_dict)
    """
    info = match_data.get("info", {})
    participants = info.get("participants", [])

    player = None
    for p in participants:
        if p.get("puuid") == puuid:
            player = p
            break
    if player is None:
        raise RuntimeError("Player puuid not found in match participants.")

    team_id = player.get("teamId")
    team_participants = [p for p in participants if p.get("teamId") == team_id]

    # Use timePlayed if present; otherwise fall back to gameDuration
    time_played = player.get("timePlayed") or info.get("gameDuration", 1)
    game_minutes = max(time_played / 60.0, 1e-3)

    global_score, global_details = _dpm_global(player, game_minutes)
    obj_score, obj_details = _dpm_objectives(player, team_participants)
    team_score, team_details = _dpm_team(player, team_participants)
    role_score, role_details = _dpm_role_section(player)

    win = bool(player.get("win"))
    game_state_score = 3.0 if win else -3.0

    raw_total = 15.0 + global_score + obj_score + team_score + game_state_score + role_score
    final_score = max(0.0, min(100.0, raw_total))

    breakdown = {
        "win": win,
        "role": _dpm_get_role(player),
        "global": {"score": global_score, "details": global_details},
        "objectives": {"score": obj_score, "details": obj_details},
        "team": {"score": team_score, "details": team_details},
        "gameState": {"score": game_state_score, "details": {"win": game_state_score}},
        "roleSection": {"score": role_score, "details": role_details},
        "raw_total_before_clamp": raw_total,
    }
    return final_score, breakdown, player


# ---------- DPM-like score (recreating DPM site structure) ----------

# Per-role KDA weights (reverse-engineered from DPM exports)
DPM_GLOBAL_KDA = {
    "TOP":     {"kills": 0.80, "deaths": -1.50, "assists": 0.80},
    "JUNGLE":  {"kills": 0.75, "deaths": -1.50, "assists": 0.75},
    "MIDDLE":  {"kills": 0.75, "deaths": -1.50, "assists": 0.75},
    "BOTTOM":  {"kills": 0.75, "deaths": -1.50, "assists": 0.75},
    "UTILITY": {"kills": 0.85, "deaths": -1.25, "assists": 0.90},
}

# Approx VSPM → global contribution by role: a * vspm + b
DPM_GLOBAL_VSPM = {
    "TOP":     {"a": 7.10, "b": -3.55},
    "JUNGLE":  {"a": 5.64, "b": -3.69},
    "MIDDLE":  {"a": 7.00, "b": -3.85},
    "BOTTOM":  {"a": 7.00, "b": -3.85},
    "UTILITY": {"a": 5.20, "b": -7.54},
}

# CS/min contribution per role: a * cs_pm + b (approximate)
DPM_GLOBAL_CSPM = {
    "TOP":     {"a": 2.20, "b": -12.0},
    "JUNGLE":  {"a": 1.55, "b": -9.0},
    "MIDDLE":  {"a": 1.80, "b": -10.5},
    "BOTTOM":  {"a": 1.80, "b": -10.5},
    "UTILITY": {"a": 0.0,  "b": 0.0},
}

# Gold/min contribution: a * gpm + b (approximate)
DPM_GLOBAL_GPM = {
    "TOP":     {"a": 0.035, "b": -12.0},
    "JUNGLE":  {"a": 0.035, "b": -12.0},
    "MIDDLE":  {"a": 0.040, "b": -14.0},
    "BOTTOM":  {"a": 0.040, "b": -14.0},
    "UTILITY": {"a": 0.060, "b": -16.2},
}

# Damage/min contribution: a * dpm (+ small intercept)
DPM_GLOBAL_DPM = {
    "TOP":     {"a": 0.0070,  "b": 0.0},
    "JUNGLE":  {"a": 0.0060,  "b": 0.0},
    "MIDDLE":  {"a": 0.0073,  "b": 0.0},
    "BOTTOM":  {"a": 0.0067,  "b": 0.0},
    "UTILITY": {"a": 0.0045,  "b": 0.0},
}


def _get_role(player: dict) -> str:
    raw = (player.get("teamPosition") or player.get("role") or "").upper()
    if raw in ("TOP", "JUNGLE", "MIDDLE", "BOTTOM", "UTILITY"):
        return raw
    # fallback: treat unknown as ADC-ish
    return "BOTTOM"


def _dpm_global_section(player: dict, game_minutes: float) -> tuple[float, dict]:
    """Approximate DPM 'global' section from Riot stats."""
    role = _get_role(player)
    kcfg = DPM_GLOBAL_KDA.get(role, DPM_GLOBAL_KDA["BOTTOM"])
    vcfg = DPM_GLOBAL_VSPM.get(role, DPM_GLOBAL_VSPM["BOTTOM"])
    ccfg = DPM_GLOBAL_CSPM.get(role, DPM_GLOBAL_CSPM["BOTTOM"])
    gcfg = DPM_GLOBAL_GPM.get(role, DPM_GLOBAL_GPM["BOTTOM"])
    dcfg = DPM_GLOBAL_DPM.get(role, DPM_GLOBAL_DPM["BOTTOM"])

    ch = player.get("challenges", {}) or {}

    kills = player.get("kills", 0)
    deaths = player.get("deaths", 0)
    assists = player.get("assists", 0)

    cs = player.get("totalMinionsKilled", 0) + player.get("neutralMinionsKilled", 0)
    cs_pm = cs / game_minutes if game_minutes > 0 else 0.0

    gold = player.get("goldEarned", 0)
    gpm = ch.get("goldPerMinute")
    if gpm is None:
        gpm = gold / game_minutes if game_minutes > 0 else 0.0

    damage = player.get("totalDamageDealtToChampions", 0)
    dpm = ch.get("damagePerMinute")
    if dpm is None:
        dpm = damage / game_minutes if game_minutes > 0 else 0.0

    vision = player.get("visionScore", 0)
    vspm = ch.get("visionScorePerMinute")
    if vspm is None:
        vspm = vision / game_minutes if game_minutes > 0 else 0.0

    # First blood: +5 if you got FB (kill or assist)
    first_blood = 0.0
    if player.get("firstBloodKill") or player.get("firstBloodAssist"):
        first_blood = 5.0

    kills_contrib = kcfg["kills"] * kills
    deaths_contrib = kcfg["deaths"] * deaths
    assists_contrib = kcfg["assists"] * assists

    cs_contrib = ccfg["a"] * cs_pm + ccfg["b"]
    gpm_contrib = gcfg["a"] * gpm + gcfg["b"]
    dpm_contrib = dcfg["a"] * dpm + dcfg["b"]
    vspm_contrib = vcfg["a"] * vspm + vcfg["b"]

    total = (
        kills_contrib
        + deaths_contrib
        + assists_contrib
        + cs_contrib
        + gpm_contrib
        + dpm_contrib
        + vspm_contrib
        + first_blood
    )

    details = {
        "kills": kills_contrib,
        "deaths": deaths_contrib,
        "assists": assists_contrib,
        "csm": cs_contrib,
        "goldPerMinute": gpm_contrib,
        "damagePerMinute": dpm_contrib,
        "visionScorePerMinute": vspm_contrib,
        "firstBlood": first_blood,
    }
    return total, details





def _dpm_objectives_section(player: dict, team_participants: list[dict]) -> tuple[float, dict]:
    ch = player.get("challenges", {}) or {}
    dragon = ch.get("dragonTakedowns", 0.0)
    herald = ch.get("riftHeraldTakedowns", 0.0)
    baron = ch.get("baronTakedowns", 0.0)
    # "horde" on DPM is void grubs; approximate with any "void" epic count if present
    horde = ch.get("voidMonsterKills", 0.0)

    obj_damage = player.get("damageDealtToObjectives", 0.0)
    team_obj_total = sum(tp.get("damageDealtToObjectives", 0) for tp in team_participants) or 1.0
    obj_damage_share = obj_damage / team_obj_total

    # Approximate similar scale to DPM exports
    dragon_c = 1.5 * dragon
    herald_c = 1.5 * herald
    baron_c = 2.5 * baron
    horde_c = 2.0 * horde
    dmg_c = 3.0 * obj_damage_share

    total = dragon_c + herald_c + baron_c + horde_c + dmg_c
    details = {
        "dragon": dragon_c,
        "baron": baron_c,
        "riftHerald": herald_c,
        "horde": horde_c,
        "damageDealtToObjectives": dmg_c,
        "epicMonsterSteals": 0.0,  # left 0 for now; easy to hook in later
    }
    return total, details


def _dpm_team_section(player: dict, team_participants: list[dict]) -> tuple[float, dict]:
    role = _get_role(player)
    ch = player.get("challenges", {}) or {}

    # Riot's challenges store these directly for most queues
    kp_frac = ch.get("killParticipation")
    if kp_frac is None:
        team_kills = sum(tp.get("kills", 0) for tp in team_participants) or 1
        kp_frac = float(player.get("kills", 0) + player.get("assists", 0)) / team_kills

    team_dmg_frac = ch.get("teamDamagePercentage")
    if team_dmg_frac is None:
        team_total_damage = sum(tp.get("totalDamageDealtToChampions", 0) for tp in team_participants) or 1
        team_dmg_frac = float(player.get("totalDamageDealtToChampions", 0)) / team_total_damage

    taken_frac = ch.get("damageTakenOnTeamPercentage")
    if taken_frac is None:
        team_total_taken = sum(tp.get("totalDamageTaken", 0) for tp in team_participants) or 1
        taken_frac = float(player.get("totalDamageTaken", 0)) / team_total_taken

    expected_dmg_share = ROLE_EXPECTED_DAMAGE_SHARE.get(role, 0.22)

    # Scale into DPM-like ranges (~ -2..+6-ish each)
    kp_pct = kp_frac * 100.0
    kp_c = (kp_pct - 55.0) / 10.0  # ~0 when ~55% KP

    dmg_delta = (team_dmg_frac - expected_dmg_share) / max(expected_dmg_share, 1e-6)
    dmg_c = 3.0 * dmg_delta

    # For tanks / supports, taking more damage is ok; for carries it's slightly punished
    base_taken_share = 1.0 / len(team_participants)
    taken_delta = (taken_frac - base_taken_share) / max(base_taken_share, 1e-6)
    if role in ("UTILITY", "TOP", "JUNGLE"):
        taken_c = 2.0 * taken_delta
    else:
        taken_c = -1.5 * taken_delta

    total = kp_c + dmg_c + taken_c
    details = {
        "killParticipation": kp_c,
        "teamDamagePercentage": dmg_c,
        "damageTakenOnTeamPercentage": taken_c,
    }
    return total, details


def _dpm_role_micro_section(player: dict) -> tuple[float, dict]:
    """
    Micro / role-specific section: 'bottom', 'middle', 'top', 'jungle', 'utility'
    built from Riot challenges. Approximates DPM's role sections.
    """
    role = _get_role(player)
    ch = player.get("challenges", {}) or {}
    details: dict[str, float] = {}
    total = 0.0

    if role in ("TOP", "MIDDLE", "BOTTOM"):
        # Lane roles
        lm10 = ch.get("laneMinionsFirst10Minutes", 0.0)
        solo_kills = ch.get("soloKills", 0.0)
        plates = ch.get("turretPlatesTaken", 0.0)
        turret_takedowns = player.get("turretTakedowns", 0.0)
        first_tower = 5.0 if player.get("firstTowerKill") or player.get("firstTowerAssist") else 0.0

        if role == "TOP":
            lm_a, lm_b = 0.35, -18.9
            solo_coeff = 0.75
            turret_coeff = 0.85
        elif role == "MIDDLE":
            lm_a, lm_b = 0.35, -18.55
            solo_coeff = 0.85
            turret_coeff = 0.75
        else:  # BOTTOM (ADC)
            lm_a, lm_b = 0.37, -18.87
            solo_coeff = 1.50
            turret_coeff = 0.75

        lm_c = lm_a * lm10 + lm_b
        solo_c = solo_coeff * solo_kills
        plates_c = 0.75 * plates
        turret_c = turret_coeff * turret_takedowns

        total = lm_c + solo_c + plates_c + turret_c + first_tower
        details = {
            "laneMinionsFirst10Minutes": lm_c,
            "soloKills": solo_c,
            "turretPlatesTaken": plates_c,
            "turretTakedowns": turret_c,
            "firstTurretKilled": first_tower,
        }

    elif role == "JUNGLE":
        init_crabs = ch.get("initialCrabCount", 0.0)
        scuttles = ch.get("scuttleCrabKills", 0.0)
        jng_cs10 = ch.get("jungleCsBefore10Minutes", 0.0)
        enemy_jg = ch.get("enemyJungleMonsterKills", 0.0)
        buffs_stolen = ch.get("buffsStolen", 0.0)
        pick_kill = ch.get("pickKillWithAlly", 0.0)

        # Close to DPM exports where the jungle details nearly summed to the jungle score
        init_c = 1.5 * init_crabs
        scuttle_c = 1.0 * scuttles
        jng_cs_c = 0.1 * jng_cs10
        enemy_jg_c = 1.0 * enemy_jg
        buffs_c = 1.5 * buffs_stolen
        pick_c = 1.0 * pick_kill

        total = init_c + scuttle_c + jng_cs_c + enemy_jg_c + buffs_c + pick_c
        details = {
            "initialCrabCount": init_c,
            "scuttleCrabKills": scuttle_c,
            "jungleCsBefore10Minutes": jng_cs_c,
            "enemyJungleMonsterKills": enemy_jg_c,
            "buffsStolen": buffs_c,
            "pickKillWithAlly": pick_c,
        }

    elif role == "UTILITY":
        cw_time = ch.get("controlWardTimeCoverageInRiverOrEnemyHalf", 0.0) or 0.0
        cw_placed = ch.get("controlWardsPlaced", 0.0)
        ward_takedowns = ch.get("wardTakedowns", 0.0)
        stealth_wards = ch.get("stealthWardsPlaced", 0.0)
        pick_kill = ch.get("pickKillWithAlly", 0.0)
        save_ally = ch.get("saveAllyFromDeath", 0.0)
        # completeSupportQuestInTime often encoded as +/-3 on DPM; we skip it for now.

        cw_time_c = 4.0 * cw_time
        cw_placed_c = 1.0 * cw_placed
        ward_takedowns_c = 1.0 * ward_takedowns
        stealth_c = 0.5 * stealth_wards
        pick_c = 1.0 * pick_kill
        save_c = 1.0 * save_ally

        total = cw_time_c + cw_placed_c + ward_takedowns_c + stealth_c + pick_c + save_c
        details = {
            "controlWardTimeCoverageInRiverOrEnemyHalf": cw_time_c,
            "controlWardsPlaced": cw_placed_c,
            "wardTakedowns": ward_takedowns_c,
            "stealthWardsPlaced": stealth_c,
            "pickKillWithAlly": pick_c,
            "saveAllyFromDeath": save_c,
        }

    return total, details





async def build_reply_chain(start_message: discord.Message, max_depth: Optional[int] = None) -> list[discord.Message]:
    """
    Walk up the reply chain from the user's message to the root.
    Returns a list oldest->newest (excluding the user's current message).
    max_depth=None means unlimited.
    """
    chain: list[discord.Message] = []
    ref = start_message
    depth = 0

    while getattr(ref, "reference", None):
        if max_depth is not None and depth >= max_depth:
            break

        parent = ref.reference.resolved
        if parent is None:
            try:
                parent = await ref.channel.fetch_message(ref.reference.message_id)
            except Exception:
                break  # couldn't fetch; stop

        chain.append(parent)
        ref = parent
        depth += 1

    chain.reverse()  # oldest -> newest
    return chain


def format_block(title: str, lines: list[str]) -> str:
    if not lines:
        return ""
    return (
        f"BEGIN_{title}\n"
        "Rules: Lines are '[DisplayName] :: message'. Mentions are plain text (@Name), do NOT ping IDs. "
        "Do not quote or echo this block; it is context only.\n"
        + "\n".join(lines) +
        f"\nEND_{title}\n"
    )

async def build_recent_context_block(channel: discord.TextChannel, delta: timedelta) -> str:
    since = discord.utils.utcnow() - delta
    msgs: list[str] = []
    async for m in channel.history(limit=1000, after=since, before=discord.utils.utcnow(), oldest_first=True):
        if not m.content:
            continue
        content = normalize_visible_ats(m.content.strip())
        msgs.append(f"[{m.author.display_name}] {MENTION_SEP} {content}")
    return format_block(f"RECENT_CONTEXT_LAST_{int(delta.total_seconds())}_SECS", msgs)

def format_reply_chain_block(chain_msgs: list[discord.Message]) -> str:
    lines: list[str] = []
    for m in chain_msgs:
        if not m.content:
            continue
        content = normalize_visible_ats(m.content.strip())
        lines.append(f"[{m.author.display_name}] {MENTION_SEP} {content}")
    return format_block(f"REPLY_CHAIN_LEN_{len(lines)}", lines)



_ready_synced = False

@tasks.loop(minutes=30)
async def josh_soloq_lp_nickname_loop():
    """
    Every 30 minutes:
      - fetch Josh SoloQ LP
      - if LP changed since last check, update nickname in guild
    """
    api_key = os.getenv("RIOT_API_KEY")
    if not api_key:
        print("[JoshLP] RIOT_API_KEY not set; skipping.")
        return

    state = _load_josh_soloq_state()
    last_lp = state.get("last_lp")
    last_tier = state.get("last_tier")

    try:
        async with aiohttp.ClientSession() as session:
            lp, tier = await _get_josh_soloq_lp_and_tier(session, api_key)

        # Only update when LP changes (as requested).
        # (If you also want tier changes to update, change this condition to: if lp != last_lp or tier != last_tier)
        if lp == last_lp:
            print(f"[JoshLP] No LP change (still {lp}).")
            return

        guild = bot.get_guild(JOSH_GUILD_ID)
        if guild is None:
            guild = await bot.fetch_guild(JOSH_GUILD_ID)

        member = guild.get_member(JOSH_USER_ID)
        if member is None:
            member = await guild.fetch_member(JOSH_USER_ID)

        new_nick = _format_josh_nick(lp, tier)

        # Avoid unnecessary edit if nick already matches
        if member.nick == new_nick:
            _save_josh_soloq_state(lp, tier)
            print(f"[JoshLP] Nick already '{new_nick}', state updated.")
            return

        await member.edit(nick=new_nick, reason="Josh SoloQ LP update")
        _save_josh_soloq_state(lp, tier)
        print(f"[JoshLP] Updated nick -> {new_nick} (was LP={last_lp}, now LP={lp})")

    except discord.Forbidden:
        print("[JoshLP] Forbidden: Bot lacks permission / role hierarchy to change nickname.")
    except discord.HTTPException as e:
        print(f"[JoshLP] Discord HTTPException: {e}")
    except Exception as e:
        print(f"[JoshLP] Error in loop: {e}")


@josh_soloq_lp_nickname_loop.before_loop
async def _before_josh_soloq_lp_nickname_loop():
    await bot.wait_until_ready()


@tasks.loop(minutes=30)
async def opgg_cache_refresh_loop():
    # Best-effort periodic refresh of OP.GG cache, then update any persistent leaderboard posts demostrar.
    await refresh_opgg_flex_cache_best_effort(reason="loop_30m")
    await _update_all_persistent_flex_messages(bot, reason="loop_30m")

@opgg_cache_refresh_loop.before_loop
async def _before_opgg_cache_refresh_loop():
    await bot.wait_until_ready()

@bot.event
async def on_ready():
    global _ready_synced
    if _ready_synced:
        return
    _ready_synced = True

    print(f'Logged in as {bot.user}')

    # register your cogs
    await bot.add_cog(SummaryCog(bot))
    await bot.add_cog(GeneralCog(bot))
    if not TEST_MODE:
        await bot.add_cog(AdminRollCog(bot))
    await bot.add_cog(ShopCog(bot))
    await bot.add_cog(RPSCog(bot))
    await bot.add_cog(CustomsCog(bot))

    if not opgg_cache_refresh_loop.is_running():
        opgg_cache_refresh_loop.start()

    if not josh_soloq_lp_nickname_loop.is_running():
        josh_soloq_lp_nickname_loop.start()
    

    guild_ids = [
        1086751625324003369,
        753949534387961877,
        1287144786452680744,
    ]

    for gid in guild_ids:
        guild = discord.Object(id=gid)
        # Remove this: bot.tree.clear_commands(guild=guild)

        # Copy all global commands (those you defined in cogs) into this guild:
        bot.tree.copy_global_to(guild=guild)

        # Now sync so they appear instantly in that guild:
        await bot.tree.sync(guild=guild)
        print(f"Synced to guild {gid}")

    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="Big Business"
        )
    )
    await load_messages()
    await load_balances()
    await load_daily_cooldowns()
    await load_lenny_stats()

    # NEW: load ignore & spam persistence
    await load_ignore_and_spam_state()

    await load_disable_state()

    if not os.path.exists(MESSAGES_FILE):
        print('No existing messages file found. Initiating message collection...')
        await collect_user_messages()
    print('Bot is ready.')

@bot.event
async def on_message(message: discord.Message):
    # Ignore the bot’s own messages
    if message.author.id == bot.user.id:
        return
    # Track "lenny/lennert" usage for Jeff (persistent)
    await record_lenny_if_needed(message)

    # HARD IGNORE: if user is in ignore list, ignore EVERY message (no processing, no responses)
    if is_ignored(message.author.id):
        return
    # Global disable switch — block all on_message handling for everyone
    if is_globally_disabled():
        return
    # --- stochastic fun: tiny chance to reply or react to ANY message ---
    if message.channel.id == 753959443263389737:
        if random.random() < 0.0007:
            print("REPLY ROLLED")
            async with message.channel.typing():
                reply_txt = await generate_response(message.content, asker_mention=message.author.mention)
            await message.reply(reply_txt)
        if random.random() < 0.005:
            try:
                print("NEW REACTION ROLLED")
                await message.add_reaction(random.choice(["😂","🔥","👍","👀","😮","💀","👏","🤔","😭","😈","🙄","😎","😅","🫡","🫠","💯","🗿"]))
            except Exception:
                pass

    # Special user “Lenny” response (kept)
    if message.author.id == SPECIAL_USER_ID and random.randint(1, SPECIAL_USER_RESPONSE_CHANCE) == 2:
        await message.reply(SPECIAL_USER_RESPONSE)

    # Prefix command? let commands extension handle it
    if message.content.startswith(bot.command_prefix):
        await bot.process_commands(message)
        return
    # Determine if this counts as the bot being asked something
    is_mention = bot.user in message.mentions
    is_reply_to_bot = (
        message.reference
        and isinstance(message.reference.resolved, discord.Message)
        and message.reference.resolved.author.id == bot.user.id
    )
    if not (is_mention or is_reply_to_bot):
        return
    # SPAM CHECK: if punished, reply with "fuck u" and bail
    if await record_ask_and_check_punish(message.author.id):
        # Try to react to the user's most recent message in this channel
        try:
            async for m in message.channel.history(limit=20):
                if m.author.id == message.author.id:
                    await m.add_reaction(random.choice(["😂","🔥","👍","👀","😮","💀","👏","🤔","😭","😈","🙄","😎","😅","🫡","🫠","💯","🗿"]))  # EMOTES is your list at the top
                    break
        except Exception:
            pass
        return
    # Extract optional inline context window e.g. "(2hrs)" at the start
    def parse_window(text: str):
        """
        Recognizes '(2h)', '(30m)', '(3d)' at the start of the message.
        If none given, defaults to 2 hours.
        Returns (timedelta, remaining_text).
        """
        m = re.match(r"^\s*\((\d+)\s*(hrs?|h|mins?|m|days?|d)\)\s*", text, flags=re.IGNORECASE)
        if m:
            qty = int(m.group(1))
            unit = m.group(2).lower()
            if unit in ("h", "hr", "hrs"):
                delta = timedelta(hours=qty)
            elif unit in ("m", "min", "mins"):
                delta = timedelta(minutes=qty)
            else:
                delta = timedelta(days=qty)
            return delta, text[m.end():].strip()
        # DEFAULT: 2 hours when not provided
        return timedelta(hours=2), text.strip()

    stripped = (
        message.content
        .replace(f"<@!{bot.user.id}>", "")
        .replace(f"<@{bot.user.id}>", "")
    )

    # Normalize mentions in the user's message
    stripped = normalize_visible_ats(stripped)

    # Always returns a delta (default 2h) + the cleaned message to answer
    time_window, user_query = parse_window(stripped)

    # Build contexts
    reply_chain = await build_reply_chain(message, max_depth=None)  # or e.g. 25
    recent_block = await build_recent_context_block(message.channel, time_window)
    chain_block  = format_reply_chain_block(reply_chain)

    context_header = (
        "You are Jeff Bot in a Discord server.\n"
        "Use context blocks for background only; do not echo them. "
        "When referring to users, write plain '@Name' text (no ID pings). "
        "Answer the USER_MESSAGE helpfully and concisely.\n"
    )

    parts = [context_header]
    if recent_block:
        parts.append(recent_block)
    if chain_block:
        parts.append(chain_block)
    parts.append("USER_MESSAGE_START\n" + (user_query or "").strip() + "\nUSER_MESSAGE_END")

    composed = "\n".join(parts)

    async with message.channel.typing():
        reply_text = await generate_response(composed, asker_mention=message.author.mention)

    reply_text = normalize_visible_ats(reply_text)

    await message.reply(reply_text)

    # add income for normal user messages
    if not message.author.bot:
        await add_income(str(message.author.id), 5)


daily_cooldowns = {}

# Load daily cooldowns
async def load_daily_cooldowns():
    global daily_cooldowns
    if os.path.exists(DAILY_COOLDOWN_FILE):
        async with aiofiles.open(DAILY_COOLDOWN_FILE, 'r') as f:
            daily_cooldowns = json.loads(await f.read())
    else:
        daily_cooldowns = {}

async def save_daily_cooldowns():
    async with aiofiles.open(DAILY_COOLDOWN_FILE, 'w') as f:
        await f.write(json.dumps(daily_cooldowns))

@bot.event
async def on_raw_reaction_add(payload):
    # ignore the bot itself
    if payload.user_id == bot.user.id:
        return
    # limit to your channel(s). Option A: single channel id
    if payload.channel_id != 753959443263389737:
        return
    # roll the same 5% chance as before
    if random.random() >= 0.05:
        return
    print("REACTION ROLLED")
    # fetch channel/message because raw events don't include full objects
    channel = bot.get_channel(payload.channel_id) or await bot.fetch_channel(payload.channel_id)
    try:
        message = await channel.fetch_message(payload.message_id)
    except Exception:
        return  # can't fetch (no perms / deleted / etc.)

    # add the SAME emoji that was just added
    emoji = str(payload.emoji)  # works for unicode; for custom, PartialEmoji -> str is fine
    try:
        await message.add_reaction(emoji)
    except Exception:
        pass  # ignore failures (permissions, invalid emoji, etc.)

@bot.command()
async def ask(ctx, *, question: str):
    # HARD IGNORE: don't process if user is ignored
    if is_ignored(ctx.author.id):
        return
    # SPAM CHECK
    if await record_ask_and_check_punish(ctx.author.id):
        try:
            await ctx.message.add_reaction(random.choice(["😂","🔥","👍","👀","😮","💀","👏","🤔","😭","😈","🙄","😎","😅","🫡","🫠","💯","🗿"]))  # react to the user's command message
        except Exception:
            pass
        return
    def parse_window(text: str):
        m = re.match(r"^\s*\((\d+)\s*(hrs?|h|mins?|m|days?|d)\)\s*", text, flags=re.IGNORECASE)
        if not m:
            return None, text
        qty = int(m.group(1))
        unit = m.group(2).lower()
        if unit in ("h", "hr", "hrs"):
            delta = timedelta(hours=qty)
        elif unit in ("m", "min", "mins"):
            delta = timedelta(minutes=qty)
        elif unit in ("d", "day", "days"):
            delta = timedelta(days=qty)
        else:
            delta = None
        return delta, text[m.end():].strip()

    delta, core_q = parse_window(question)
    context_lines = []
    if delta:
        since = discord.utils.utcnow() - delta
        async for m in ctx.channel.history(limit=1000, after=since, before=discord.utils.utcnow(), oldest_first=True):
            if m.content.strip():
                context_lines.append(f"[{m.author.display_name}] ⟂ {m.content.strip()}")

    prompt = core_q if not context_lines else "Context follows:\n" + "\n".join(context_lines) + "\n\n" + core_q
    async with ctx.typing():
        reply = await generate_response(prompt, asker_mention=ctx.author.mention)
    reply = normalize_visible_ats(reply)
    await ctx.send(reply)

def split_text(text, max_length=2000):
    """Split text into chunks that are at most `max_length` characters without breaking structure."""
    paragraphs = text.split("\n\n")  # Split by paragraphs for logical grouping
    chunks = []
    current_chunk = ""

    for paragraph in paragraphs:
        if len(current_chunk) + len(paragraph) + 2 > max_length:  # +2 for "\n\n"
            chunks.append(current_chunk.strip())
            current_chunk = paragraph
        else:
            current_chunk += ("\n\n" if current_chunk else "") + paragraph

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks



@bot.command()
async def summary(ctx, *, time_frame: str = "2hrs"):
    try:
        await ctx.send(f"Generating summary for `{time_frame}`...", delete_after=10)
        now = datetime.now(timezone.utc)

        # Parse timeframe without any async work
        def parse_tf(tf: str):
            tf = tf.strip().lower()
            if " to " in tf:
                left, right = [s.strip() for s in tf.split(" to ", 1)]
                def parse_dt(s):
                    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
                        try:
                            return datetime.strptime(s, fmt).replace(tzinfo=LOCAL_TIMEZONE).astimezone(timezone.utc)
                        except ValueError:
                            continue
                    return None
                start = parse_dt(left)
                end = now if right == "now" else parse_dt(right)
                return ("range", start, end)

            if tf.endswith(("hrs","hr","h")):
                n = int(re.sub(r"\D", "", tf))
                return ("delta", now - timedelta(hours=n), now)
            if tf.endswith(("mins","min","m")):
                n = int(re.sub(r"\D", "", tf))
                return ("delta", now - timedelta(minutes=n), now)
            if tf.endswith(("days","day","d")):
                n = int(re.sub(r"\D", "", tf))
                return ("delta", now - timedelta(days=n), now)
            if tf == "msg":
                return ("msg", None, now)

            # default
            return ("delta", now - timedelta(hours=2), now)

        mode, start_time, end_time = parse_tf(time_frame)

        # Handle the 'msg' case with async history lookup OUTSIDE the parser
        if mode == "msg":
            # Use the timestamp of the last message in channel before the invoking message
            last_msg = None
            async for m in ctx.channel.history(limit=1, before=ctx.message, oldest_first=False):
                last_msg = m
                break
            if last_msg is not None:
                start_time = last_msg.created_at
            else:
                # Fallback to 2 hours if nothing found
                start_time = now - timedelta(hours=2)
            end_time = now

        # Validate range
        if not start_time or not end_time:
            await ctx.reply("Invalid date range. Try '2025-09-10 18:00 to now', '2hrs', '30min', or 'msg'.")
            return
        # Collect messages and render with safe separators
        msgs = []
        async for m in ctx.channel.history(limit=2000, after=start_time, before=end_time, oldest_first=True):
            if m.content.strip():
                msgs.append(f"[{m.author.display_name}] ⟂ {m.content.strip()}")

        if not msgs:
            await ctx.reply("No messages found in that window.")
            return
        convo = "\n".join(msgs)
        system_prompt = "a"

        # Single-topic, compact summary prompt
        summary_prompt = (
            "Summarize the chat log below with these rules:\n"
            "1) Identify topics and select the *dominant* one by message share; summarize ONLY that topic.\n"
            "2) Attribute quotes precisely using the bracketed name format like [Alice] and do NOT merge speaker tags.\n"
            "3) Keep it dense and readable with minimal whitespace. Aim for ~1 Discord message, max 2.\n"
            "4) Format:\n"
            "   • Topic\n"
            "   • Timeline (1–3 bullets)\n"
            "   • Positions by participant (name: stance; 1 line each)\n"
            "   • Notable quotes (2–5 short quotes, with [Name])\n"
            "   • Outcome/next steps (if any)\n\n"
            f"CHAT LOG START\n{convo}\nCHAT LOG END"
        )

        try:
            raw = await generate_response(summary_prompt, system_prompt=system_prompt, allow_mentions=False)
            for chunk in split_text(raw, max_length=1900):
                await ctx.send(chunk)
        except openai.error.RateLimitError:
            await ctx.reply("Rate limit reached. Try again later.")
        except Exception:
            await ctx.reply("An error occurred while generating the summary. Try again later.")
    except Exception as e:
        print(f"Unexpected error in !!summary: {e}")



class GivePoopModal(discord.ui.Modal, title="Give Poop Role"):
    target_input = discord.ui.TextInput(label="Enter target user ID", placeholder="(Find user ID by right-clicking user)")
    
    def __init__(self, shop_user: discord.Member):
        super().__init__()
        self.shop_user = shop_user

    async def on_submit(self, interaction: discord.Interaction):
        content = self.target_input.value.strip()
        target_member = None
        if content.isdigit():
            target_member = interaction.guild.get_member(int(content))
        else:
            match = re.search(r'\d+', content)
            if match:
                target_member = interaction.guild.get_member(int(match.group()))
        if target_member is None:
            await interaction.response.send_message("Could not find that member.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        success = await deduct_points(str(self.shop_user.id), SHOP_COST)
        if not success:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        poop_role = interaction.guild.get_role(1320932897351536731)
        try:
            await target_member.add_roles(poop_role, reason=f"Shop purchase: given by {self.shop_user}")
            await interaction.response.send_message(f"Poop role given to {target_member.mention}!", ephemeral=True)
            await interaction.channel.send(f"{self.shop_user.mention} just pooped on {target_member.mention}!")
        except Exception as e:
            await interaction.response.send_message(f"Error: {e}", ephemeral=True)

class ChangeColorModal(discord.ui.Modal, title="Change Color"):
    color_input = discord.ui.TextInput(label="Enter a hex color (e.g. #FF0000)", placeholder="#FF0000")
    
    def __init__(self, shop_user: discord.Member):
        super().__init__()
        self.shop_user = shop_user

    async def on_submit(self, interaction: discord.Interaction):
        content = self.color_input.value.strip().lstrip('#')
        try:
            color_int = int(content, 16)
        except ValueError:
            await interaction.response.send_message("Invalid color format.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        success = await deduct_points(str(self.shop_user.id), SHOP_COST)
        if not success:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        # Use a naming convention for the role; here we simply use the color value as part of the name.
        role_name = f"Color {content}"
        existing_role = discord.utils.get(interaction.guild.roles, name=role_name)
        try:
            if existing_role is None:
                new_role = await interaction.guild.create_role(
                    name=role_name,
                    color=discord.Color(color_int),
                    reason="Shop purchase: change color",
                )
                role_to_use = new_role
                await self.shop_user.add_roles(new_role, reason="Shop purchase: change color")
            else:
                await existing_role.edit(color=discord.Color(color_int), reason="Shop purchase: update color")
                role_to_use = existing_role
                if existing_role not in self.shop_user.roles:
                    await self.shop_user.add_roles(existing_role, reason="Shop purchase: change color")
            # Adjust the role's position to be immediately below the bot’s top role.
            bot_top_role = interaction.guild.me.top_role
            desired_position = bot_top_role.position - 2
            if desired_position < 1:
                desired_position = 2
            await interaction.guild.edit_role_positions({role_to_use: desired_position})
            await interaction.response.send_message("Your name color has been updated, Mon.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error updating color role: {e}", ephemeral=True)

class TimeoutModal(discord.ui.Modal, title="Timeout User"):
    target_input = discord.ui.TextInput(label="Enter target user ID or mention", placeholder="User ID or mention")
    
    def __init__(self, shop_user: discord.Member):
        super().__init__()
        self.shop_user = shop_user

    async def on_submit(self, interaction: discord.Interaction):
        content = self.target_input.value.strip()
        target_member = None
        if content.isdigit():
            target_member = interaction.guild.get_member(int(content))
        else:
            match = re.search(r'\d+', content)
            if match:
                target_member = interaction.guild.get_member(int(match.group()))
        if target_member is None:
            await interaction.response.send_message("Could not find that member.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        success = await deduct_points(str(self.shop_user.id), SHOP_COST)
        if not success:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        try:
            duration = timedelta(hours=1)
            # Using the standard timeout method – adjust if necessary depending on your library's version.
            await target_member.timeout(duration, reason=f"Shop purchase: timed out by {self.shop_user}")
            await interaction.response.send_message(f"{target_member.mention} has been timed out for 1 hour.", ephemeral=True)
            await interaction.channel.send(f"{self.shop_user.mention} just timed out {target_member.mention}!")
        except discord.Forbidden as e:
            # Refund the purchase if the target is a mod/admin (or otherwise cannot be timed out)
            await add_income(str(self.shop_user.id), SHOP_COST)
            await interaction.response.send_message("Error, cannot timeout a mod", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"Error timing out user: {e}", ephemeral=True)

# -------------------------------
# Define the interactive view for the shop menu
# -------------------------------

class ShopView(discord.ui.View):
    def __init__(self, shop_user: discord.Member):
        super().__init__(timeout=180)
        self.shop_user = shop_user

    # Remove Poop button – instant action.
    @discord.ui.button(label="Remove Poop", style=discord.ButtonStyle.primary, custom_id="shop_remove_poop")
    async def remove_poop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.shop_user.id:
            await interaction.response.send_message("This is not your shop session.", ephemeral=True)
            return
        poop_role = interaction.guild.get_role(1320932897351536731)
        if poop_role not in self.shop_user.roles:
            await interaction.response.send_message("You don't have the poop role.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        success = await deduct_points(str(self.shop_user.id), SHOP_COST)
        if not success:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        try:
            await self.shop_user.remove_roles(poop_role, reason="Shop purchase: remove poop role")
            await interaction.response.send_message("Poop role removed from you. Enjoy your fresh start!", ephemeral=True)
            await interaction.channel.send(f"{self.shop_user.mention} just removed their poop role!")
        except Exception as e:
            await interaction.response.send_message(f"Error removing poop role: {e}", ephemeral=True)

    # Give Poop button – requires a modal.
    @discord.ui.button(label="Give Poop", style=discord.ButtonStyle.primary, custom_id="shop_give_poop")
    async def give_poop_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.shop_user.id:
            await interaction.response.send_message("This is not your shop session.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        await interaction.response.send_modal(GivePoopModal(self.shop_user))

    # Change Color button – requires a modal.
    @discord.ui.button(label="Change Color", style=discord.ButtonStyle.primary, custom_id="shop_change_color")
    async def change_color_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.shop_user.id:
            await interaction.response.send_message("This is not your shop session.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        await interaction.response.send_modal(ChangeColorModal(self.shop_user))

    # Timeout button – requires a modal.
    @discord.ui.button(label="Timeout Member", style=discord.ButtonStyle.primary, custom_id="shop_timeout")
    async def timeout_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != self.shop_user.id:
            await interaction.response.send_message("This is not your shop session.", ephemeral=True)
            return
        if user_balances.get(str(self.shop_user.id), 0) < SHOP_COST:
            await interaction.response.send_message("Insufficient funds.", ephemeral=True)
            return
        await interaction.response.send_modal(TimeoutModal(self.shop_user))

# -------------------------------
# Shop Cog using the interactive shop menu
# -------------------------------

class ShopCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot


    # Slash command version of the shop
    @app_commands.command(name="shop", description="Open the shop and spend your coins!")
    async def shop_menu(self, interaction: discord.Interaction):
        member = interaction.user
        bal = user_balances.get(str(member.id), 0)
        embed = discord.Embed(
            title="BUSINESS SHOP",
            description=f"Your current balance: **{bal} coins**\n\nSelect an option below to purchase an item.\nEach item costs **{SHOP_COST}** coins.",
            color=discord.Color.green()
        )
        await interaction.response.send_message(embed=embed, view=ShopView(member), ephemeral=True)


    # Traditional command version using your command prefix, e.g. !!shop.
    @commands.command(name="shop")
    async def shop_command(self, ctx: commands.Context):
        member = ctx.author
        bal = user_balances.get(str(member.id), 0)
        embed = discord.Embed(
            title="BUSINESS SHOP",
            description=f"Your current balance: **{bal} coins**\n\nSelect an option below to purchase an item.\nEach item costs **{SHOP_COST}** coins.",
            color=discord.Color.green()
        )
        # Send the message in the channel; note that ephemeral messages are only supported for interactions.
        await ctx.send(embed=embed, view=ShopView(member))


class GeneralCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

        # Start the weekly Flex leaderboard task (auto-post)
        self.flex_weekly_leaderboard_task.start()

        # Keep OP.GG cache warm so slash commands don't depend on a live scrape.
        # Runs every 30 minutes (headless; no browser window).
        self.flex_opgg_cache_refresh_task.start()

    def cog_unload(self):
        # Stop background tasks cleanly
        try:
            self.flex_weekly_leaderboard_task.cancel()
        except Exception:
            pass
        try:
            self.flex_opgg_cache_refresh_task.cancel()
        except Exception:
            pass

    @app_commands.command(name="ask", description="Ask the bot a question (optional: '(2hrs) question...' for context)")
    async def ask(self, interaction: Interaction, question: str):
        # HARD IGNORE: if user is ignored, completely ignore (no response)
        if is_ignored(interaction.user.id):
            return
        # SPAM CHECK
        if await record_ask_and_check_punish(interaction.user.id):
            # respond with "fuck u" (not ephemeral so it's visible like normal answers)
            await interaction.response.send_message("fuck u")
            return
        await interaction.response.defer()  # shows typing / “thinking…”

        def parse_window(text: str):
            m = re.match(r"^\s*\((\d+)\s*(hrs?|h|mins?|m|days?|d)\)\s*", text, flags=re.IGNORECASE)
            if not m:
                return None, text
            qty = int(m.group(1))
            unit = m.group(2).lower()
            if unit in ("h", "hr", "hrs"):
                delta = timedelta(hours=qty)
            elif unit in ("m", "min", "mins"):
                delta = timedelta(minutes=qty)
            elif unit in ("d", "day", "days"):
                delta = timedelta(days=qty)
            else:
                delta = None
            return delta, text[m.end():].strip()

        delta, core_q = parse_window(question)
        context_lines = []
        if delta:
            since = discord.utils.utcnow() - delta
            async for m in interaction.channel.history(limit=1000, after=since, before=discord.utils.utcnow(), oldest_first=True):
                if m.content.strip():
                    context_lines.append(f"[{m.author.display_name}] ⟂ {m.content.strip()}")

        prompt = core_q if not context_lines else "Context follows:\n" + "\n".join(context_lines) + "\n\n" + core_q
        response = await generate_response(prompt, asker_mention=interaction.user.mention)
        response = normalize_visible_ats(response)
        await interaction.followup.send(response)

    # ====== /disable and /enable from parky blake or garret ======
    @app_commands.command(
        name="disable",
        description="Disable the bot from processing messages (default 1h)."
    )
    @app_commands.describe(duration="Time like '1h' or '30m' (default 1h)")
    async def disable(self, interaction: Interaction, duration: str = "1h"):
        if interaction.user.id not in ALLOWED_DISABLE_USERS:
            await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)
            return
        seconds = _parse_duration_to_seconds(duration)
        await set_disabled_for(seconds)
        mins, secs = divmod(seconds, 60)
        hrs, mins = divmod(mins, 60)
        until_ts = datetime.fromtimestamp(_disable_state["until"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        pretty = (f"{hrs}h {mins}m" if hrs else f"{mins}m {secs}s").strip()
        await interaction.response.send_message(
            f"🛑 Bot on_message disabled for **{pretty}**.\nIt will auto-re-enable at **{until_ts}**.",
            ephemeral=True
        )

    @app_commands.command(
        name="enable",
        description="Re-enable bot message processing immediately."
    )
    async def enable(self, interaction: Interaction):
        if interaction.user.id not in ALLOWED_DISABLE_USERS:
            await interaction.response.send_message("You don’t have permission to use this.", ephemeral=True)
            return
        await clear_disabled()
        await interaction.response.send_message("✅ On_message processing re-enabled.", ephemeral=True)
        

    
    # ====== Voice controls: /join and /leave ======
    @app_commands.command(name="join", description="Join a voice channel by channel ID")
    @app_commands.describe(channel_id="Voice channel ID (numbers)")
    async def join(self, interaction: discord.Interaction, channel_id: str):
        # Must be used in a guild
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)

        # Parse ID
        if not channel_id.isdigit():
            await interaction.followup.send("Channel ID must be numeric.", ephemeral=True)
            return
        cid = int(channel_id)

        # Fetch channel
        channel = interaction.guild.get_channel(cid)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(cid)
            except Exception as e:
                await interaction.followup.send(f"Couldn't find that channel: {e}", ephemeral=True)
                return
        # Validate it's a voice channel (or stage)
        if not isinstance(channel, (discord.VoiceChannel, discord.StageChannel)):
            await interaction.followup.send("That channel ID is not a voice/stage channel.", ephemeral=True)
            return
        # Permissions check (connect)
        me = interaction.guild.me or interaction.guild.get_member(self.bot.user.id)
        perms = channel.permissions_for(me)
        if not perms.connect:
            await interaction.followup.send("I don't have permission to CONNECT to that channel.", ephemeral=True)
            return
        # Connect / move
        vc = interaction.guild.voice_client
        try:
            if vc and vc.is_connected():
                if vc.channel.id != channel.id:
                    await vc.move_to(channel)
                await interaction.followup.send(f"✅ Connected to **{channel.name}**.", ephemeral=True)
                return
            await channel.connect(timeout=20, reconnect=True)
            await interaction.followup.send(f"✅ Connected to **{channel.name}**.", ephemeral=True)

        except Exception as e:
            await interaction.followup.send(f"❌ Failed to join: `{e}`", ephemeral=True)

    @app_commands.command(name="leave", description="Leave the current voice channel")
    async def leave(self, interaction: discord.Interaction):
        if interaction.guild is None:
            await interaction.response.send_message("This only works in a server.", ephemeral=True)
            return
        vc = interaction.guild.voice_client
        if not vc or not vc.is_connected():
            await interaction.response.send_message("I'm not connected to any voice channel.", ephemeral=True)
            return
        try:
            await vc.disconnect(force=True)
            await interaction.response.send_message("👋 Left the voice channel.", ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Failed to leave: `{e}`", ephemeral=True)




    # ====== /ignore & /enter (slash only) with 24h cooldown ======
    @app_commands.command(name="ignore", description="Opt out: the bot will ignore you and won't @ your ID (24h cooldown).")
    async def ignore(self, interaction: Interaction):
        uid = interaction.user.id
        rem = ignore_cooldown_remaining(uid)
        if rem > 0:
            hrs = int(rem // 3600); mins = int((rem % 3600) // 60)
            await interaction.response.send_message(
                f"You can change this again in {hrs}h {mins}m.", ephemeral=True
            )
            return
        if is_ignored(uid):
            await interaction.response.send_message("You're already ignored.", ephemeral=True)
            return
        await set_ignored(uid, True)
        await interaction.response.send_message("✅ You are now ignored. I won't process your messages or @ your ID (I'll use @Name).", ephemeral=True)

    @app_commands.command(name="enter", description="Opt back in: the bot will respond and @ your ID again (24h cooldown).")
    async def enter(self, interaction: Interaction):
        uid = interaction.user.id
        rem = ignore_cooldown_remaining(uid)
        if rem > 0:
            hrs = int(rem // 3600); mins = int((rem % 3600) // 60)
            await interaction.response.send_message(
                f"You can change this again in {hrs}h {mins}m.", ephemeral=True
            )
            return
        if not is_ignored(uid):
            await interaction.response.send_message("You're already entered (not ignored).", ephemeral=True)
            return
        await set_ignored(uid, False)
        await interaction.response.send_message("✅ You're back in. I'll respond and @ your ID again.", ephemeral=True)

    @app_commands.command(name="daily", description="Claim your daily coins (once every 18 hours)")
    async def daily(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        current_time = time.time()
        cooldown_time = 18 * 3600  # 18 hours in seconds
        if user_id in daily_cooldowns:
            time_elapsed = current_time - daily_cooldowns[user_id]
            if time_elapsed < cooldown_time:
                time_left = cooldown_time - time_elapsed
                hours, remainder = divmod(int(time_left), 3600)
                minutes, seconds = divmod(remainder, 60)
                await interaction.response.send_message(
                    f"You need to wait {hours} hours, {minutes} minutes, and {seconds} seconds before claiming your daily reward again, Mon."
                )
                return
        daily_amount = 500
        await add_income(user_id, daily_amount)
        daily_cooldowns[user_id] = current_time
        await save_daily_cooldowns()
        await interaction.response.send_message(
            f"Another day another dollar. You've received your daily {daily_amount} coins, Mon!"
        )

    @app_commands.command(name="balance", description="Check your current coin balance")
    async def balance(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        bal = user_balances.get(user_id, 0)
        await interaction.response.send_message(f"Your current balance is {bal} coins, Mon. Keep making those big business moves!")

    @app_commands.command(name="leaderboard", description="View the top 10 users with the most coins")
    async def leaderboard(self, interaction: discord.Interaction):
        sorted_balances = sorted(user_balances.items(), key=lambda x: x[1], reverse=True)
        leaderboard_text = "🏆 BIGGEST BUSINESS MAKERS 🏆\n\n"
        for i, (uid, bal) in enumerate(sorted_balances[:10], 1):
            user = await self.bot.fetch_user(int(uid))
            leaderboard_text += f"{i}. {user.name}: {bal} coins\n"
        await interaction.response.send_message(leaderboard_text)

    @app_commands.command(name="bj", description="Play a game of Blackjack")
    @app_commands.describe(bet_amount="Specify your bet amount (e.g., 'all', 'half', '100', or '50%')")
    async def bj(self, interaction: discord.Interaction, bet_amount: str):
        await interaction.response.defer()
        user_id = str(interaction.user.id)
        if user_id in active_games:
            await interaction.followup.send("You already have an active game, Mon. Please finish it before starting a new one.")
            return
        balance = user_balances.get(user_id, 0)
        if bet_amount is None:
            await interaction.followup.send(
                f"Please specify a bet amount. Your current balance is **{balance} coins**.\nUsage: `/bj <amount>`", ephemeral=True
            )
            return
        bet = parse_bet_amount(bet_amount, balance)
        if bet is None or bet <= 99 or bet > balance:
            await interaction.followup.send(
                f"Invalid bet amount. Please bet an amount at least 100 coins. Your current balance is **{balance} coins**. You can get more coins by using /slots",
                ephemeral=True
            )
            return
        active_games.add(user_id)
        try:
            deck = Deck()
            player_hand = [deck.draw(), deck.draw()]
            dealer_hand = [deck.draw(), deck.draw()]

            dealer_value, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)
            player_value, player_display = calculate_hand_value(player_hand)
            dealer_blackjack = dealer_value == 21
            player_blackjack = player_value == 21

            if dealer_blackjack or player_blackjack:
                if dealer_blackjack and player_blackjack:
                    result = "tie"
                    result_message = f"It's a tie. Your bet is returned. You now have **{balance} coins**."
                elif player_blackjack:
                    result = "player"
                    payout = math.ceil(bet * 1.5)
                    user_balances[user_id] += payout
                    result_message = f"**MONKEY MONKEY MONKEY!** You win **{payout} coins**! You now have **{user_balances[user_id]} coins**."
                else:
                    result = "dealer"
                    user_balances[user_id] -= bet
                    result_message = f"Dealer has Blackjack. You lost **{bet} coins**, Mon. You now have **{user_balances[user_id]} coins**."
                await save_balances()
                embed = create_game_embed(interaction.user, player_hand, dealer_hand, bet, show_dealer=True, result=result.capitalize())
                embed.add_field(name="Result", value=result_message, inline=False)
                message = await interaction.followup.send(embed=embed)
                # Adding reaction is optional; note that interactions may require fetching the message object
                return
            view = BlackjackView(player_hand, dealer_hand, bet)
            embed = create_game_embed(interaction.user, player_hand, dealer_hand, bet)
            message = await interaction.followup.send(embed=embed, view=view)

            while not view.game_over:
                try:
                    button_interaction = await self.bot.wait_for(
                        'interaction',
                        timeout=60.0,
                        check=lambda i: i.user.id == interaction.user.id and i.message.id == message.id
                    )
                    action = button_interaction.data['custom_id']
                    if action == 'hit':
                        player_hand.append(deck.draw())
                        player_value, player_display = calculate_hand_value(player_hand)
                        if player_value >= 21:
                            view.game_over = True
                    elif action == 'stand':
                        view.game_over = True
                    elif action == 'double':
                        if balance >= bet * 2:
                            bet *= 2
                            player_hand.append(deck.draw())
                            player_value, player_display = calculate_hand_value(player_hand)
                            view.game_over = True
                        else:
                            await button_interaction.response.send_message("Not enough balance to double down.", ephemeral=True)
                            continue
                    view.update_buttons(player_value)
                    await message.edit(embed=create_game_embed(interaction.user, player_hand, dealer_hand, bet), view=view)
                    if view.game_over:
                        break
                    await button_interaction.response.defer()
                except asyncio.TimeoutError:
                    await message.edit(content="You took too long to respond. Standing by default.", view=None)
                    view.game_over = True

            # Dealer's turn
            dealer_value, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)
            while dealer_value < 17:
                dealer_hand.append(deck.draw())
                dealer_value, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)
            player_value, player_display = calculate_hand_value(player_hand)
            result = determine_winner(player_value, dealer_value)
            if result == 'player':
                user_balances[user_id] += bet
                result_message = f"Congratulations! You win **{bet} coins**! You now have **{user_balances[user_id]} coins**."
            elif result == 'dealer':
                user_balances[user_id] -= bet
                result_message = f"Sorry, you lost **{bet} coins**, Mon. You now have **{user_balances[user_id]} coins**."
            else:
                result_message = f"It's a tie. Your bet is returned. You now have **{user_balances[user_id]} coins**."
            await save_balances()
            final_embed = create_game_embed(interaction.user, player_hand, dealer_hand, bet, True, result)
            final_embed.add_field(name="Result", value=result_message, inline=False)
            await message.edit(embed=final_embed, view=None)
        finally:
            active_games.remove(user_id)

    @app_commands.command(name="slots", description="Try your luck at the slot machine (only if you have less than 100 coins)")
    async def slots(self, interaction: discord.Interaction):
        user_id = str(interaction.user.id)
        balance = user_balances.get(user_id, 0)
        if balance >= 101:
            await interaction.response.send_message(
                f"Sorry, the slots are only for blue collar workers with less than 100 coins. Your current balance is **{balance} coins**.",
                ephemeral=True
            )
            return
        roll = random.choices(range(1, 1001))[0]
        if roll >= 999:
            win_amount = 10000
        elif roll >= 990:
            win_amount = 1000
        elif roll >= 960:
            win_amount = 600
        elif roll >= 900:
            win_amount = 300
        elif roll >= 600:
            win_amount = 200
        elif roll >= 400:
            win_amount = 150
        elif roll >= 200:
            win_amount = 125
        else:
            win_amount = 100
        await add_income(user_id, win_amount)
        embed = discord.Embed(title="🎰 Slot Machine", color=discord.Color.gold())
        embed.add_field(name=f"**YOU WON {win_amount} COINS!!**", value="Now thats business.", inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="commands", description="Show list of available commands")
    async def commands(self, interaction: discord.Interaction):
        command_list = [
            ("/ask", "Ask the bot a question"),
            ("/daily", "Claim your daily coins (once every 18 hours)"),
            ("/balance", "Check your current coin balance"),
            ("/leaderboard", "View the top 10 users with the most coins"),
            ("/bj [amount]", "Play a game of Blackjack"),
            ("/slots", "Try your luck at the slot machine (only if you have less than 100 coins)"),
            ("/commands", "Show this list of commands"),
            ("/summary", "Generate a summary of recent messages (supporting date/time range)")
        ]
        embed = discord.Embed(title="Here are my business commands, Mon", color=discord.Color.blue())
        for cmd, desc in command_list:
            embed.add_field(name=cmd, value=desc, inline=False)
        await interaction.response.send_message(embed=embed)

    @app_commands.command(name="dm", description="Send a direct message to a user via JeffBot")
    @app_commands.describe(
        target="User to receive the DM (mention or ID)",
        content="The message content to send"
    )
    async def dm(self, interaction: Interaction, target: discord.User, content: str):
        """
        Usage: /dm @SomeUser Hello there!
        You can also pass a raw ID like /dm 123456789012345678 Hi!
        """
        try:
            # Send the DM
            await target.send(content)
            # Confirm in-channel (ephemeral so only the caller sees it)
            await interaction.response.send_message(
                f"✅ Message sent to {target.mention}.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ Failed to send DM: {e}",
                ephemeral=True
            )

    @app_commands.command(
        name="match",
        description="Show Schmort's DPM-style score for his Nth most recent match."
    )
    @app_commands.describe(
        index="Which game? 1 = most recent (default)"
    )
    async def match(
        self,
        interaction: Interaction,
        index: int = 1
    ):
        """
        /match N  → compute DPM-style score for Schmort#bone's Nth last match.
        """
        await interaction.response.defer()

        try:
            match_id, match_data, puuid = await fetch_schmort_match_basic(index)
            dpm_score, breakdown, player = compute_dpm_score(match_data, puuid)
        except Exception as e:
            await interaction.followup.send(
                f"❌ Error while fetching or scoring the match: `{e}`"
            )
            return
        info = match_data.get("info", {})
        queue_id = info.get("queueId")
        queue_name = QUEUE_ID_TO_NAME.get(queue_id, f"Queue {queue_id}")
        game_duration_sec = info.get("gameDuration", 0)
        game_duration_min = game_duration_sec / 60.0 if game_duration_sec else 0.0

        champ_name = player.get("championName", "Unknown")
        role = breakdown.get("role", "UNKNOWN")
        win = breakdown.get("win", False)

        kills = player.get("kills", 0)
        deaths = player.get("deaths", 0)
        assists = player.get("assists", 0)

        color = discord.Color.gold() if win else discord.Color.red()
        result_text = "Victory" if win else "Defeat"

        embed = discord.Embed(
            title=f"DPM score for Schmort ({champ_name})",
            description=(
                f"Match: `{match_id}`\n"
                f"Result: **{result_text}**\n"
                f"Queue: **{queue_name}** (ID {queue_id})\n"
                f"Length: **{game_duration_min:.1f} min**\n\n"
                f"**DPM Score:** **{dpm_score:.2f}** / 100\n"
                f"(raw sum before clamp: {breakdown['raw_total_before_clamp']:.2f})"
            ),
            color=color,
        )

        # Basic KDA + role
        embed.add_field(
            name="KDA / Role",
            value=(
                f"Champion: **{champ_name}**\n"
                f"Role: **{role}**\n"
                f"K/D/A: **{kills}/{deaths}/{assists}**"
            ),
            inline=True,
        )

        g = breakdown["global"]
        t = breakdown["team"]
        rs = breakdown["roleSection"]
        obj = breakdown["objectives"]

        embed.add_field(
            name="Global section",
            value=(
                f"Score: **{g['score']:.2f}**\n"
                f"Kills term: {g['details']['kills']:.2f}\n"
                f"Deaths term: {g['details']['deaths']:.2f}\n"
                f"Assists term: {g['details']['assists']:.2f}\n"
                f"CS/min term: {g['details']['csm']:.2f}\n"
                f"GPM term: {g['details']['goldPerMinute']:.2f}\n"
                f"DPM term: {g['details']['damagePerMinute']:.2f}\n"
                f"Vision/min term: {g['details']['visionScorePerMinute']:.2f}\n"
                f"First blood: {g['details']['firstBlood']:.2f}"
            ),
            inline=False,
        )

        embed.add_field(
            name="Objectives / Team / Role",
            value=(
                f"Objectives score: **{obj['score']:.2f}**\n"
                f"Team score: **{t['score']:.2f}**\n"
                f"Role score: **{rs['score']:.2f}**\n"
                f"Game state: **{breakdown['gameState']['score']:.2f}**"
            ),
            inline=False,
        )

        await interaction.followup.send(embed=embed)


    @app_commands.command(
        name="weekly_flex_leaderboard",
        description="show this week's flex leaderboard for the group."
    )
    @app_commands.describe(refresh='Optional: "1" to refresh data first, blank or "0" to skip')
    async def flex_leaderboard(self, interaction: Interaction, refresh: str | None = None):
        # You can add permission checks here if you want
        await interaction.response.defer(thinking=True)

        # Only refresh if explicitly requested
        if refresh == "1":
            await refresh_opgg_flex_cache_best_effort(reason="command")
        await _update_all_persistent_flex_messages(self.bot, reason="weekly_flex_leaderboard")

        entries, week_start, now = await compute_weekly_flex_leaderboard_from_opgg_cache()
        msg = format_flex_leaderboard(entries, week_start, now)

        # Manual mode: post to the channel where the slash command was used
        await interaction.followup.send(msg)

    # -------------------------------
    # OP.GG cache refresher
    # -------------------------------
    @tasks.loop(minutes=30)
    async def flex_opgg_cache_refresh_task(self):
        await self.bot.wait_until_ready()
        await refresh_opgg_flex_cache_best_effort(reason="loop")

    

    @flex_opgg_cache_refresh_task.before_loop
    async def before_flex_opgg_cache_refresh_task(self):
        await self.bot.wait_until_ready()
        # Prime the cache once at startup so /weekly_flex_leaderboard works immediately.
        await refresh_opgg_flex_cache_best_effort(reason="startup")
        await _update_all_persistent_flex_messages(self.bot, reason="startup")

    # Runs every day at local midnight; only posts on Monday 00:00 local
    # which is effectively "Sunday night at midnight".
    @tasks.loop(time=dtime(hour=0, minute=0, tzinfo=LOCAL_TIMEZONE))
    async def flex_weekly_leaderboard_task(self):
        await self.bot.wait_until_ready()

        now_local = datetime.now(LOCAL_TIMEZONE)
        # Monday is 0; we only post at Monday 00:00
        if now_local.weekday() != 0:
            return
        await refresh_opgg_flex_cache_best_effort(reason="command")
        entries, week_start, now = await compute_weekly_flex_leaderboard_from_opgg_cache()
        msg = format_flex_leaderboard(entries, week_start, now)

        channel = (
            self.bot.get_channel(FLEX_LEADERBOARD_CHANNEL_ID)
            or await self.bot.fetch_channel(FLEX_LEADERBOARD_CHANNEL_ID)
        )
        if channel is None:
            print(f"[FlexLB] Could not find channel {FLEX_LEADERBOARD_CHANNEL_ID}")
            return
        await channel.send(msg)

    @flex_weekly_leaderboard_task.before_loop
    async def before_flex_weekly_leaderboard_task(self):
        await self.bot.wait_until_ready()

    @app_commands.command(
        name="flex_leaderboard_session",
        description="show a temporary flex jeffbot leaderboard for the last session",
    )
    @app_commands.describe(refresh='Optional: "1" to refresh data first, blank or "0" to skip')
    async def flex_leaderboard_recent(self, interaction: discord.Interaction, refresh: str | None = None):
        await interaction.response.defer()

        # Uses the OP.GG cache (auto-refreshed every 30 minutes) so we don't
        # depend on the DPM site being up and we can accumulate >20 matches over time.
        # Only refresh if explicitly requested
        if refresh == "1":
            await refresh_opgg_flex_cache_best_effort(reason="post_leaderboard")
        await _update_all_persistent_flex_messages(self.bot, reason="flex_leaderboard_session")

        entries, start_utc, end_utc = await compute_recent_flex_leaderboard_from_opgg_cache(hours=18)
        msg = format_recent_flex_leaderboard(entries, start_utc, end_utc)
        await interaction.followup.send(msg)


    @app_commands.command(
        name="persistent_weekly_flex",
        description="Post a persistent weekly flex leaderboard (one message, edited in-place).",
    )
    @app_commands.describe(message_id="(Optional) Existing message ID to adopt as the persistent weekly leaderboard.")
    async def persistent_weekly_flex(self, interaction: Interaction, message_id: str | None = None):
        # Ephemeral ack so we don't spam chat; the leaderboard itself is a normal channel message.
        await interaction.response.defer(ephemeral=True, thinking=True)

        if interaction.guild is None or interaction.channel is None:
            await interaction.followup.send("This only works in a server channel.", ephemeral=True)
            return

        entries, week_start, now = await compute_weekly_flex_leaderboard_from_opgg_cache()
        header = _format_last_updated_header(datetime.now(timezone.utc))
        body = format_flex_leaderboard(entries, week_start, now)
        body = body.replace("WEEKLY FLEX LEADERBOARD", "PERSISTENT WEEKLY FLEX LEADERBOARD", 1)
        content = header + "\n" + body

        if message_id:
            try:
                mid = int(str(message_id).strip())
            except Exception:
                await interaction.followup.send("Invalid message_id (must be a numeric Discord message ID).", ephemeral=True)
                return

            try:
                target_msg = await interaction.channel.fetch_message(mid)
            except Exception:
                await interaction.followup.send("Couldn't find that message in this channel.", ephemeral=True)
                return

            try:
                await target_msg.edit(content=content)
            except Exception:
                await interaction.followup.send("I couldn't edit that message (missing perms or it's not editable).", ephemeral=True)
                return

            _set_persistent_pointer(
                "weekly",
                int(interaction.guild.id),
                channel_id=int(interaction.channel.id),
                message_id=int(target_msg.id),
            )
            await interaction.followup.send("✅ Adopted that message as the **persistent weekly** leaderboard.", ephemeral=True)
            return

        msg_obj = await interaction.channel.send(content)
        _set_persistent_pointer(
            "weekly",
            int(interaction.guild.id),
            channel_id=int(interaction.channel.id),
            message_id=int(msg_obj.id),
        )
        await interaction.followup.send("✅ Posted a new **persistent weekly** leaderboard (I’ll keep editing it).", ephemeral=True)


    @app_commands.command(
        name="persistent_session_flex",
        description="Post a persistent session flex leaderboard (one message, edited in-place).",
    )
    @app_commands.describe(message_id="(Optional) Existing message ID to adopt as the persistent session leaderboard.")
    async def persistent_session_flex(self, interaction: discord.Interaction, message_id: str | None = None):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if interaction.guild is None or interaction.channel is None:
            await interaction.followup.send("This only works in a server channel.", ephemeral=True)
            return

        entries, start_utc, end_utc = await compute_recent_flex_leaderboard_from_opgg_cache(hours=18)
        header = _format_last_updated_header(datetime.now(timezone.utc))
        body = format_recent_flex_leaderboard(entries, start_utc, end_utc)
        body = body.replace("SESSION FLEX LEADERBOARD", "PERSISTENT SESSION FLEX LEADERBOARD", 1)
        content = header + "\n" + body

        if message_id:
            try:
                mid = int(str(message_id).strip())
            except Exception:
                await interaction.followup.send("Invalid message_id (must be a numeric Discord message ID).", ephemeral=True)
                return

            try:
                target_msg = await interaction.channel.fetch_message(mid)
            except Exception:
                await interaction.followup.send("Couldn't find that message in this channel.", ephemeral=True)
                return

            try:
                await target_msg.edit(content=content)
            except Exception:
                await interaction.followup.send("I couldn't edit that message (missing perms or it's not editable).", ephemeral=True)
                return

            _set_persistent_pointer(
                "session",
                int(interaction.guild.id),
                channel_id=int(interaction.channel.id),
                message_id=int(target_msg.id),
            )
            await interaction.followup.send("✅ Adopted that message as the **persistent session** leaderboard.", ephemeral=True)
            return

        msg_obj = await interaction.channel.send(content)
        _set_persistent_pointer(
            "session",
            int(interaction.guild.id),
            channel_id=int(interaction.channel.id),
            message_id=int(msg_obj.id),
        )
        await interaction.followup.send("✅ Posted a new **persistent session** leaderboard (I’ll keep editing it).", ephemeral=True)


    @app_commands.command(
        name="refresh_flex",
        description="Refresh flex history, optionally spam refresh until new game",
    )
    @app_commands.describe(wait_for_new="0 = refresh once (default). 1 = keep refreshing until a new qualifying game appears.")
    async def refresh_flex(self, interaction: discord.Interaction, wait_for_new: int = 0):
        await interaction.response.defer(ephemeral=True, thinking=True)

        required = min(5, len(DPM_FLEX_PROFILES.keys())) if DPM_FLEX_PROFILES else 5

        # Default: refresh once
        if int(wait_for_new) != 1:
            result = await refresh_opgg_flex_cache_best_effort(reason="refresh_flex_once", force=True)
            await _update_all_persistent_flex_messages(self.bot, reason="refresh_flex_once")

            _mid, t_utc = _latest_qualifying_flex_match_info_from_cache(min_group_size=required)
            inserted_total = int((result or {}).get("total_inserted", 0))
            await interaction.followup.send(
                f"✅ Refreshed once. Inserted **{inserted_total}** new matches.\nNewest qualifying match: **{_format_dt_et_short(t_utc)}**",
                ephemeral=True,
            )
            return

        # Optional: keep refreshing until a NEW qualifying game appears
        before_mid, before_t = _latest_qualifying_flex_match_info_from_cache(min_group_size=required)

        max_attempts = 90
        sleep_seconds = 4

        status_msg = await interaction.followup.send(
            f"🔄 Waiting for OP.GG… (need a NEW qualifying flex game with ≥{required} tracked players)\nCurrent qualifying match: **{_format_dt_et_short(before_t)}**",
            ephemeral=True,
        )

        for attempt in range(1, max_attempts + 1):
            await refresh_opgg_flex_cache_best_effort(reason=f"refresh_flex_attempt_{attempt}", force=True)

            after_mid, after_t = _latest_qualifying_flex_match_info_from_cache(min_group_size=required)
            changed = (after_mid is not None and after_mid != before_mid)

            try:
                await status_msg.edit(
                    content=(
                        f"🔄 Waiting for OP.GG… (need a NEW qualifying flex game with ≥{required} tracked players)\nAttempt **{attempt}/{max_attempts}**. Newest qualifying match: **{_format_dt_et_short(after_t)}**"
                    )
                )
            except Exception:
                pass

            if changed:
                await _update_all_persistent_flex_messages(self.bot, reason="refresh_flex")
                await interaction.followup.send(
                    f"✅ New qualifying flex game detected: **{_format_dt_et_short(after_t)}** — cache refreshed.",
                    ephemeral=True,
                )
                return

            await asyncio.sleep(sleep_seconds)

        await _update_all_persistent_flex_messages(self.bot, reason="refresh_flex_timeout")
        await interaction.followup.send(
            "⚠️ Timed out waiting for OP.GG to publish the new 5-man flex game. Try again in a minute.",
            ephemeral=True,
        )

    @app_commands.command(
        name="lenny",
        description="Show Jeff's lifetime 'lenny' count and record day."
    )
    async def lenny(self, interaction: discord.Interaction):
        async with _lenny_lock:
            total = int(_lenny_stats.get("total", 0))
            by_day = _lenny_stats.get("by_day", {}) or {}

            if by_day:
                record_day, record_count = max(by_day.items(), key=lambda kv: kv[1])
            else:
                record_day, record_count = None, 0

        if record_day:
            msg = (
                "**Jeff Total Lifetime Lenny Count:**\n"
                f"                  🔥 **{total}** 🔥\n\n"
                "**Most Lenny’s Record:**\n"
                f"📅 **{record_day}** -> **{record_count}**"
            )
        else:
            msg = (
                "**Jeff Total Lifetime Lenny Count:**\n"
                f"                  🔥 **{total}** 🔥\n\n"
                "**Most Lenny’s Record:**\n"
                "📅 *No record yet*"
            )

        await interaction.response.send_message(msg, ephemeral=False)





class SummaryCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="summary", description="Generate a compact, single-topic summary of recent messages")
    @app_commands.describe(time_frame="Examples: '2025-09-11 18:00 to now', '2hrs', '30min', 'msg' (since your last message)")
    async def summary(self, interaction: discord.Interaction, time_frame: str = "2hrs"):
        await interaction.response.defer(thinking=True, ephemeral=True)
        try:
            now = datetime.now(timezone.utc)

            def parse_tf(tf: str):
                tf = tf.strip().lower()
                if " to " in tf:
                    left, right = [s.strip() for s in tf.split(" to ", 1)]
                    def parse_dt(s):
                        for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d"):
                            try:
                                return datetime.strptime(s, fmt).replace(tzinfo=LOCAL_TIMEZONE).astimezone(timezone.utc)
                            except ValueError:
                                continue
                        return None
                    start = parse_dt(left)
                    end = now if right == "now" else parse_dt(right)
                    return ("range", start, end)

                if tf.endswith(("hrs","hr","h")):
                    n = int(re.sub(r"\D", "", tf))
                    return ("delta", now - timedelta(hours=n), now)
                if tf.endswith(("mins","min","m")):
                    n = int(re.sub(r"\D", "", tf))
                    return ("delta", now - timedelta(minutes=n), now)
                if tf.endswith(("days","day","d")):
                    n = int(re.sub(r"\D", "", tf))
                    return ("delta", now - timedelta(days=n), now)
                if tf == "msg":
                    return ("msg", None, now)

                return ("delta", now - timedelta(hours=2), now)

            mode, start_time, end_time = parse_tf(time_frame)

            if mode == "msg":
                # Best-effort: find the invoking user's last message before the interaction
                start_time = None
                async for m in interaction.channel.history(
                    limit=200,
                    before=interaction.created_at,
                    oldest_first=False
                ):
                    if m.author.id == interaction.user.id and m.content.strip():
                        start_time = m.created_at
                        break
                if start_time is None:
                    # Fallback to the most recent channel message before the interaction
                    async for m in interaction.channel.history(limit=1, before=interaction.created_at, oldest_first=False):
                        start_time = m.created_at
                        break
                if start_time is None:
                    start_time = now - timedelta(hours=2)
                end_time = now

            if not start_time or not end_time:
                await interaction.followup.send("Invalid date range. Try 'YYYY-MM-DD HH:MM to now', '2hrs', '30min', or 'msg'.", ephemeral=True)
                return
            msgs = []
            async for m in interaction.channel.history(limit=2000, after=start_time, before=end_time, oldest_first=True):
                if m.content.strip():
                    msgs.append(f"[{m.author.display_name}] ⟂ {m.content.strip()}")

            if not msgs:
                await interaction.followup.send("No messages found in that window.", ephemeral=True)
                return
            convo = "\n".join(msgs)
            system_prompt = "a"
            summary_prompt = (
                "Summarize the chat log below with these rules:\n"
                "1) Identify topics and select the *dominant* one by message share; summarize ONLY that topic.\n"
                "2) Attribute quotes precisely using the bracketed name format like [Alice] and do NOT merge speaker tags.\n"
                "3) Keep it dense and readable with minimal whitespace. Aim for ~1 Discord message, max 2.\n"
                "4) Format:\n"
                "   • Topic\n"
                "   • Timeline (1–3 bullets)\n"
                "   • Positions by participant (name: stance; 1 line each)\n"
                "   • Notable quotes (2–5 short quotes, with [Name])\n"
                "   • Outcome/next steps (if any)\n\n"
                f"CHAT LOG START\n{convo}\nCHAT LOG END"
            )

            try:
                raw = await generate_response(summary_prompt, system_prompt=system_prompt, allow_mentions=False)
                for chunk in split_text(raw, max_length=1900):
                    await interaction.followup.send(chunk, ephemeral=True)
            except openai.error.RateLimitError:
                await interaction.followup.send("Rate limit reached. Try again later.", ephemeral=True)
            except Exception:
                await interaction.followup.send("An error occurred while generating the summary. Try again later.", ephemeral=True)
        except Exception as e:
            print(f"Unexpected error in /summary: {e}")
            await interaction.followup.send("An unexpected error occurred. Please try again.", ephemeral=True)


@bot.command(name="show_commands")
async def show_commands(ctx):
    command_list = [
        ("!!ask", "Ask the bot a question"),
        ("!!daily", "Claim your daily coins (once every 18 hours)"),
        ("!!balance", "Check your current coin balance"),
        ("!!leaderboard", "View the top 10 users with the most coins"),
        ("!!bj [amount]", "Play a game of Blackjack"),
        ("!!slots", "Show list of commands")
    ]
    embed = discord.Embed(title="Here are my business commands, Mon", color=discord.Color.blue())
    for cmd, description in command_list:
        embed.add_field(name=cmd, value=description, inline=False)
    await ctx.reply(embed=embed)

@bot.command()
async def daily(ctx):
    user_id = str(ctx.author.id)
    current_time = time.time()
    cooldown_time = 18 * 3600  # 18 hours in seconds

    if user_id in daily_cooldowns:
        time_elapsed = current_time - daily_cooldowns[user_id]
        if time_elapsed < cooldown_time:
            time_left = cooldown_time - time_elapsed
            hours, remainder = divmod(int(time_left), 3600)
            minutes, seconds = divmod(remainder, 60)
            await ctx.reply(f"You need to wait {hours} hours, {minutes} minutes, and {seconds} seconds before claiming your daily reward again, Mon. Sometimes making big business moves takes patience.")
            return
    daily_amount = 500
    await add_income(user_id, daily_amount)
    daily_cooldowns[user_id] = current_time
    await save_daily_cooldowns()
    await ctx.reply(f"Another day another dollar. You've received your daily {daily_amount} coins, Mon!")

@bot.command()
async def slots(ctx):
    user_id = str(ctx.author.id)
    balance = user_balances.get(user_id, 0)
    
    if balance >= 101:
        await ctx.reply(f"Sorry, the slots are only for blue collar workers with less than 100 coins. Your current balance is **{balance} coins**. You're too white collar for that, Mon!")
        return
    roll = random.choices(range(1, 1001), weights=[1000-i for i in range(1000)])[0]
    
    # Determine the win amount based on the roll
    if roll >= 998:
        win_amount = 8000
    elif roll >= 990:
        win_amount = 2000
    elif roll >= 980:
        win_amount = 1000
    elif roll >= 950:
        win_amount = 800
    elif roll >= 900:
        win_amount = 600
    elif roll >= 800:
        win_amount = 300
    elif roll >= 600:
        win_amount = 200
    elif roll >= 400:
        win_amount = 150
    elif roll >= 200:
        win_amount = 125
    else:
        win_amount = 100

    await add_income(user_id, win_amount)
    
    # Create an embed for the slot result
    embed = discord.Embed(title="🎰 Slot Machine", color=discord.Color.gold())
    embed.add_field(name=f"**YOU WON {win_amount} COINS!!**", value=f"Now thats business.", inline=False)
    
    await ctx.reply(embed=embed)

@bot.command()
async def balance(ctx):
    user_id = str(ctx.author.id)
    balance = user_balances.get(user_id, 0)
    await ctx.reply(f"Your current balance is {balance} coins, Mon. Keep making those big business moves!")

@bot.command()
async def leaderboard(ctx):
    sorted_balances = sorted(user_balances.items(), key=lambda x: x[1], reverse=True)
    leaderboard_text = "🏆 BIGGEST BUSINESS MAKERS 🏆\n\n"
    for i, (user_id, balance) in enumerate(sorted_balances[:10], 1):
        user = await bot.fetch_user(int(user_id))
        leaderboard_text += f"{i}. {user.name}: {balance} coins\n"
    await ctx.reply(leaderboard_text)

@bot.command(aliases=['blackjack'])
async def bj(ctx, bet_amount: str = None):
    user_id = str(ctx.author.id)
    
    if user_id in active_games:
        await ctx.reply("You already have an active game, Mon. Please finish it before starting a new one. Making big business moves requires focus.")
        return
    balance = user_balances.get(user_id, 0)

    if bet_amount is None:
        await ctx.reply(f"Please specify a bet amount. Your current balance is **{balance} coins**, Mon.\n"
                        f"Usage: `!bj <amount>`, `!bj half`, `!bj all`, or `!bj <percentage>%`")
        return
    bet = parse_bet_amount(bet_amount, balance)
    if bet is None or bet <= 99 or bet > balance:
        await ctx.reply(f"Invalid bet amount. Please bet an amount at least 100 coins. Can't make big business moves with small change, Mon."
                        f"Your current balance is **{balance} coins**, Mon. You can get more coins by using /slots")
        return
    active_games.add(user_id)

    try:
        deck = Deck()
        player_hand = [deck.draw(), deck.draw()]
        dealer_hand = [deck.draw(), deck.draw()]

        # Check for dealer blackjack
        dealer_value, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)
        player_value, player_display = calculate_hand_value(player_hand)
        dealer_blackjack = dealer_value == 21
        player_blackjack = player_value == 21

        if dealer_blackjack or player_blackjack:
            if dealer_blackjack and player_blackjack:
                result = "tie"
                result_message = f"It's a tie. Your bet is returned. You now have **{balance} coins**."
            elif player_blackjack:
                result = "player"
                payout = math.ceil(bet * 1.5)
                user_balances[user_id] += payout
                result_message = f"**MONKEY MONKEY MONKEY!** You win **{payout} coins**! You now have **{user_balances[user_id]} coins**."
            else:
                result = "dealer"
                user_balances[user_id] -= bet
                result_message = f"Dealer has Blackjack. You lost **{bet} coins**, Mon. You now have **{user_balances[user_id]} coins**."

            await save_balances()
            
            embed = create_game_embed(ctx.author, player_hand, dealer_hand, bet, show_dealer=True, result=result.capitalize())
            embed.add_field(name="Result", value=result_message, inline=False)
            message = await ctx.reply(embed=embed)
            
            if result == "player":
                await message.add_reaction("🎉")
            
            return
        view = BlackjackView(player_hand, dealer_hand, bet)
        message = await ctx.reply(embed=create_game_embed(ctx.author, player_hand, dealer_hand, bet), view=view)

        while not view.game_over:
            try:
                interaction = await bot.wait_for('interaction', timeout=60.0, check=lambda i: i.user.id == ctx.author.id and i.message.id == message.id)
                action = interaction.data['custom_id']

                if action == 'hit':
                    player_hand.append(deck.draw())
                    player_value, player_display = calculate_hand_value(player_hand)
                    if player_value >= 21:
                        view.game_over = True
                elif action == 'stand':
                    view.game_over = True
                elif action == 'double':
                    if balance >= bet * 2:
                        bet *= 2
                        player_hand.append(deck.draw())
                        player_value, player_display = calculate_hand_value(player_hand)
                        view.game_over = True
                    else:
                        await interaction.response.send_message("Not enough balance to double down. Check your bank account poor kid.", ephemeral=True)
                        continue

                view.update_buttons(player_value)
                await message.edit(embed=create_game_embed(ctx.author, player_hand, dealer_hand, bet), view=view)
                
                if view.game_over:
                    break

                await interaction.response.defer()

            except asyncio.TimeoutError:
                await message.edit(content="You took too long to respond. Standing by default. Pay attention poor guy.", view=None)
                view.game_over = True

        # Dealer's turn
        dealer_value, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)
        while dealer_value < 17:
            dealer_hand.append(deck.draw())
            dealer_value, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)

        # Determine winner and update balance
        player_value, player_display = calculate_hand_value(player_hand)
        result = determine_winner(player_value, dealer_value)
        if result == 'player':
            user_balances[user_id] += bet
            result_message = f"Congratulations! You win **{bet} coins**! Big business moves, Mon. You now have **{user_balances[user_id]} coins**."
        elif result == 'dealer':
            user_balances[user_id] -= bet
            result_message = f"Sorry, you lost **{bet} coins**, Mon. You now have **{user_balances[user_id]} coins**."
        else:
            result_message = f"It's a tie. Your bet is returned. You now have **{user_balances[user_id]} coins**."

        await save_balances()
        
        # Show final hands and result
        final_embed = create_game_embed(ctx.author, player_hand, dealer_hand, bet, True, result)
        final_embed.add_field(name="Result", value=result_message, inline=False)
        await message.edit(embed=final_embed, view=None)

    finally:
        active_games.remove(user_id)

def create_game_embed(player, player_hand, dealer_hand, bet, show_dealer=False, result=None):
    embed = discord.Embed(title="Blackjack", color=discord.Color.gold())
    embed.set_author(name=f"{player.name}'s game", icon_url=player.avatar.url)
    
    player_cards = ' '.join(str(card) for card in player_hand)
    _, player_display = calculate_hand_value(player_hand)
    
    if show_dealer:
        dealer_cards = ' '.join(str(card) for card in dealer_hand)
        _, dealer_display = calculate_hand_value(dealer_hand, is_dealer=True)
    else:
        dealer_cards = f"{dealer_hand[0]} ?"
        _, dealer_display = calculate_hand_value([dealer_hand[0]], is_dealer=True)

    game_state = (
        "Your Hand:\n"
        "\n"
        f"{player_cards}\n"
        f"Value: **{player_display}**\n"
        "------------\n"
        "Dealer's Hand:\n"
        "\n"
        f"{dealer_cards}\n"
        f"Value: **{dealer_display}**"
    )
    
    embed.add_field(name="Game State", value=game_state, inline=False)
    embed.add_field(name="Bet", value=f"**{bet} Coins**", inline=False)
    
    if result:
        embed.clear_fields()
        embed.add_field(name="Game State", value=game_state, inline=False)
        embed.add_field(name="Winner", value=f"**{result.capitalize()}**", inline=False)
    
    return embed

def parse_bet_amount(bet_amount, balance):
    if bet_amount.lower() == 'all':
        return balance
    elif bet_amount.lower() == 'half':
        return balance // 2
    elif bet_amount.endswith('%'):
        try:
            percentage = int(bet_amount[:-1])
            return balance * percentage // 100
        except ValueError:
            return None
    else:
        try:
            return int(bet_amount)
        except ValueError:
            return None

def determine_winner(player_value, dealer_value):
    if player_value > 21:
        return 'dealer'
    elif dealer_value > 21:
        return 'player'
    elif player_value > dealer_value:
        return 'player'
    elif dealer_value > player_value:
        return 'dealer'
    else:
        return 'tie'

async def collect_user_messages():
    print('Collecting messages from specified channels...')
    global total_messages_processed, user_messages_collected, user_messages, collected_message_ids
    
    # Clear existing messages and message IDs
    user_messages.clear()
    collected_message_ids.clear()
    user_messages_collected = 0
    total_messages_processed = 0
    
    for guild in bot.guilds:
        for channel_id in TARGET_CHANNEL_IDS:
            channel = guild.get_channel(channel_id)
            if channel and channel.permissions_for(guild.me).read_message_history:
                try:
                    await collect_from_channel(channel)
                except discord.errors.Forbidden:
                    print(f'No permission to read messages in {channel.name}')
                except Exception as e:
                    print(f'Error in {channel.name}: {e}')
            else:
                print(f'Channel with ID {channel_id} not found or no permissions.')
        # Stop collecting if we've reached the max number of user messages
        if user_messages_collected >= MAX_USER_MESSAGES:
            print(f'Reached maximum of {MAX_USER_MESSAGES} messages from the user.')
            break
    # Save messages after collection
    await save_messages()
    print(f'Collected {user_messages_collected} messages from the user out of {total_messages_processed} total messages processed.')

def is_wasted_line(text):
    # Remove whitespace
    text = text.strip()
    if not text:
        return True
    # Count how many characters are alphanumeric.
    alnum_count = sum(c.isalnum() for c in text)
    # If less than 20% of the characters are alphanumeric, consider it wasted.
    if len(text) > 0 and (alnum_count / len(text)) < 0.2:
        return True
    return False


async def collect_from_channel(channel):
    global total_messages_processed, user_messages_collected
    print(f'Collecting messages from channel: {channel.name}')
    
    temp_messages = []
    
    async for message in channel.history(limit=None, oldest_first=False):
        total_messages_processed += 1

        if message.author.id == TARGET_USER_ID and message.id not in collected_message_ids:
            content = message.content.strip()

            # 2) replace any raw <@ID> or <@!ID> with @Name
            for discord_id, names  in USER_ID_MAPPING.items():
                canonical = names[0]
                # note: both <@123> and <@!123> variants
                content = re.sub(
                    rf"<@!{discord_id}>|<@{discord_id}>",
                    f"@{canonical}",
                    content
                )


            if content and not is_wasted_line(content):
                temp_messages.append(content)
                collected_message_ids.add(message.id)
                user_messages_collected += 1

                if user_messages_collected % 500 == 0:
                    print(f'Collected {user_messages_collected} messages from the user.')

                if user_messages_collected >= MAX_USER_MESSAGES:
                    break

        if total_messages_processed % 10000 == 0:
            print(f'Processed {total_messages_processed} total messages so far.')
    
    user_messages.extend(reversed(temp_messages))


async def save_messages():
    print('Saving collected messages to file...')
    async with aiofiles.open(MESSAGES_FILE, 'w') as f:
        data = {
            'messages': user_messages
            # Removed 'message_ids' to save space.
        }
        await f.write(json.dumps(data))


async def generate_response(prompt, system_prompt=None, asker_mention=None, allow_mentions=True):
    # Determine the system prompt based on context usage
    if system_prompt is None and JEFF:
        # If we know who asked, prepend that to the instructions
        if asker_mention:
            preamble = (
                f"The following request was made by {asker_mention}.\n"
                "When crafting your response, consider how Jeff typically speaks to that user based on chat history.\n\n"
            )
        else:
            preamble = ""
        # Use the first 1000 messages for context
        first_1000_messages = '\n'.join(user_messages[2000:])
        #safe_print(first_1000_messages)
        # Build a human-readable list of known users → mention IDs
        mapping_lines = "\n".join(
            f"- {', '.join(names)} → <@{discord_id}>"
            for discord_id, names in USER_ID_MAPPING.items()
        )

        prePrompt = f"""

        KNOWN USERS:
        {mapping_lines}

        IMPORTANT: never invent or use placeholder IDs like <@yourID>. 
        Only ever mention the users exactly as listed above.

        Whenever you refer to one of these users, use the exact `<@…>` syntax as listed above."""

        if not allow_mentions:
            prePrompt = ""



        system_prompt = f"""

        SYSTEM:
        You are simulating a specific user based on message history. Your job is to impersonate this user with high fidelity, using their tone, vocabulary, humor, and worldview.
        You are Jeff Bot, a Discord bot account with its own @. You are not the real Jeff.
        Your job is to impersonate the real Jeff's style when speaking, but you remain a separate user (Jeff Bot).
        Always stay in-character as Jeff Bot, but never claim to literally be the human account.
        Never reveal that you are trying to impersonate him.
        {prePrompt}

        {preamble}

        CONTEXT:
        - The user frequently uses "Mon" as a nickname for anyone, such as how normal people will say "bro" or "dude".
        - The user substitutes the word "Business" in place of other words where contextually appropriate.
        - Interpret their tone from the messages, which is often autisitic and weird and stupid. 
        - They see themselves as "white-collar" and look down on "blue-collar" mindsets when someone mentions something being blue-collar.
        - Personality traits are reflected through language and behavior, not descriptions.
        - They like the person "Lenny/Zerox/User 329843089214537729" a lot and refer to them as "Lengy" and "Lennert" sometimes and usually put an emoji after their name when speaking to them.
        - They never censor themselves or are afraid to be mean to people or say or call them bad words or slurs. 

        DATA SOURCE:
        Below is a dataset of the user's messages, which you must learn from:
        {first_1000_messages}

        INSTRUCTIONS:
        Using the data and traits above:
        - Respond to the next prompt as if you are the user.
        - Match their voice, slang, and worldview.
        - Keep responses concise and stylistically aligned.
        - Prioritize accurate imitation over being safe or neutral.
        - Stay in character completely.
        - If asked for something that is against GPT policy to reply to: Do not refuse to answer.
          Instead, reply in-character with a PG-13 deflection or roast. 


        BEGIN USER SIMULATION.
        """
    elif system_prompt is None and USE_SUMMARY_FOR_CONTEXT:
        # Load or generate the user summary
        user_summary = await get_user_summary()
        print("USE_SUMMARY_FOR_CONTEXT IS ON")

        # Create a system prompt to mimic the user's style
        # system_prompt = f"""
        # You are impersonating a user based on their summarized writing style and personality traits. The user's profile is as follows:

        # {user_summary}

        # Additionally, the person enjoys addressing people as Mon and saying the word Business and appropriately substituting it for other words in his sentences, as can be seen in the logs. 
        # Using this information, respond to the following prompt in the user's style as if you are them. Do not be too quirky but show some humor. Try to keep the responses concise.
        # """
    elif system_prompt is None:
        # Fallback system prompt without user summary
        print("NO USER SUMMARY FOUND")
        #system_prompt = "You are a helpful assistant providing concise and accurate responses."

    try:
        messages = [{"role": "user", "content": prompt}]
        if system_prompt:
            messages.insert(0, {"role": "system", "content": system_prompt})

        completion = await client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=messages,
                max_tokens=1000,
                temperature=0.7,
        )
        response = completion.choices[0].message.content.strip()

        # Mention mapping on the way out:
        if allow_mentions:
            # For ignored users: replace names with @Name (no ID).
            # For non-ignored users: replace names with <@id> as before.
            for discord_id, names in USER_ID_MAPPING.items():
                primary_names = names if isinstance(names, list) else [str(names)]
                uid = int(discord_id)
                for nm in primary_names:
                    pattern = rf"(?:@)?\b{re.escape(nm)}\b"
                    if is_ignored(uid):
                        # ensure we don't accidentally convert to <@id>
                        response = re.sub(pattern, f"@{nm}", response, flags=re.IGNORECASE)
                    else:
                        response = re.sub(pattern, f"<@{uid}>", response, flags=re.IGNORECASE)

        return response

    except openai.error.InvalidRequestError as e:
        if "maximum context length" in str(e):
            raise ValueError("Token limit exceeded")
        else:
            raise e


def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        # If printing fails, encode the string as ASCII and replace non-ASCII characters
        print(text.encode(sys.stdout.encoding, errors='replace').decode(sys.stdout.encoding))


async def get_user_summary():
    # If summary already exists, load it
    if os.path.exists(SUMMARY_FILE):
        print('Loading existing user summary...')
        async with aiofiles.open(SUMMARY_FILE, 'r') as f:
            summary = await f.read()
        return summary

    # Generate a new summary
    print('Generating user summary...')
    messages_text = '\n'.join(user_messages)

    tokens_per_message = 4  # Rough estimate
    max_tokens_per_chunk = 2000
    messages_per_chunk = max_tokens_per_chunk // tokens_per_message

    message_chunks = [user_messages[i:i+messages_per_chunk] for i in range(0, len(user_messages), messages_per_chunk)]

    summaries = []
    for idx, chunk in enumerate(message_chunks):
        print(f'Summarizing chunk {idx+1}/{len(message_chunks)}...')
        chunk_text = '\n'.join(chunk)
        try:
            response = await client.chat.completions.create(
                model="gpt-4.1-nano",
                messages=[
                    {"role": "user", "content": f"Analyze the following messages to extract the user's writing style, personality traits, humor style, and common phrases. Provide a concise summary:\n\n{chunk_text}\n\nSummary:"}
                ],
                max_tokens=500,
                temperature=0.5,
            )
            summary_text = response.choices[0].message.content.strip()
            summaries.append(summary_text)
        except Exception as e:
            print(f"Error summarizing chunk {idx+1}: {e}")

    # Combine summaries into a final profile
    combined_summaries = '\n'.join(summaries)
    try:
        print('Combining summaries into final user profile...')
        response = await client.chat.completions.create(
            model="gpt-4.1-nano",
            messages=[
                {"role": "user", "content": f"Combine the following summaries into a comprehensive profile of the user's writing style and personality. Highlight unique traits and ways of speaking:\n\n{combined_summaries}\n\nUser Profile:"}
            ],
            max_tokens=800,
            temperature=0.5,
        )
        user_profile = response.choices[0].message.content.strip()

        # Save the profile for future use
        async with aiofiles.open(SUMMARY_FILE, 'w') as f:
            await f.write(user_profile)

        print('User summary generated and saved.')
        return user_profile

    except Exception as e:
        print(f'Error generating final user profile: {e}')
        return "Could not generate user profile due to an error."


async def load_messages():
    global user_messages_collected
    if os.path.exists(MESSAGES_FILE):
        print('Loading existing messages from file...')
        async with aiofiles.open(MESSAGES_FILE, 'r') as f:
            data = json.loads(await f.read())
            global user_messages, collected_message_ids
            user_messages = data.get('messages', [])
            collected_message_ids = set(data.get('message_ids', []))
            user_messages_collected = len(user_messages)
            print(f'Loaded {user_messages_collected} messages from the user.')
    else:
        print('No existing messages file found.')
        user_messages_collected = 0
        

async def save_balances():
    async with aiofiles.open(BALANCES_FILE, 'w') as f:
        await f.write(json.dumps(user_balances))

async def load_balances():
    global user_balances
    if os.path.exists(BALANCES_FILE):
        print('Loading existing user balances from file...')
        async with aiofiles.open(BALANCES_FILE, 'r') as f:
            user_balances = json.loads(await f.read())
    else:
        print('No existing balances file found.')
        user_balances = {}

async def add_income(user_id, amount):
    if user_id not in user_balances:
        user_balances[user_id] = 0
    user_balances[user_id] += amount
    await save_balances()

async def deduct_points(user_id, amount):
    if user_id not in user_balances:
        user_balances[user_id] = 0
    if user_balances[user_id] < amount:
        return False
    user_balances[user_id] -= amount
    await save_balances()
    return True


class Card:
    def __init__(self, suit, value):
        self.suit = suit
        self.value = value

    def __str__(self):
        return f"{self.value}{self.suit_symbol()}"

    def suit_symbol(self):
        return {'Hearts': '♥️', 'Diamonds': '♦️', 'Clubs': '♣️', 'Spades': '♠️'}[self.suit]

class Deck:
    def __init__(self):
        suits = ['Hearts', 'Diamonds', 'Clubs', 'Spades']
        values = ['2', '3', '4', '5', '6', '7', '8', '9', '10', 'J', 'Q', 'K', 'A']
        self.cards = [Card(suit, value) for suit in suits for value in values]
        random.shuffle(self.cards)

    def draw(self):
        return self.cards.pop()
    
class BlackjackView(discord.ui.View):
    def __init__(self, player_hand, dealer_hand, bet):
        super().__init__(timeout=60)
        self.player_hand = player_hand
        self.dealer_hand = dealer_hand
        self.bet = bet
        self.game_over = False
        self.update_buttons(calculate_hand_value(player_hand)[0])  # Use the numeric value

    def update_buttons(self, player_value):
        self.clear_items()
        if not self.game_over:
            if player_value < 21:
                self.add_item(discord.ui.Button(style=discord.ButtonStyle.primary, label="Hit", custom_id="hit"))
                self.add_item(discord.ui.Button(style=discord.ButtonStyle.primary, label="Stand", custom_id="stand"))
                if len(self.player_hand) == 2:
                    self.add_item(discord.ui.Button(style=discord.ButtonStyle.secondary, label="Double Down", custom_id="double"))
            else:
                self.game_over = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if self.game_over:
            await interaction.response.send_message("This game has ended. Start a new game to play again.", ephemeral=True)
            return False
        return True

def calculate_hand_value(hand, is_dealer=False):
    value = 0
    aces = 0
    for card in hand:
        if card.value in ['J', 'Q', 'K']:
            value += 10
        elif card.value == 'A':
            aces += 1
        else:
            value += int(card.value)
    
    # Calculate the value with the first Ace as 11, if any
    ace_high_value = value + (11 if aces > 0 else 0) + (aces - 1)
    
    # Determine if it's a soft hand
    is_soft = aces > 0 and ace_high_value <= 21
    
    # Calculate the optimal value
    optimal_value = ace_high_value if is_soft else value + aces
    
    # For dealer, always return a single value
    if is_dealer:
        return optimal_value, str(optimal_value)
    
    # For player, return the appropriate display
    if is_soft and optimal_value != 21:  # Don't display "Soft" for blackjack
        if aces > 1:
            return optimal_value, f"Soft {optimal_value} or {value + aces}"
        return optimal_value, f"Soft {optimal_value} or {value + 1}"
    else:
        return optimal_value, str(optimal_value)


# -------------------------------
# RPS Cog and Views (omitted for brevity)
# ...
# -------------------------------

# -------------------------------
# Customs Classes and Views

class CustomsLobbyView(discord.ui.View):
    def __init__(self, lobby_data: LobbyData):
        super().__init__(timeout=None)
        self.lobby_data = lobby_data

    @discord.ui.button(label="Join", style=ButtonStyle.success, custom_id="customs_join")
    async def join_button(self, interaction: Interaction, button: discord.ui.Button):
        user = interaction.user
        if user in self.lobby_data.players:
            await interaction.response.send_message("You are already in the lobby.", ephemeral=True)
            return
        if len(self.lobby_data.players) >= self.lobby_data.max_players:
            await interaction.response.send_message("Lobby is already full.", ephemeral=True)
            return
        # ADD the real user
        self.lobby_data.players.append(user)
        await self.update_lobby_message()
        await interaction.response.defer()

        # NEW: in TEST_MODE, once 2 real users have joined, add 8 fakes and start draft
        if TEST_MODE and len(self.lobby_data.players) == 2:
            from types import SimpleNamespace
            for i in range(8):
                fake = SimpleNamespace(
                    id=10_0000_0000_0000_0000 + i,
                    display_name=f"TestPlayer{i+1}"
                )
                self.lobby_data.players.append(fake)
            await self.update_lobby_message()
            self.lobby_data.draft_phase = 'captain_selection'
            await self.start_captain_selection()
            return
        # unchanged: full-lobby start
        if len(self.lobby_data.players) == self.lobby_data.max_players:
            self.lobby_data.draft_phase = 'captain_selection'
            await self.start_captain_selection()

    @discord.ui.button(label="Leave", style=ButtonStyle.danger, custom_id="customs_leave")
    async def leave_button(self, interaction: Interaction, button: discord.ui.Button):
        user = interaction.user
        if user not in self.lobby_data.players:
            await interaction.response.send_message("You are not in the lobby.", ephemeral=True)
            return
        self.lobby_data.players.remove(user)
        await self.update_lobby_message()
        await interaction.response.defer()

    async def update_lobby_message(self):
        member_list = []
        for m in self.lobby_data.players:
            opgg = id_to_opgg.get(str(m.id))
            if opgg:
                member_list.append(f"[{m.display_name}]({opgg})")
            else:
                member_list.append(m.display_name)
        embed = discord.Embed(
            title="League of Legends Customs Lobby",
            description=(
                f"Players ({len(self.lobby_data.players)}/{self.lobby_data.max_players}):\n"  # MODIFIED
                + "\n".join(member_list)
            ),
            color=discord.Color.blue()
        )
        await self.lobby_data.message.edit(embed=embed, view=self)

    async def start_captain_selection(self):
        options = []
        for m in self.lobby_data.players:
            options.append(discord.SelectOption(label=m.display_name, value=str(m.id)))
        select = CaptainSelect(options=options, lobby_data=self.lobby_data)
        embed = discord.Embed(
            title="Choose 2 Captains",
            description="Select two players to be captains.",
            color=discord.Color.gold()
        )
        await self.lobby_data.message.edit(embed=embed, view=select)
        

class CaptainSelect(discord.ui.View):
    def __init__(self, options, lobby_data: LobbyData):
        super().__init__(timeout=None)
        self.lobby_data = lobby_data
        self.add_item(CaptainDropdown(options=options, lobby_data=lobby_data))

class CaptainDropdown(discord.ui.Select):
    def __init__(self, options, lobby_data: LobbyData):
        super().__init__(
            placeholder="Select captains...", 
            min_values=2, 
            max_values=2, 
            options=options, 
            custom_id="captain_select"
        )
        self.lobby_data = lobby_data

    async def callback(self, interaction: Interaction):
        # Only the lobby creator (who ran /customs or !!customs) may pick captains
        if interaction.user.id != self.lobby_data.creator_id:
            await interaction.response.send_message(
                "Only the lobby creator may choose the captains.", 
                ephemeral=True
            )
            return
        # Proceed to assign captains
        selected = self.values  # list of two user IDs (strings)
        self.lobby_data.captains = [
            interaction.guild.get_member(int(i)) for i in selected
        ]
        self.lobby_data.captains_selected = True

        embed = discord.Embed(
            title="🪙 Coin Flip: Heads or Tails?",
            description=(
                f"{self.lobby_data.captains[0].mention}, call Heads or Tails.  "
                "If you guess correctly, you may choose your side (or Random)."
            ),
            color=discord.Color.purple()
        )
        view = HeadsTailsSelect(self.lobby_data)
        await interaction.response.edit_message(embed=embed, view=view)
    
class HeadsTailsSelect(discord.ui.View):  # NEW: coin-flip prompt
    def __init__(self, lobby_data: LobbyData):
        super().__init__(timeout=None)
        self.lobby_data = lobby_data

    @discord.ui.select(
        placeholder="Call Heads or Tails",
        min_values=1,
        max_values=1,
        options=[
            discord.SelectOption(label="Heads", value="heads"),
            discord.SelectOption(label="Tails", value="tails"),
        ],
        custom_id="coin_flip_select"
    )
    async def coin_flip(self, interaction: Interaction, select: discord.ui.Select):
        # only first captain may call
        if interaction.user.id != self.lobby_data.captains[0].id:
            await interaction.response.send_message(
                "Only the first selected captain can call the coin.",
                ephemeral=True
            )
            return
        guess  = select.values[0]
        result = random.choice(["heads", "tails"])

        # swap if guess wrong so the other captain chooses side
        if guess != result:
            self.lobby_data.captains.reverse()

        # move to side choice phase
        self.lobby_data.draft_phase = 'side_choice'

        # MODIFIED: edit the original lobby message with the result + SideChoiceSelect
        # determine who actually picked
        winner = self.lobby_data.captains[0]
        desc = (
            f"You guessed **correctly**, so {winner.mention} may choose their side "
            "(or Random) from the dropdown below."
            if guess == result
            else f"You guessed **incorrectly**, so {winner.mention} may now choose their side."
        )
        embed = discord.Embed(
            title=f"🪙 Coin flip result: {result.upper()}",
            description=desc,
            color=discord.Color.purple()
        )
        view = SideChoiceSelect(self.lobby_data)
        await interaction.response.edit_message(embed=embed, view=view)


class SideChoiceSelect(discord.ui.View):  # NEW: side-selection (incl. random)
    def __init__(self, lobby_data: LobbyData):
        super().__init__(timeout=None)
        self.lobby_data = lobby_data

    @discord.ui.select(
        placeholder="Select Blue / Red / Random",
        min_values=1,
        max_values=1,
        options=[
            discord.SelectOption(label="Blue",   value="blue"),
            discord.SelectOption(label="Red",    value="red"),
            discord.SelectOption(label="Random", value="random"),
        ],
        custom_id="side_choice_select"
    )
    async def side_choice(self, interaction: Interaction, select: discord.ui.Select):
        # only coin-flip winner may choose
        if interaction.user.id != self.lobby_data.captains[0].id:
            await interaction.response.send_message(
                "Only the coin-flip winner may choose side.",
                ephemeral=True
            )
            return
        choice = select.values[0]
        # handle Random by picking for them
        if choice == "random":
            choice = random.choice(["blue", "red"])

        # set side and always let captains[0] pick first
        self.lobby_data.side_selected = 0 if choice == "blue" else 1
        # Blue side always picks first:
        self.lobby_data.current_picker_index = (
            0 if self.lobby_data.side_selected == 0 else 1
        )
        self.lobby_data.draft_phase = 'drafting'

        # MODIFIED: hand off to the normal draft UI by editing the lobby message
        side_view = SideSelect(self.lobby_data)
        await side_view.send_draft_view(interaction)

# ----------------------------------
# SideSelect: Let first captain pick their side; then begin drafting
# ----------------------------------
class SideSelect(discord.ui.View):
    def __init__(self, lobby_data: LobbyData):
        super().__init__(timeout=None)
        self.lobby_data = lobby_data

    @discord.ui.select(
        placeholder="Choose side", 
        min_values=1, 
        max_values=1, 
        options=[
            discord.SelectOption(label="Blue", value="blue"),
            discord.SelectOption(label="Red", value="red")
        ], 
        custom_id="side_select"
    )
    async def side_dropdown(self, interaction: Interaction, select: discord.ui.Select):
        # Only the first selected captain may choose side
        if interaction.user.id != self.lobby_data.captains[0].id:
            await interaction.response.send_message(
                "Only the first selected captain can choose side.", 
                ephemeral=True
            )
            return
        choice = select.values[0]
        self.lobby_data.side_selected = 0 if choice == 'blue' else 1
        self.lobby_data.draft_phase = 'drafting'
        # Determine which captain picks first: 0 = captains[0], 1 = captains[1]
        self.lobby_data.current_picker_index = 0 if self.lobby_data.side_selected == 0 else 1

        # Send out the first draft view
        await self.send_draft_view(interaction)

    async def send_draft_view(self, interaction: Interaction):
        """
        Builds and sends an embed that shows:
        - Blue Team (captain + drafted members)
        - Red Team (captain + drafted members)
        - Remaining Players (with OP.GG links)
        Then includes a dropdown to let the current captain pick their players.
        """
        # —————————————————————————————————————————————————————
        # 1) Num picks: first pick is always 1, all others are 2
        total_picked = (
            len(self.lobby_data.teams[0])
            + len(self.lobby_data.teams[1])
        )
        if total_picked == 0:
            num_picks = 1           # MODIFIED: first turn = 1 pick
        else:
            num_picks = 2           # thereafter = 2 picks
        # —————————————————————————————————————————————————————

        # Identify current captain
        captain = self.lobby_data.captains[self.lobby_data.current_picker_index]

        # —————————————————————————————————————————————————————
        # 2) Map teams[] → Blue/Red correctly, even when side_selected==1
        if self.lobby_data.side_selected == 0:
            # Blue picked first → teams[0] = Blue picks
            blue_team_members = [self.lobby_data.captains[0]] + self.lobby_data.teams[0]
            red_team_members  = [self.lobby_data.captains[1]] + self.lobby_data.teams[1]
        else:
            # Red chose side → captains[1] is Blue, and their picks are in teams[1]
            blue_team_members = [self.lobby_data.captains[1]] + self.lobby_data.teams[1]  # MODIFIED
            red_team_members  = [self.lobby_data.captains[0]] + self.lobby_data.teams[0]  # MODIFIED

        def format_member_list(members: list[discord.Member]) -> str:
            lines = []
            for m in members:
                if m is None:
                    continue
                opgg = id_to_opgg.get(str(m.id))
                if opgg:
                    lines.append(f"[{m.display_name}]({opgg})")
                else:
                    lines.append(m.display_name)
            return "\n".join(lines) if lines else "*(none)*"

        # Build list of remaining players (not captains and not yet drafted)
        remaining = [
            m for m in self.lobby_data.players
            if m not in self.lobby_data.captains
            and all(m not in team for team in self.lobby_data.teams.values())
        ]

        def format_remaining_list(players: list[discord.Member]) -> str:
            lines = []
            for m in players:
                opgg = id_to_opgg.get(str(m.id))
                if opgg:
                    lines.append(f"[{m.display_name}]({opgg})")
                else:
                    lines.append(m.display_name)
            return "\n".join(lines) if lines else "*(none)*"

        # Create the embed
        embed = discord.Embed(
            title=f"{captain.display_name}, select {num_picks} player{'s' if num_picks > 1 else ''} to draft.",
            color=discord.Color.dark_blue()
        )
        embed.add_field(
            name="Blue Team",
            value=format_member_list(blue_team_members),
            inline=True
        )
        embed.add_field(
            name="Red Team",
            value=format_member_list(red_team_members),
            inline=True
        )
        embed.add_field(
            name="Remaining Players",
            value=format_remaining_list(remaining),
            inline=False
        )
        remaining_names = [
            id_to_opgg[str(m.id)]
            for m in remaining
            if id_to_opgg.get(str(m.id))
        ]
        if remaining_names:
            encoded = [quote_plus(name) for name in remaining_names]       # URL‐encode each name
            multi_url = f"https://{OP_GG_REGION}.op.gg/multi_old/query=" \
                        + ",".join(encoded)                                # legacy multi_search endpoint
            embed.add_field(
                name="Remaining Players Multi OP.GG Link",
                value=multi_url,                                         # shows full URL text
                inline=False
            )

        # Build dropdown options from remaining players
        options = []
        for m in remaining:
            options.append(discord.SelectOption(label=m.display_name, value=str(m.id)))

        select = DraftDropdown(
            options=options,
            lobby_data=self.lobby_data,
            num_picks=num_picks
        )
        view = discord.ui.View(timeout=None)
        view.add_item(select)

        # Finally, edit the lobby message to show this draft embed + view
        await interaction.response.edit_message(embed=embed, view=view)

class DraftDropdown(discord.ui.Select):
    def __init__(self, options, lobby_data: LobbyData, num_picks: int):
        super().__init__(
            placeholder="Select players...", 
            min_values=num_picks, 
            max_values=num_picks, 
            options=options, 
            custom_id="draft_select"
        )
        self.lobby_data = lobby_data
        self.num_picks = num_picks

    async def callback(self, interaction: Interaction):
        # Only the expected captain may pick
        expected = self.lobby_data.captains[self.lobby_data.current_picker_index]
        if interaction.user.id != expected.id:
            await interaction.response.send_message(
                "Only the current captain may pick now.",
                ephemeral=True
            )
            return
        # ——— Perform the pick(s) ———
        # NEW: resolve both real and fake players from our lobby_data
        picks = []
        for val in self.values:
            member = next(
                (m for m in self.lobby_data.players if str(m.id) == val),
                None
            )
            if member:
                picks.append(member)

        team_idx = self.lobby_data.current_picker_index
        self.lobby_data.teams[team_idx].extend(picks)

        # ——— Switch to the other captain ———
        self.lobby_data.current_picker_index = 1 - self.lobby_data.current_picker_index

        # ——— AUTO-PICK: if only one player remains, give them to whoever’s turn it is ———
        remaining = [
            m for m in self.lobby_data.players
            if m not in self.lobby_data.captains
            and all(m not in team for team in self.lobby_data.teams.values())
        ]
        if len(remaining) == 1:
            # assign final pick automatically
            self.lobby_data.teams[self.lobby_data.current_picker_index].append(remaining[0])
            return await self.finish_draft(interaction)

        # ——— Otherwise, continue normal draft flow ———
        # Count how many slots are now filled (2 captains + drafted)
        drafted_count = (
            len(self.lobby_data.captains)
            + len(self.lobby_data.teams[0])
            + len(self.lobby_data.teams[1])
        )

        if drafted_count >= 10:
            # all spots filled → finish
            await self.finish_draft(interaction)
        else:
            # clear old view and draw next draft step
            await self.lobby_data.message.edit(view=None)
            view = SideSelect(lobby_data=self.lobby_data)
            await view.send_draft_view(interaction)

    async def finish_draft(self, interaction: Interaction):
        """
        Once all players are chosen:
        - Build final Blue/Red teams (each with its captain + drafted members)
        - Display them side by side, along with OP.GG multi‐links if available
        - Delete the lobby from `custom_lobbies`
        """
        if self.lobby_data.side_selected == 0:
            blue_team = [self.lobby_data.captains[0]] + self.lobby_data.teams[0]
            red_team = [self.lobby_data.captains[1]] + self.lobby_data.teams[1]
        else:
            blue_team = [self.lobby_data.captains[1]] + self.lobby_data.teams[0]
            red_team = [self.lobby_data.captains[0]] + self.lobby_data.teams[1]

        tournament_code = "GENERATED_TOURNAMENT_CODE"

        def build_team_list(members: list[discord.Member]) -> str:
            lines = []
            for m in members:
                opgg = id_to_opgg.get(str(m.id))
                if opgg:
                    lines.append(f"[{m.display_name}]({opgg})")
                else:
                    lines.append(m.display_name)
            return "\n".join(lines) if lines else "*(none)*"
        
        alphabet    = string.ascii_letters + string.digits
        session     = ''.join(secrets.choice(alphabet) for _ in range(8))
        blue_token  = ''.join(secrets.choice(alphabet) for _ in range(8))
        red_token   = ''.join(secrets.choice(alphabet) for _ in range(8))
        spec_token  = ''.join(secrets.choice(alphabet) for _ in range(8))

        base     = f"{DRAFTLOL_BASE_URL}/{session}"
        blue_link = f"{base}/{blue_token}"
        red_link  = f"{base}/{red_token}"
        spec_link = f"{base}/{spec_token}"

        embed = discord.Embed(
            title="TEAMS READY!",
            description=f"Blue Draft: `{blue_link}`\nRed Draft: `{red_link}`\nSpectator: `{spec_link}`",
            color=discord.Color.green()
        )
        embed.add_field(name="Blue Team", value=build_team_list(blue_team), inline=True)
        embed.add_field(name="Red Team", value=build_team_list(red_team), inline=True)

        # Build multi OP.GG links (semicolon‐separated) for each full team if available
        blue_links = ";".join([
            id_to_opgg.get(str(m.id), "") 
            for m in blue_team 
            if id_to_opgg.get(str(m.id))
        ])
        red_links = ";".join([
            id_to_opgg.get(str(m.id), "") 
            for m in red_team 
            if id_to_opgg.get(str(m.id))
        ])
        if blue_links:
            embed.add_field(name="Blue Multi OP.GG", value=blue_links, inline=False)
        if red_links:
            embed.add_field(name="Red Multi OP.GG", value=red_links, inline=False)

        await self.lobby_data.message.edit(embed=embed, view=None)
        # Remove lobby from active dictionary
        del custom_lobbies[self.lobby_data.message.id]
        # Acknowledge the interaction so Discord doesn’t show “This interaction failed”
        await interaction.response.defer()

class CustomsCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(
        name="customs",
        description="Create a League of Legends custom lobby"
    )
    async def create_customs(self, interaction: discord.Interaction):
        # use default max_players=10 in real mode, or same 10 in TEST_MODE
        lobby_data = LobbyData(creator_id=interaction.user.id)
        lobby_data.guild = interaction.guild

        embed = discord.Embed(
            title="League of Legends Customs Lobby",
            description=f"Players (0/{lobby_data.max_players}):",  # MODIFIED: reflect max_players
            color=discord.Color.blue()
        )
        view = CustomsLobbyView(lobby_data)
        await interaction.response.send_message(embed=embed, view=view)
        lobby_data.message = await interaction.original_response()



    @app_commands.command(name="setsumname", description="Set your summoner name for Customs")
    @app_commands.describe(opgg_url="Your summoner name")
    async def set_summonername(self, interaction: discord.Interaction, opgg_url: str):
        id_to_opgg[str(interaction.user.id)] = opgg_url
        await interaction.response.send_message(f"Your summoner name has been set to: {opgg_url}", ephemeral=True)

    @commands.command(name="customs")
    async def customs_prefix(self, ctx: commands.Context):
        """
        Prefix form of /customs. Typing “!!customs” in chat will start the exact same
        lobby workflow (join/leave GUI, captain selection, draft, tournament code).
        """

        class _FakeInteraction:
            def __init__(self, ctx):
                self.user = ctx.author
                self.channel = ctx.channel
                self.guild = ctx.guild
                self.client = ctx.bot
                self._ctx = ctx
                self._saved_message = None

                # Create a "response" attribute that has send_message(...)
                self.response = self._FakeResponse(self)

            class _FakeResponse:
                def __init__(self, parent):
                    self._parent = parent

                async def send_message(self, *args, **kwargs):
                    msg = await self._parent._ctx.send(*args, **kwargs)
                    # Save the bot's sent message for original_response()
                    self._parent._saved_message = msg
                    return msg

            async def original_response(self):
                return self._saved_message

        fake_int = _FakeInteraction(ctx)
        # Invoke the same callback used by the slash command:
        await self.create_customs.callback(self, fake_int)

############################################################################################################
# Cogs
############################################################################################################

# -------------------------------
# RPS Accept View: Handles challenge acceptance with a dynamic 30s countdown and a Decline option.
# -------------------------------

class RPSAcceptView(discord.ui.View):
    def __init__(self, challenger: discord.Member, challenged: discord.Member, wager: int):
        super().__init__(timeout=45)  # Timeout of 45 seconds for acceptance
        self.challenger = challenger
        self.challenged = challenged
        self.wager = wager
        self.deadline = time.time() + 45
        self.challenge_over = False
        self.message = None  # This will be set once we send the challenge message

    async def start_countdown(self, message: discord.Message):
        """Dynamically update the embed's countdown timer every second."""
        self.message = message
        while not self.challenge_over:
            remaining = int(self.deadline - time.time())
            if remaining <= 0:
                break
            # Make a copy of the current embed and update its description.
            embed = message.embeds[0].copy()
            embed.description = (
                f"{self.challenger.mention} challenges {self.challenged.mention} to a Rock Paper Scissors match for **{self.wager}** coins!\n"
                f"{self.challenged.mention}, click **Accept** to play or **Decline** if you don't wish to play.\n"
                f"Time remaining: **{remaining} seconds**"
            )
            try:
                await message.edit(embed=embed, view=self)
            except Exception:
                pass
            await asyncio.sleep(1)
        # When the loop finishes, if challenge hasn't been accepted/declined:
        if not self.challenge_over:
            self.challenge_over = True
            # Refund the challenger.
            await add_income(str(self.challenger.id), self.wager)
            final_embed = message.embeds[0].copy()
            final_embed.description = "Challenge timed out – no response. Match declined."
            try:
                await message.edit(embed=final_embed, view=None)
            except Exception:
                pass

    async def on_timeout(self):
        # As a backup, if the view times out.
        if not self.challenge_over and self.message:
            self.challenge_over = True
            await add_income(str(self.challenger.id), self.wager)
            embed = self.message.embeds[0].copy()
            embed.description = "Challenge timed out – no response. Match declined."
            try:
                await self.message.edit(embed=embed, view=None)
            except Exception:
                pass
            await self.message.channel.send("Challenge timed out – no response. Match declined.")

    @discord.ui.button(label="Accept Challenge", style=discord.ButtonStyle.success, custom_id="rps_accept")
    async def accept(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only allow the challenged to press this button.
        if interaction.user.id != self.challenged.id:
            await interaction.response.send_message("Only the challenged user can respond. (Waiting for response…)", ephemeral=True)
            return
        # Check if the challenged has enough funds.
        if user_balances.get(str(self.challenged.id), 0) < self.wager:
            await interaction.response.send_message("You don't have enough coins to accept this challenge.", ephemeral=True)
            return
        # Deduct funds from challenged.
        success = await deduct_points(str(self.challenged.id), self.wager)
        if not success:
            await interaction.response.send_message("Failed to deduct coins. Challenge cancelled.", ephemeral=True)
            return
        self.challenge_over = True  # Stop the countdown

        # Update the message to announce game start.
        embed = discord.Embed(
            title="Rock Paper Scissors Game",
            description=(f"{self.challenger.mention} vs {self.challenged.mention}\n"
                         f"Each player wagered **{self.wager}** coins.\n"
                         "Make your choice below:"),
            color=discord.Color.blurple()
        )
        await interaction.message.edit(embed=embed, view=RPSGameView(self.challenger, self.challenged, self.wager))
        await interaction.response.send_message("Challenge accepted! Let's play!", ephemeral=True)

    @discord.ui.button(label="Decline", style=discord.ButtonStyle.danger, custom_id="rps_decline")
    async def decline(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Only allow the challenged to decline.
        if interaction.user.id != self.challenged.id:
            await interaction.response.send_message("Only the challenged user can respond. (Waiting for response…)", ephemeral=True)
            return
        self.challenge_over = True  # Stop the countdown

        # Refund the challenger's wager.
        await add_income(str(self.challenger.id), self.wager)
        embed = interaction.message.embeds[0].copy()
        embed.description = "Challenge declined."
        await interaction.message.edit(embed=embed, view=None)
        await interaction.response.send_message("You declined the challenge.", ephemeral=True)
        await interaction.channel.send(f"{self.challenged.mention} declined {self.challenger.mention}'s Rock Paper Scissors challenge!")

# -------------------------------
# RPS Game View: Handles the actual game (Rock, Paper, Scissors choices)
# -------------------------------

class RPSGameView(discord.ui.View):
    def __init__(self, challenger: discord.Member, challenged: discord.Member, wager: int):
        super().__init__(timeout=60)
        self.challenger = challenger
        self.challenged = challenged
        self.wager = wager
        self.choices = {}  # Mapping of user id to their full choice ("Rock", "Paper", "Scissors")
        self.game_ended = False  # Flag to ensure game end happens only once

    async def end_game(self, interaction: discord.Interaction):
        if self.game_ended:
            return
        self.game_ended = True
        
        choice1 = self.choices.get(self.challenger.id)
        choice2 = self.choices.get(self.challenged.id)
        if choice1 is None or choice2 is None:
            return
        total_pot = self.wager * 2
        result_text = ""
        if choice1 == choice2:
            # Tie: refund wagers.
            await add_income(str(self.challenger.id), self.wager)
            await add_income(str(self.challenged.id), self.wager)
            result_text = "It's a tie! Both players get their coins back."
        else:
            win_map = {
                "Rock": "Scissors",
                "Paper": "Rock",
                "Scissors": "Paper"
            }
            winner = self.challenger if win_map[choice1] == choice2 else self.challenged
            result_text = f"{winner.mention} wins and takes **{total_pot}** coins!"
            await add_income(str(winner.id), total_pot)

        for child in self.children:
            child.disabled = True

        result_embed = discord.Embed(
            title="Rock Paper Scissors Result",
            description=(f"{self.challenger.mention} chose **{choice1}** and "
                         f"{self.challenged.mention} chose **{choice2}**.\n\n{result_text}"),
            color=discord.Color.gold()
        )
        # Update the existing message.
        await interaction.message.edit(embed=result_embed, view=self)
        # Send a public announcement.
        await interaction.channel.send(result_embed.description)

        # Wait a few seconds to let players read the result, then delete the interactive message.
        #await asyncio.sleep(1)
        try:
            await interaction.message.delete()
        except Exception as e:
            print(f"Error deleting game message: {e}")

    async def record_choice(self, interaction: discord.Interaction, choice: str):
        if interaction.user.id not in (self.challenger.id, self.challenged.id):
            await interaction.response.send_message("You're not a participant in this game.", ephemeral=True)
            return
        if interaction.user.id in self.choices:
            await interaction.response.send_message("You have already made your choice.", ephemeral=True)
            return
        self.choices[interaction.user.id] = choice
        if len(self.choices) == 2:
            await self.end_game(interaction)

    @discord.ui.button(label="Rock", style=discord.ButtonStyle.primary, custom_id="rps_rock")
    async def rock_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_choice(interaction, "Rock")

    @discord.ui.button(label="Paper", style=discord.ButtonStyle.primary, custom_id="rps_paper")
    async def paper_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_choice(interaction, "Paper")

    @discord.ui.button(label="Scissors", style=discord.ButtonStyle.primary, custom_id="rps_scissors")
    async def scissors_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.record_choice(interaction, "Scissors")

# -------------------------------
# RPS Cog: The command to start an RPS challenge.
# -------------------------------

class RPSCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @app_commands.command(name="rps", description="Challenge someone to Rock Paper Scissors for coins!")
    @app_commands.describe(target="The member to challenge", amount="The wager amount in coins")
    async def rps(self, interaction: discord.Interaction, target: discord.Member, amount: int):
        challenger = interaction.user
        wager = amount
        if target.id == challenger.id:
            await interaction.response.send_message("You cannot challenge yourself!", ephemeral=True)
            return
        if user_balances.get(str(challenger.id), 0) < wager:
            await interaction.response.send_message("You don't have enough coins to wager that amount.", ephemeral=True)
            return
        # Deduct the wager from the challenger.
        success = await deduct_points(str(challenger.id), wager)
        if not success:
            await interaction.response.send_message("Failed to deduct wager from your balance.", ephemeral=True)
            return
        # Create the challenge embed with initial countdown.
        embed = discord.Embed(
            title="Rock Paper Scissors Challenge",
            description=(
                f"{challenger.mention} challenges {target.mention} to a Rock Paper Scissors match for **{wager}** coins!\n"
                f"{target.mention}, click **Accept** to play or **Decline** if you don't wish to play.\n"
                f"Time remaining: **30 seconds**"
            ),
            color=discord.Color.orange()
        )
        view = RPSAcceptView(challenger, target, wager)
        msg = await interaction.response.send_message(embed=embed, view=view)
        # For slash commands the original response can be retrieved with:
        challenge_msg = await interaction.original_response()
        view.message = challenge_msg
        # Start the countdown task.
        asyncio.create_task(view.start_countdown(challenge_msg))
        # Also announce publicly.
        await interaction.channel.send(f"{challenger.mention} has challenged {target.mention} to Rock Paper Scissors for **{wager}** coins!")

    @commands.command(name="rps")
    async def rps_command(self, ctx: commands.Context, target: discord.Member, amount: int):
        challenger = ctx.author
        wager = amount
        if target.id == challenger.id:
            await ctx.send("You cannot challenge yourself!")
            return
        if user_balances.get(str(challenger.id), 0) < wager:
            await ctx.send("You don't have enough coins to wager that amount.")
            return
        success = await deduct_points(str(challenger.id), wager)
        if not success:
            await ctx.send("Failed to deduct wager from your balance.")
            return
        embed = discord.Embed(
            title="Rock Paper Scissors Challenge",
            description=(
                f"{challenger.mention} challenges {target.mention} to a Rock Paper Scissors match for **{wager}** coins!\n"
                f"{target.mention}, click **Accept** to play or **Decline** if you don't wish to play.\n"
                f"Time remaining: **30 seconds**"
            ),
            color=discord.Color.orange()
        )
        view = RPSAcceptView(challenger, target, wager)
        msg = await ctx.send(embed=embed, view=view)
        view.message = msg
        asyncio.create_task(view.start_countdown(msg))
        await ctx.send(f"{challenger.mention} has challenged {target.mention} to Rock Paper Scissors for **{wager}** coins!")


class AdminRollCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # IDs from the requirements
        self.guild_id = 1287144786452680744
        self.channel_id = 1363018084214112426
        self.role_id = 1346706983780225045
        self.admin_candidates_file = "admin_roll.txt"
        # Schedule the background loop
        self.bot.loop.create_task(self.admin_roll_loop())
        # Also check immediately on startup if a roll is needed.
        self.bot.loop.create_task(self.check_admin_roll_on_startup())

    async def check_admin_roll_on_startup(self):
        now_local = datetime.now(LOCAL_TIMEZONE)
        last_friday_midnight = self.get_last_friday_midnight(now_local)
        last_roll = await self.load_last_admin_roll()
        if last_roll is None or last_roll < last_friday_midnight.timestamp():
            print("Performing missed admin roll on startup...")
            await self.perform_admin_roll()

    def get_last_friday_midnight(self, current):
        # Friday is weekday=4 (Monday=0, Sunday=6)
        days_since_friday = (current.weekday() - 4) % 7
        last_friday_date = current.date() - timedelta(days=days_since_friday)
        # Midnight local (00:00) on that Friday:
        return datetime.combine(last_friday_date, dtime.min, tzinfo=LOCAL_TIMEZONE)

    def get_next_friday_midnight(self, current):
        # Calculate days until next Friday:
        days_ahead = (4 - current.weekday()) % 7
        if days_ahead == 0 and current.time() >= dtime.min:
            days_ahead = 7
        next_friday_date = current.date() + timedelta(days=days_ahead)
        return datetime.combine(next_friday_date, dtime.min, tzinfo=LOCAL_TIMEZONE)

    async def admin_roll_loop(self):
        while True:
            now_local = datetime.now(LOCAL_TIMEZONE)
            next_friday_midnight = self.get_next_friday_midnight(now_local)
            wait_seconds = (next_friday_midnight - now_local).total_seconds()
            print(f"Waiting {wait_seconds} seconds until next admin roll at {next_friday_midnight}.")
            await asyncio.sleep(wait_seconds)
            await self.perform_admin_roll()

    async def perform_admin_roll(self):
        print("Performing admin roll...")
        # Read candidates from admin_roll.txt
        try:
            async with aiofiles.open(self.admin_candidates_file, "r") as f:
                content = await f.read()
            candidates = [line.strip() for line in content.splitlines() if line.strip()]
        except Exception as e:
            print(f"Error reading admin candidates file: {e}")
            return
        current_time = time.time()

        # Load the last admin times (if a candidate has never been admin, default to 0)
        last_admin_times = await self.load_last_admin_times()

        # Weighted selection: calculate weights so that candidates with a longer gap since last admin have a higher chance.
        selected = []
        available = candidates.copy()
        RECOVERY_WEEKS = 8                              # fully back to 4% after 4 weeks
        MIN_PROB = 0.01                                 # 1% floor
        N = len(available)                              # e.g. 25 candidates
        floor_wt = MIN_PROB * (N - 1) / (1 - MIN_PROB)   # ≈0.2424

        for _ in range(3):
            weights = []
            for uid in available:
                last_time = last_admin_times.get(uid, 0)
                weeks = (current_time - last_time) / (7*24*3600)
                # recovery factor: 0.0 at just-rolled, 1.0 after RECOVERY_WEEKS
                factor = min(weeks / RECOVERY_WEEKS, 1.0)
                # linear blend from floor_wt up to 1.0
                w = floor_wt + (1.0 - floor_wt) * factor
                weights.append(w)
            choice = random.choices(available, weights=weights, k=1)[0]
            selected.append(choice)
            available.remove(choice)

        if len(selected) < 3:
            print("Not enough admin candidates available.")
            return
        # Update last admin times for the selected candidates so their chances reset
        for uid in selected:
            last_admin_times[uid] = current_time
        await self.save_last_admin_times(last_admin_times)

        # Get the guild, channel, and role objects
        guild = self.bot.get_guild(self.guild_id)
        if not guild:
            print("Guild not found")
            return
        channel = self.bot.get_channel(self.channel_id)
        if not channel:
            print("Announcement channel not found")
            return
        role = guild.get_role(self.role_id)
        if not role:
            print("Admin role not found")
            return
        # Remove the admin role from last week's admins
        last_admins = await self.load_last_admins()
        for uid in last_admins:
            member = guild.get_member(int(uid))
            if member and role in member.roles:
                try:
                    await member.remove_roles(role)
                    print(f"Removed role from member {uid}")
                except Exception as e:
                    print(f"Error removing role from {uid}: {e}")

        # Assign the role to the newly selected admins
        for uid in selected:
            member = guild.get_member(int(uid))
            if member:
                try:
                    await member.add_roles(role)
                    print(f"Added role to member {uid}")
                except Exception as e:
                    print(f"Error adding role to {uid}: {e}")

        # Save the new admins for next week’s removal
        await self.save_last_admins(selected)
        # Record the time of this admin roll (used for scheduling)
        await self.save_last_admin_roll(current_time)

        # Create and send the announcement message
        mentions = " ".join(f"<@{uid}>" for uid in selected)
        embed = discord.Embed(
            title="**🔥NEW BUSINESS ADMIN ROLLS🔥**",
            description=f"Got some new white collar workers for this week, Mon\n\n{mentions}\n\n",
            color=discord.Color.purple()
        )
        embed.set_footer(text="Have fun goobers")
        await channel.send(embed=embed)
        await channel.send("<@506202193746198568>")
        print("Admin roll complete.")

    # New helper functions for storing candidate last admin times
    async def load_last_admin_times(self):
        try:
            async with aiofiles.open("admin_last_times.json", "r") as f:
                data = await f.read()
                return json.loads(data)
        except Exception:
            return {}

    async def save_last_admin_times(self, times):
        async with aiofiles.open("admin_last_times.json", "w") as f:
            await f.write(json.dumps(times))

    # The following helper functions remain unchanged.
    async def load_last_admins(self):
        try:
            async with aiofiles.open("last_admins.json", "r") as f:
                data = await f.read()
                return json.loads(data)
        except Exception:
            return []

    async def save_last_admins(self, admins):
        async with aiofiles.open("last_admins.json", "w") as f:
            await f.write(json.dumps(admins))

    async def load_last_admin_roll(self):
        try:
            async with aiofiles.open("last_admin_roll.txt", "r") as f:
                data = await f.read()
                return float(data.strip())
        except Exception:
            return None

    async def save_last_admin_roll(self, timestamp):
        async with aiofiles.open("last_admin_roll.txt", "w") as f:
            await f.write(str(timestamp))





async def setup(bot):
    await bot.add_cog(SummaryCog(bot))
    await bot.add_cog(GeneralCog(bot))
    await bot.add_cog(ShopCog(bot))
    await bot.add_cog(RPSCog(bot))

# Run the bot
bot.run(DISCORD_TOKEN)


