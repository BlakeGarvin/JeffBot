import discord
import openai
import asyncio
import aiofiles
import os
import json
import time
import sys
import math
import random
import copy
import secrets, string
from dotenv import load_dotenv
from discord.ext import commands
from discord import ButtonStyle, app_commands, Interaction
from datetime import datetime, timedelta, timezone
from datetime import time as dtime  # for midnight
from openai import AsyncOpenAI
from urllib.parse import quote_plus  # NEW: for URL‐encoding summoner names
from typing import Optional
import tiktoken
import re
import random

load_dotenv()
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

# List of channel IDs to collect messages from
TARGET_CHANNEL_IDS = [753959443263389737, 781309198855438336]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Update the file paths to use absolute paths
MESSAGES_FILE = os.path.join(SCRIPT_DIR, 'user_messages.json')
SUMMARY_FILE = os.path.join(SCRIPT_DIR, 'user_summary.txt')
BALANCES_FILE = os.path.join(SCRIPT_DIR, 'user_balances.json')
DAILY_COOLDOWN_FILE = os.path.join(SCRIPT_DIR, 'daily_cooldowns.json')


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



_ignore_state = {"ignored": [], "cooldowns": {}}  # {"ignored":[str(user_id),...], "cooldowns": {str(user_id): last_toggle_epoch}}
_spam_state = {"punished_until": {}, "recent": {}}  # punished_until: {str(user_id): epoch}, recent: {str(user_id): [epochs]}

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
        content = normalize_visible_ats(normalize_mentions_raw(m.content.strip()))
        msgs.append(f"[{m.author.display_name}] {MENTION_SEP} {content}")
    return format_block(f"RECENT_CONTEXT_LAST_{int(delta.total_seconds())}_SECS", msgs)

def format_reply_chain_block(chain_msgs: list[discord.Message]) -> str:
    lines: list[str] = []
    for m in chain_msgs:
        if not m.content:
            continue
        content = normalize_visible_ats(normalize_mentions_raw(m.content.strip()))
        lines.append(f"[{m.author.display_name}] {MENTION_SEP} {content}")
    return format_block(f"REPLY_CHAIN_LEN_{len(lines)}", lines)



_ready_synced = False

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
    stripped = normalize_visible_ats(normalize_mentions_raw(stripped))

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

    reply_text = normalize_visible_ats(normalize_mentions_raw(reply_text))

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
    reply = normalize_visible_ats(normalize_mentions_raw(reply))
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
        response = normalize_visible_ats(normalize_mentions_raw(response))
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



