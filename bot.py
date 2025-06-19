import os
import random
import re
import asyncio
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from telethon import TelegramClient, events, Button
from telethon.sessions import StringSession
from telethon.errors import (
    SessionPasswordNeededError, FloodWaitError, UserNotParticipantError, ReactionInvalidError,
    InviteHashExpiredError, UserAlreadyParticipantError, InviteHashInvalidError,
    ChannelsTooMuchError, ChannelInvalidError
)
from telethon.tl.functions.messages import SendReactionRequest, GetMessagesViewsRequest, ImportChatInviteRequest
from telethon.tl.functions.channels import JoinChannelRequest, LeaveChannelRequest
from telethon.tl.types import InputPeerChannel, ReactionEmoji

# Simple print function without logging
def safe_print(text):
    try:
        print(text)
    except UnicodeEncodeError:
        print(text.encode('ascii', errors='replace').decode('ascii'))

# Load environment variables
load_dotenv()

# Check for required environment variables
required_vars = {
    "API_ID": "Telegram API ID (get from my.telegram.org)",
    "API_HASH": "Telegram API hash (get from my.telegram.org)",
    "BOT_TOKEN": "Bot token (get from @BotFather)",
    "OWNER_IDS": "Your Telegram user ID (get from @userinfobot)"
}
missing_vars = [var for var, desc in required_vars.items() if not os.getenv(var)]

if missing_vars:
    error_msg = "❌ Missing required environment variables in .env file:\n"
    for var in missing_vars:
        error_msg += f"- {var}: {required_vars[var]}\n"
    error_msg += "Please create or update the .env file with these values and try again."
    safe_print(error_msg)
    exit(1)

try:
    API_ID = int(os.getenv("API_ID"))
except ValueError:
    safe_print("❌ API_ID must be a valid integer. Check your .env file.")
    exit(1)

API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
try:
    OWNER_IDS = set(map(int, os.getenv("OWNER_IDS").split(',')))
except (ValueError, AttributeError):
    safe_print("❌ OWNER_IDS must be a comma-separated list of integers. Check your .env file.")
    exit(1)

# Validate delay settings
for var in ["JOIN_SECONDS", "LEAVE_SECONDS", "REACT_SECONDS", "VIEW_SECONDS", "STATE_TIMEOUT"]:
    try:
        value = float(os.getenv(var, "1"))
        if value < 0:
            raise ValueError(f"{var} must be non-negative")
    except ValueError:
        safe_print(f"❌ {var} must be a valid non-negative number")
        exit(1)

# Directories for session, sudo user, and users files
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)
SUDO_USERS_FILE = SESSIONS_DIR / "sudo_users.json"
USERS_FILE = SESSIONS_DIR / "users.json"

# Delay settings with defaults
JOIN_DELAY = float(os.getenv("JOIN_SECONDS", "1"))
LEAVE_DELAY = float(os.getenv("LEAVE_SECONDS", "1"))
REACT_DELAY = float(os.getenv("REACT_SECONDS", "1"))
VIEW_DELAY = float(os.getenv("VIEW_SECONDS", "0.5"))
STATE_TIMEOUT = float(os.getenv("STATE_TIMEOUT", "300"))  # 5 minutes timeout for states

# Global variables
user_clients = {}  # {phone: (client, session_string)}
flood_wait_until = {}
EMOJIS = [e.strip() for e in os.getenv("EMOJIS", "👍,🔥,👏,🎉,😍").split(",") if e.strip()]
if not EMOJIS:
    EMOJIS = ["👍,","👏","🎉","😍,"]
    safe_print("⚠ No valid emojis in EMOJIS env variable; using defaults.")
cancellation_events = {}
user_states = {}
reaction_history = {}  # {message_link: {phone: set(used_emojis)}}
user_roles = {}  # {user_id: {'role': 'co-owner'/'admin', 'promoted_by': user_id, 'promoted_at': timestamp}}
known_users = set()  # Set of user IDs who have interacted with the bot

def save_sudo_users():
    """Save sudo users to JSON file."""
    try:
        with SUDO_USERS_FILE.open('w') as f:
            json.dump(user_roles, f, default=str)
        safe_print(f"✅ Saved sudo users to {SUDO_USERS_FILE}")
    except Exception as e:
        safe_print(f"❌ Failed to save sudo users: {e}")

def load_sudo_users():
    """Load sudo users from JSON file."""
    global user_roles
    if SUDO_USERS_FILE.exists():
        try:
            with SUDO_USERS_FILE.open('r') as f:
                loaded_data = json.load(f)
            # Convert string keys to integers and ensure data structure
            user_roles.update({
                int(k): {
                    'role': v['role'],
                    'promoted_by': int(v['promoted_by']),
                    'promoted_at': v['promoted_at']
                } for k, v in loaded_data.items()
            })
            safe_print(f"✅ Loaded {len(user_roles)} sudo users from {SUDO_USERS_FILE}")
        except Exception as e:
            safe_print(f"❌ Failed to load sudo users: {e}")

def save_known_users():
    """Save known users to JSON file."""
    try:
        with USERS_FILE.open('w') as f:
            json.dump(list(known_users), f)
        safe_print(f"✅ Saved {len(known_users)} known users to {USERS_FILE}")
    except Exception as e:
        safe_print(f"❌ Failed to save known users: {e}")

def load_known_users():
    """Load known users from JSON file."""
    global known_users
    if USERS_FILE.exists():
        try:
            with USERS_FILE.open('r') as f:
                loaded_users = json.load(f)
            known_users.update(map(int, loaded_users))
            safe_print(f"✅ Loaded {len(known_users)} known users from {USERS_FILE}")
        except Exception as e:
            safe_print(f"❌ Failed to load known users: {e}")

def has_sudo_access(sender_id):
    if sender_id in OWNER_IDS:
        return True
    role = user_roles.get(sender_id, {}).get('role')
    return role in ['co-owner', 'admin']

def access_control(allow_admin=True, allow_remove=False, allow_member_management=True):
    def decorator(func):
        async def wrapper(event, *args, **kwargs):
            sender_id = event.sender_id
            if sender_id in OWNER_IDS:
                return await func(event, *args, **kwargs)
            role = user_roles.get(sender_id, {}).get('role')
            # Restrict Member Management access for admins
            if not allow_member_management and func.__name__ in [
                'start_count_link_command', 'start_link_only_command',
                'show_member_management', 'receive_invite_link', 'process_join', 'process_leave'
            ] and role == 'admin':
                await event.reply("❌ Admins are not authorized to access Member Management.")
                return
            # Restrict Account Management features (except Add Account) for admins
            if func.__name__ in ['listaccounts', 'info'] and role == 'admin':
                await event.reply("❌ Admins are only authorized to add accounts in Account Management.")
                return
            if role == 'co-owner' and (allow_remove or not event.data.startswith(b"removeaccount")):
                return await func(event, *args, **kwargs)
            if role == 'admin' and allow_admin and (allow_remove or not event.data.startswith(b"removeaccount")):
                return await func(event, *args, **kwargs)
            await event.reply("❌ You are not authorized to use this command.")
        return wrapper
    return decorator

def normalize_phone(phone: str) -> str:
    phone = phone.strip()
    if phone.startswith('+'):
        return '+' + ''.join(c for c in phone[1:] if c.isdigit())
    return '+' + ''.join(c for c in phone if c.isdigit())

def extract_message_info(message_link: str):
    pattern = r"https://t\.me/(c/|)(@?[\w\d_-]+|\d+)/(\d+)"
    match = re.match(pattern, message_link)
    if not match:
        safe_print(f"Invalid message link format: {message_link}. Expected format: https://t.me/[c/]CHAT_ID/MESSAGE_ID")
        return None, None
    prefix = match.group(1)
    chat_identifier = match.group(2)
    message_id = int(match.group(3))
    if prefix == 'c/':
        chat_identifier = f"-100{chat_identifier}"
    return chat_identifier, message_id

async def ensure_connected(client, phone, retries=3, delay=5):
    for attempt in range(retries):
        try:
            if not client.is_connected():
                await client.connect()
            return True
        except Exception as e:
            safe_print(f"❌ Connection attempt {attempt + 1}/{retries} failed for {phone}: {e}")
            if attempt < retries - 1:
                await asyncio.sleep(delay)
    safe_print(f"❌ Failed to connect client {phone} after {retries} attempts")
    return False

async def respect_flood_limit(phone, delay=0):
    if phone in flood_wait_until:
        wait_until = flood_wait_until[phone]
        current_time = asyncio.get_event_loop().time()
        if current_time < wait_until:
            await asyncio.sleep(wait_until - current_time)
    if delay > 0:
        await asyncio.sleep(delay)

async def show_main_menu(event):
    sender_id = event.sender_id
    role = user_roles.get(sender_id, {}).get('role') if sender_id not in OWNER_IDS else 'owner'
    
    if sender_id in OWNER_IDS or role == 'co-owner':
        # Full access for owners and co-owners
        menu_message = "🌟 **Telegram Meta Bot** 🌟\n\nSelect an option below:"
        buttons = [
            [Button.inline("🔐 Account Management", b"account_management")],
            [Button.inline("👥 Member Management", b"member_management")],
            [Button.inline("❤️ Reaction Management", b"reaction_management")],
            [Button.inline("👀 View Management", b"view_management")],
            [Button.inline("🔑 Sudo Access", b"sudo_access")],
            [Button.inline("❓ Help", b"help")]
        ]
        await event.reply(menu_message, buttons=buttons, parse_mode='Markdown')
    elif role == 'admin':
        # Limited access for admins: Add Account, Reaction Management, and View Management only
        menu_message = "🌟 **Telegram Meta Bot** 🌟\n\nSelect an option below:"
        buttons = [
            [Button.inline("➕ Add Account", b"addaccount")],
            [Button.inline("❤️ Reaction Management", b"reaction_management")],
            [Button.inline("👀 View Management", b"view_management")],
            [Button.inline("❓ Help", b"help")]
        ]
        await event.reply(menu_message, buttons=buttons, parse_mode='Markdown')
    else:
        # Non-sudo users: only Add Account
        await event.reply(
            "🌟 **Telegram Meta Bot** 🌟\n\nYou can only add accounts. Click below to proceed:",
            buttons=[[Button.inline("➕ Add Account", b"addaccount")]],
            parse_mode='Markdown'
        )

async def show_account_management(event):
    sender_id = event.sender_id
    role = user_roles.get(sender_id, {}).get('role') if sender_id not in OWNER_IDS else 'owner'
    
    message = "🔐 **Account Management**\n\nChoose an action:"
    if role == 'admin':
        # Admins can only add accounts
        buttons = [
            [Button.inline("➕ Add Account", b"addaccount")],
            [Button.inline("🔙 Back", b"main_menu")]
        ]
    else:
        # Owners and co-owners have full access
        buttons = [
            [Button.inline("➕ Add Account", b"addaccount")],
            [Button.inline("📋 List Accounts", b"listaccounts")],
            [Button.inline("ℹ Info", b"info")]
        ]
        if sender_id in OWNER_IDS:
            buttons.append([Button.inline("➖ Remove Account", b"removeaccount")])
        buttons.append([Button.inline("🔙 Back", b"main_menu")])
    await event.reply(message, buttons=buttons, parse_mode='Markdown')

@access_control(allow_admin=True, allow_member_management=False)
async def show_member_management(event):
    sender_id = event.sender_id
    role = user_roles.get(sender_id, {}).get('role') if sender_id not in OWNER_IDS else 'owner'
    if role not in ['owner', 'co-owner']:
        await event.reply("❌ You are not authorized to access Member Management.")
        return
    message = "👥 **Member Management**\n\nChoose an action:"
    buttons = [
        [Button.inline("📥 Mega Join", b"mega_join")],
        [Button.inline("📥 Join", b"join")],
        [Button.inline("📤 Mega Leave", b"mega_leave")],
        [Button.inline("📤 Leave", b"leave")],
        [Button.inline("🔙 Back", b"main_menu")]
    ]
    await event.reply(message, buttons=buttons, parse_mode='Markdown')

async def show_reaction_management(event):
    message = "❤️ **Reaction Management**\n\nChoose an action:"
    buttons = [
        [Button.inline("❤️ Mega React", b"mega_react")],
        [Button.inline("❤️ React", b"react")],
        [Button.inline("🔙 Back", b"main_menu")]
    ]
    await event.reply(message, buttons=buttons, parse_mode='Markdown')

async def show_view_management(event):
    message = "👀 **View Management**\n\nChoose an action:"
    buttons = [
        [Button.inline("👀 Mega View", b"mega_view")],
        [Button.inline("👀 View", b"view")],
        [Button.inline("🔙 Back", b"main_menu")]
    ]
    await event.reply(message, buttons=buttons, parse_mode='Markdown')

async def show_help_message(event):
    sender_id = event.sender_id
    role = user_roles.get(sender_id, {}).get('role') if sender_id not in OWNER_IDS else 'owner'
    help_message = (
        "🌟 **Telegram Meta Bot Help** 🌟\n\n"
        "Welcome to the Telegram Meta Bot! This bot helps you manage multiple Telegram accounts to automate tasks like reacting to messages and adding views.\n\n"
        "📌 **Available Commands**:\n"
        "- `/start`: Display the main menu.\n"
        "- `/help`: Show the main menu.\n"
        "- `/cancel`: Cancel an ongoing operation (e.g., reacting, joining).\n"
        "- `/listsudo`: List all owners and sudo users with their roles and promoters (sudo users only).\n\n"
        "🔧 **Menu Sections**:\n"
        "- **🔐 Account Management**:\n"
        "  - Add Account: Add a new Telegram account.\n"
    )
    if role in ['owner', 'co-owner']:
        help_message += (
            "  - List Accounts: List all added accounts.\n"
            "  - Info: Get detailed account info.\n"
            "  - Remove Account: Delete an account (owners only).\n"
        )
    if role in ['owner', 'co-owner', 'admin']:
        help_message += (
            "- **❤️ Reaction Management**:\n"
            "  - Mega React: React to message(s) with all accounts.\n"
            "  - React: React with selected accounts.\n"
            "- **👀 View Management**:\n"
            "  - Mega View: Add views to message(s) with all accounts.\n"
            "  - View: Add views with selected accounts.\n"
        )
    if role in ['owner', 'co-owner']:
        help_message += (
            "- **👥 Member Management** (Owners/Co-Owners only):\n"
            "  - Mega Join: Join channel(s) with all accounts.\n"
            "  - Join: Join with selected accounts.\n"
            "  - Mega Leave: Leave channel(s) with all accounts.\n"
            "  - Leave: Leave with selected accounts.\n"
        )
    help_message += (
        "- **🔑 Sudo Access**:\n"
        "  - Add Sudo User: Grant co-owner or admin access (owners and co-owners only).\n"
        "  - Remove Sudo User: Revoke sudo access.\n"
        "  - List Sudo Users: View all sudo users with roles and promoters.\n\n"
        "🔒 **Access**: Restricted to authorized users. Owners can manage all features; co-owners and admins have limited access.\n"
        "🚀 Navigate using the menu buttons below!"
    )
    await event.reply(
        help_message,
        buttons=[[Button.inline("🔙 Back", b"main_menu")]],
        parse_mode='Markdown'
    )

async def show_sudo_list(event):
    client = event.client
    message = "🔑 **Sudo Users List**\n\n"
    
    # List owners
    owner_count = len(OWNER_IDS)
    owner_lines = []
    for owner_id in OWNER_IDS:
        try:
            user = await client.get_entity(owner_id)
            identifier = f"@{user.username}" if user.username else f"ID: {owner_id}"
            owner_lines.append(f"- {identifier} (Rank: Owner)")
        except Exception:
            owner_lines.append(f"- ID: {owner_id} (Rank: Owner)")
    
    if owner_lines:
        message += f"👑 **Owners** ({owner_count}):\n" + "\n".join(owner_lines) + "\n\n"
    
    # List sudo users
    if not user_roles:
        message += "No sudo users added yet."
    else:
        sudo_count = len(user_roles)
        co_owner_lines = []
        admin_lines = []
        for user_id, data in user_roles.items():
            role = data['role']
            promoted_by = data['promoted_by']
            promoted_at = datetime.fromisoformat(data['promoted_at']).strftime("%Y-%m-%d %H:%M:%S")
            rank = "Co-Owner" if role == 'co-owner' else "Admin"
            try:
                user = await client.get_entity(user_id)
                user_identifier = f"@{user.username}" if user.username else f"ID: {user_id}"
            except Exception:
                user_identifier = f"ID: {user_id}"
            try:
                promoter = await client.get_entity(promoted_by)
                promoter_identifier = f"@{promoter.username}" if promoter.username else f"ID: {promoted_by}"
            except Exception:
                promoter_identifier = f"ID: {promoted_by}"
            line = f"- {user_identifier} (Rank: {rank}, Promoted by: {promoter_identifier}, At: {promoted_at})"
            if role == 'co-owner':
                co_owner_lines.append(line)
            else:
                admin_lines.append(line)
        
        if co_owner_lines:
            message += f"👑 **Co-Owners** ({len(co_owner_lines)}):\n" + "\n".join(co_owner_lines) + "\n\n"
        if admin_lines:
            message += f"🛡️ **Admins** ({len(admin_lines)}):\n" + "\n".join(admin_lines) + "\n"
        
        message += f"\n📊 **Total Sudo Users**: {sudo_count}\n"
    
    buttons = [
        [Button.inline("➕ Add Sudo User", b"add_sudo_user")],
        [Button.inline("➖ Remove Sudo User", b"remove_sudo_user")],
        [Button.inline("🔙 Back", b"main_menu")]
    ]
    
    await event.reply(message, buttons=buttons, parse_mode='Markdown')

@access_control(allow_admin=True, allow_member_management=False)
async def start_count_link_command(event, command):
    user_states[event.sender_id] = {'command': command, 'state': 'counting', 'timestamp': asyncio.get_event_loop().time()}
    max_accounts = len(user_clients)
    
    if max_accounts == 0:
        await event.reply("❌ No accounts added yet. Add accounts via Account Management.", buttons=[[Button.inline("🔙 Back", b"main_menu")]], parse_mode='Markdown')
        return
    
    await event.reply(
        f"🚚 How many accounts to use? Enter a number between 1 and {max_accounts}:",
        parse_mode='Markdown'
    )

@access_control(allow_admin=True, allow_member_management=False)
async def start_link_only_command(event, command):
    if not user_clients:
        await event.reply("❌ No accounts added yet. Add accounts via Account Management.", buttons=[[Button.inline("🔙 Back", b"main_menu")]], parse_mode='Markdown')
        return
    
    user_states[event.sender_id] = {
        'command': command,
        'state': 'invite_link' if command in ['mega_join', 'mega_leave'] else 'message_link',
        'count': len(user_clients),
        'timestamp': asyncio.get_event_loop().time()
    }
    
    prompt = (
        "📋 Enter channel link(s) (e.g., https://t.me/ChannelName or https://t.me/+abcd1234, comma-separated):\n"
        "Or type /cancel to cancel.\n"
        if command in ['mega_join', 'mega_leave'] else
        "📜 Send message link(s) (e.g., https://t.me/channel/123, comma-separated):\n"
        "Or type /cancel to cancel.\n"
    )
    await event.reply(prompt, parse_mode='Markdown')

@access_control(allow_admin=False, allow_remove=True)
async def start_remove_phone_command(event):
    user_states[event.sender_id] = {
        'command': 'removeaccount',
        'state': 'remove_phone',
        'timestamp': asyncio.get_event_loop().time()
    }
    await event.reply(
        "📱 Enter the phone number to remove (e.g., +1234567890):",
        parse_mode='Markdown'
    )

async def receive_number(event):
    try:
        count = int(event.message.text)
        if count < 1 or count > len(user_clients):
            await event.reply(f"❌ Please enter a valid number between 1 and {len(user_clients)}.")
            return
        user_states[event.sender_id]['count'] = count
        command = user_states[event.sender_id]['command']
        state = 'invite_link' if command in ['join', 'leave'] else 'message_link'
        user_states[event.sender_id]['state'] = state
        user_states[event.sender_id]['timestamp'] = asyncio.get_event_loop().time()
        prompt = (
            "📋 Enter channel link(s) (e.g., https://t.me/ChannelName or https://t.me/+joinName, comma-separated):\n"
            "Or type /cancel to cancel.\n"
            if state == 'invite_link' else
            "📜 Send message link(s) (e.g., https://t.me/channel/123, comma-separated):\n"
            "Or type /cancel to cancel.\n"
        )
        await event.reply(prompt, parse_mode='Markdown')
    except ValueError:
        await event.reply("❌ Invalid input. Please enter a valid number (e.g., 5).")

@access_control(allow_admin=True, allow_member_management=False)
async def receive_invite_link(event):
    invite_links = [link.strip() for link in event.message.text.strip().split(',')]
    valid_links = [link for link in invite_links if link.startswith('https://t.me/')]
    invalid_links = [link for link in invite_links if not link.startswith('https://t.me/')]

    if not valid_links:
        await event.reply("❌ All links are invalid. Links must start with 'https://t.me/'. Try again or use /cancel.")
        return

    if invalid_links:
        await event.reply(f"⚠️ Skipped invalid links: {', '.join(invalid_links)}")

    command = user_states[event.sender_id].get('command')
    count = user_states[event.sender_id].get('count', len(user_clients))
    results = []

    for link in valid_links:
        if command in ['join', 'mega_join']:
            result = await process_join(event, count, link)
            results.append(f"Channel {link}:\n{result}\n")
        elif command in ['leave', 'mega_leave']:
            result = await process_leave(event, count, link)
            results.append(f"Channel {link}:\n{result}\n")

    await event.reply(
        "\n".join(results),
        buttons=[[Button.inline("🔚 Back", b"main_menu")]],
        parse_mode='Markdown'
    )
    user_states.pop(event.sender_id, None)

async def receive_msg_link(event):
    msg_links = [link.strip() for link in event.message.text.strip().split(',')]
    valid_links = [link for link in msg_links if re.match(r'https://t\.me/(c/|)(@?[\w\d_-]+|\d+)/(\d+)', link)]
    invalid_links = [link for link in msg_links if not re.match(r'https://t\.me/(c/|)(@?[\w\d_-]+|\d+)/(\d+)', link)]

    if not valid_links:
        await event.reply(
            "❌ No valid message links found. Use format:\n"
            "- Public: https://t.me/ChannelName/1234567890\n"
            "- Private: https://t.me/c/1234567890/123\n"
            "Try again or use /cancel to cancel.\n"
        )
        return

    if invalid_links:
        await event.reply(f"⚠️ Skipped the following invalid links: {', '.join(invalid_links)}")

    command = user_states[event.sender_id].get('command')
    count = user_states[event.sender_id].get('count', len(user_clients))
    results = []

    for link in valid_links:
        if command in ['react', 'mega_react']:
            result = await process_react(event, count, link)
            results.append(f"Message {link}:\n{result}\n")
        elif command in ['view', 'mega_view']:
            result = await process_view(event, count, link)
            results.append(f"Message {link}:\n{result}\n")

    await event.reply(
        "\n".join(results),
        buttons=[[Button.inline("🔚 Back", b"main_menu")]],
        parse_mode='Markdown'
    )
    user_states.pop(event.sender_id, None)

async def receive_remove_phone(event):
    user_id = event.sender_id
    raw_phone = event.message.text.strip()
    phone = normalize_phone(raw_phone)
    
    if not re.match(r'^\+\d{7,15}$', phone):
        await event.reply(
            "❌ Invalid phone number format. It must start with '+' and contain 7-15 digits (e.g., +1234567890).",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        return
    
    if phone not in user_clients:
        await event.reply(
            f"❌ Account {phone} not found.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)
        return
    
    client, _ = user_clients[phone]
    try:
        await client.disconnect()
        del user_clients[phone]
        session_file = SESSIONS_DIR / f"{phone}.session"
        if session_file.exists():
            session_file.unlink()
        await event.reply(
            f"✅ Account {phone} removed successfully.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        safe_print(f"Removed account {phone} and deleted session file {session_file}")
    except Exception as e:
        await event.reply(
            f"❌ Failed to remove account {phone}: {str(e)}",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        safe_print(f"Error removing account {phone}: {e}")
    user_states.pop(user_id, None)

async def receive_sudo_user_id(event):
    user_id = event.sender_id
    try:
        new_sudo_id = int(event.message.text.strip())
        if new_sudo_id in OWNER_IDS:
            await event.reply(
                "❌ This user is already an owner and has full access.",
                buttons=[[Button.inline("🔚 Back", b"main_menu")]],
                parse_mode='Markdown'
            )
            user_states.pop(user_id, None)
            return
        user_states[user_id].update({
            'new_user_id': new_sudo_id,
            'state': 'sudo_role',
            'timestamp': user_id
        })
        role = user_roles.get(user_id, {}).get('role')
        buttons = [[Button.inline("Admin", b"set_admin")]]
        if user_id in OWNER_IDS:
            buttons.insert(0, [Button.inline("Co-Owner", b"set_co_owner")])
        buttons.append([Button.inline("Cancel", b"cancel_sudo")])
        await event.reply(
            "🔑 Select the role for this user:",
            buttons=buttons,
            parse_mode='Markdown'
        )
    except ValueError as e:
        await event.reply(
            "❌ Invalid input. Please enter a numeric ID (e.g., 1234567890).",
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)

async def receive_remove_sudo_user_id(event):
    user_id = event.sender_id
    try:
        sudo_id_to_remove = int(event.message.text.strip())
        if sudo_id_to_remove in OWNER_IDS:
            await event.reply(
                "❌ Cannot remove an owner.",
                buttons=[[Button.inline("🔚 Back", b"main_menu")]],
                parse_mode='Markdown'
            )
            user_states.pop(user_id, None)
            return
        if sudo_id_to_remove not in user_roles:
            await event.reply(
                "❌ This user does not have sudo access.",
                buttons=[[Button.inline("🔚 Back", b"main_menu")]],
                parse_mode='Markdown'
            )
            user_states.pop(user_id, None)
            return
        user_states[user_id].update({
            'sudo_id': sudo_id_to_remove,
            'state': 'confirm_remove_sudo',
            'timestamp': user_id
        })
        await event.reply(
            f"⚡ Are you sure you want to remove {sudo_id_to_remove} as {user_roles[sudo_id_to_remove]['role'].replace('-', ' ').title()}?",
            buttons=[
                [Button.inline("Yes", b"confirm_remove_sudo")],
                [Button.inline("Cancel", b"cancel_sudo")]
            ],
            parse_mode='Markdown'
        )
    except ValueError as e:
        await event.reply(
            "❌ Invalid input. Please enter a valid number ID (e.g., 1234567890).",
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)

async def confirm_remove_sudo(event):
    user_id = event.sender_id
    if user_id not in user_states or user_states[user_id]['state'] != 'confirm_remove_sudo':
        await event.reply(
            "❌ No active user remove sudo session. Start again with Sudo Access.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        return

    sudo_id = user_states[user_id].get('sudo_id')
    if sudo_id in user_roles:
        role = user_roles[sudo_id]['role']
        del user_roles[sudo_id]
        save_sudo_users()
        await event.reply(
            f"✅ Successfully removed {sudo_id} as {role.replace('-', ' ').title()}.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        safe_print(f"Removed role '{role}' from user ID {sudo_id}")
    else:
        await event.reply(
            "❌ User no longer has sudo access.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
    user_states.pop(user_id, None)

async def set_sudo_role(event, role):
    user_id = event.sender_id
    if user_id not in user_states or user_states[user_id]['state'] != 'sudo_role':
        await event.reply(
            "❌ No active sudo session. Start again with Sudo Access.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        return

    new_user_id = user_states[user_id].get('new_user_id')
    if new_user_id in user_roles:
        await event.reply(
            f"⚡ User {new_user_id} already has role '{user_roles[new_user_id]['role']}'. Overwriting with new role.",
            parse_mode='Markdown'
        )
    user_roles[new_user_id] = {
        'role': role,
        'promoted_by': user_id,
        'promoted_at': str(datetime.utcnow().isoformat())
    }
    save_sudo_users()
    await event.reply(
        f"✅ User {new_user_id} has been assigned as {role.replace('-', ' ').title()}.\n"
        f"They now have access to {'all commands except promoting co-owners' if role == 'co-owner' else 'owner only commands except promoting others and removing accounts'}",
        buttons=[[Button.inline("🔚 Back", b"main_menu")]],
        parse_mode='Markdown'
    )
    safe_print(f"Assigned role '{role}' to user ID {new_user_id} by {user_id}")
    user_states.pop(user_id, None)

async def cancel_sudo_callback(event):
    user_id = event.sender_id
    user_states.pop(user_id, None)
    await event.reply(
        "✅ Sudo operation canceled successfully.",
        buttons=[[Button.inline("🔚 Back", b"main_menu")]],
        parse_mode='Markdown'
    )

@access_control(allow_admin=True, allow_member_management=False)
async def process_join(event, count: int, link: str) -> str:
    success = 0
    success_list = []
    failed_list = []
    selected = list(user_clients.items())[:count]
    owner = event.sender_id
    event_obj = asyncio.Event()
    cancellation_events[owner] = event_obj

    try:
        for phone, (client, _) in selected:
            if event_obj.is_set():
                await event.reply("❌ Join operation canceled successfully.")
                return "❌ Join operation canceled successfully."
            if await join_channel(client, link, phone):
                success += 1
                success_list.append(f"✅ Account {phone} joined successfully")
                await event.reply(f"✅ Account {phone} joined channel {link} successfully")
                safe_print(f"✅ Successfully joined channel: {link} by account {phone}")
            else:
                failed_list.append(f"❌ Account {phone} failed to join channel")
                await event.reply(f"❌ Account {phone} failed to join channel {link}")
                safe_print(f"Failed to join channel: {link}")
            result = f"📜 Successfully joined {success}/{count} accounts\n"
            if success_list:
                result += "\n".join(success_list) + "\n"
            if failed_list:
                result += "\n".join(failed_list) + "\n"
        return result
    finally:
        cancellation_events.pop(owner, None)

@access_control(allow_admin=True, allow_member_management=False)
async def process_leave(event, count: int, link: str) -> str:
    success = 0
    success_list = []
    failed_list = []
    selected = list(user_clients.items())[:count]
    owner = event.sender_id
    event_obj = asyncio.Event()
    cancellation_events[owner] = event_obj

    try:
        for phone, (client, _) in selected:
            if event_obj.is_set():
                await event.reply("❌ Operation canceled successfully.")
                return "❌ Operation canceled successfully."
            try:
                if not await ensure_connected(client, phone):
                    failed_list.append(f"❌ Account {phone} failed to connect")
                    await event.reply(f"❌ Account {phone} failed to connect for leaving {link}")
                    continue
                await respect_flood_limit(phone)
                entity = await client.get_entity(link)
                await client(LeaveChannelRequest(entity))
                success += 1
                success_list.append(f"✅ Account {phone} left channel successfully")
                await event.reply(f"✅ Account {phone} left channel {link} successfully")
                safe_print(f"✅ Successfully left channel: {link} with {phone}")
                await respect_flood_limit(phone)
            except FloodWaitError as e:
                flood_wait_until[phone] = asyncio.get_event_loop().time() + e.seconds
                failed_list.append(f"❌ Account {phone} failed—flood wait for {e.seconds}s")
                await event.reply(f"❌ Account {phone} failed to leave channel {link}—flood wait {e.seconds}s")
                safe_print(f"Flood wait for {phone} on {link}: {e.seconds}s")
            except Exception as e:
                failed_list.append(f"❌ Account {phone} failed to leave channel")
                await event.reply(f"❌ Account {phone} failed to leave channel {link}")
                safe_print(f"Failed to leave channel {link} with {phone}: {e}")
        result = f"📤 Successfully removed {success}/{count} accounts from channels\n"
        if success_list:
            result += "\n".join(success_list) + "\n"
        if failed_list:
            result += "\n".join(failed_list) + "\n"
        return result
    finally:
        cancellation_events.pop(owner, None)

async def process_react(event, count: int, link: str) -> str:
    success = 0
    success_list = []
    failed_list = []
    selected = list(user_clients.items())[:count]
    owner = event.sender_id
    event_obj = asyncio.Event()
    cancellation_events[owner] = event_obj

    if link not in reaction_history:
        reaction_history[link] = {}

    try:
        for phone, (client, _) in selected:
            if event_obj.is_set():
                await event.reply("❌ React operation canceled successfully.")
                return "❌ React operation canceled successfully."
                
            if phone not in reaction_history[link]:
                reaction_history[link][phone] = set()
                
            # Get unused emojis for this account
            unused_emojis = [e for e in EMOJIS if e not in reaction_history[link][phone]]
            
            # If all emojis used, reset history for this account
            if not unused_emojis:
                reaction_history[link][phone].clear()
                unused_emojis = EMOJIS.copy()
            
            # Select random emoji from unused ones
            selected_emoji = random.choice(unused_emojis)
            
            emoji_used = await send_reaction(client, link, phone, emoji=selected_emoji)
            if emoji_used:
                success += 1
                success_list.append(f"✅ Account {phone} reacted with {emoji_used}")
                await event.reply(f"✅ Account {phone} reacted to {link} with {emoji_used}")
                safe_print(f"✅ Account {phone} successfully reacted with {emoji_used} to {link}")
                reaction_history[link][phone].add(emoji_used)
            else:
                failed_list.append(f"❌ Account {phone} failed to react to message")
                await event.reply(f"❌ Account {phone} failed to react to message {link}")
            await respect_flood_limit(phone, REACT_DELAY)
            
        result = f"❤️ Successfully sent {success}/{count} reactions\n"
        if success_list:
            result += "\n".join(success_list) + "\n"
        if failed_list:
            result += "\n".join(failed_list) + "\n"
        return result
    finally:
        cancellation_events.pop(owner, None)

async def process_view(event, count: int, link: str) -> str:
    success = 0
    success_list = []
    failed_list = []
    selected = list(user_clients.items())[:count]
    owner = event.sender_id
    event_obj = asyncio.Event()
    cancellation_events[owner] = event_obj

    try:
        for phone, (client, _) in selected:
            if event_obj.is_set():
                await event.reply("❌ View operation canceled successfully.")
                return "❌ View operation canceled successfully."
            try:
                if await send_view(client, link, phone):
                    success += 1
                    success_list.append(f"✅ Account {phone} sent view successfully")
                    await event.reply(f"✅ Account {phone} sent view to {link} successfully")
                    safe_print(f"✅ Successfully sent view to {link} with {phone}")
                else:
                    failed_list.append(f"❌ Account {phone} failed to send view")
                    await event.reply(f"❌ Account {phone} failed to send view to {link}")
                await respect_flood_limit(phone, VIEW_DELAY)
            except Exception as e:
                failed_list.append(f"❌ Account {phone} failed—{str(e)}")
                await event.reply(f"❌ Account {phone} failed to send view to {link}")
                safe_print(f"❌ Failed to send view to {link} with {phone}: {e}")
        result = f"👀 Successfully sent {success}/{count} views\n"
        if success_list:
            result += "\n".join(success_list) + "\n"
        if failed_list:
            result += "\n".join(failed_list) + "\n"
        return result
    finally:
        cancellation_events.pop(owner, None)

async def addaccount(event):
    user_id = event.sender_id
    if user_id in user_states:
        safe_print(f"Cleaning up previous state for user {user_id} before starting /addaccount")
        user_states.pop(user_id, None)
    
    user_states[user_id] = {
        'command': 'addaccount',
        'state': 'phone',
        'timestamp': asyncio.get_event_loop().time()
    }
    try:
        await event.reply(
            "📱 Please enter the phone number to add (e.g., +1234567890):",
            buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
            parse_mode='Markdown'
        )
        if not has_sudo_access(user_id):
            await event.answer("Please add an account to proceed.", alert=True)
    except Exception as e:
        safe_print(f"Failed to start /addaccount for {user_id}: {e}")

async def receive_phone(event):
    user_id = event.sender_id
    if user_id not in user_states or user_states[user_id]['state'] != 'phone':
        await event.reply("❌ No valid /addaccount session active. Use /addaccount to start.", buttons=[[Button.inline("🔚 Back", b"main_menu")]], parse_mode='Markdown')
        return

    raw_phone = event.message.text.strip()
    phone = normalize_phone(raw_phone)
    safe_print(f"Received phone number input: raw='{raw_phone}', normalized='{phone}'")

    if not re.match(r'^\+\d{7,15}$', phone):
        await event.reply(
            "❌ Invalid phone number format. It must start with '+' and contain 7-15 digits only (e.g., +1234567890). Try again or press Cancel.\n",
            buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
            parse_mode='Markdown'
        )
        safe_print(f"Phone validation failed: {phone}")
        return
    
    if phone in user_clients:
        await event.reply(
            f"❌ Account {phone} is already added.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)
        safe_print(f"Duplicate account detected: {phone}")
        return

    session_file = SESSIONS_DIR / f"{phone}.session"
    session = StringSession()
    client = TelegramClient(session, API_ID, API_HASH)
    
    try:
        if not await ensure_connected(client, phone):
            await event.reply(
                "❌ Failed to connect to Telegram. Try again or press Cancel.",
                buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
                parse_mode='Markdown'
            )
            user_states.pop(user_id, None)
            return
        safe_print(f"Attempting to send OTP to {phone}")
        sent_code = await client.send_code_request(phone)
        user_states[user_id].update({
            'phone': phone,
            'client': client,
            'session_file': session_file,
            'session_string': session.save(),
            'phone_code_hash': sent_code.phone_code_hash,
            'state': 'otp',
            'timestamp': asyncio.get_event_loop().time()
        })
        await event.reply(
            f"📩 OTP sent to {phone}! Please send the OTP code:",
            buttons=[
                [Button.inline("Resend OTP", b"resend_otp")],
                [Button.inline("Cancel", b"cancel_addaccount")]
            ],
            parse_mode='Markdown'
        )
        safe_print(f"OTP sent successfully to {phone}")
    except FloodWaitError as e:
        flood_wait_until[phone] = asyncio.get_event_loop().time() + e.seconds
        safe_print(f"❌ Flood wait for {phone}: {e.seconds}s")
        await event.reply(
            f"⏖ Too many requests. Please try again after {e.seconds} seconds.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        await client.disconnect()
        user_states.pop(user_id, None)
    except Exception as e:
        safe_print(f"❌ Error sending OTP to {phone}: {e}")
        await event.reply(
            f"❌ Failed to send OTP: {str(e)}. Try again or press Cancel.",
            buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
            parse_mode='Markdown'
        )
        await client.disconnect()
        user_states.pop(user_id, None)

async def resend_otp_callback(event):
    user_id = event.sender_id
    state = user_states.get(user_id, {})
    phone = state.get('phone')
    client = state.get('client')
    
    if not phone or not client or state.get('state') not in ['otp', 'password']:
        await event.reply(
            "❌ No active OTP session. Start again with /addaccount.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)
        return
    
    try:
        safe_print(f"Resending OTP to {phone}")
        sent_code = await client.send_code_request(phone)
        user_states[user_id].update({
            'phone_code_hash': sent_code.phone_code_hash,
            'timestamp': asyncio.get_event_loop().time()
        })
        await event.reply(
            f"🔱 OTP resent to {phone}. Please send the OTP code:",
            buttons=[
                [Button.inline("Resend OTP", b"resend_otp")],
                [Button.inline("Cancel", b"cancel_addaccount")]
            ],
            parse_mode='Markdown'
        )
        safe_print(f"✅ Successfully resent OTP to {phone}")
    except FloodWaitError as e:
        flood_wait_until[phone] = asyncio.get_event_loop().time() + e.seconds
        safe_print(f"❌ Flood wait for {phone} for {e.seconds} seconds")
        await event.reply(
            f"❌ Too many requests. Retry after {e.seconds} seconds.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)
    except Exception as e:
        safe_print(f"❌ Error resending OTP to {phone}: {e}")
        await event.reply(
            f"❌ Failed to resend OTP: {str(e)}. Try again or press Cancel.",
            buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
            parse_mode='Markdown'
        )
        user_states.pop(user_id, None)

async def receive_code(event):
    user_id = event.sender_id
    if user_id not in user_states or user_states[user_id]['state'] != 'otp':
        await event.reply("❌ No active OTP session. Start again with /addaccount.", buttons=[[Button.inline("🔚 Back", b"main_menu")]], parse_mode='Markdown')
        return

    code = event.message.text.strip()
    if not code.isdigit():
        await event.reply(
            "❌ Invalid OTP. Please send a numeric code.",
            buttons=[
                [Button.inline("Resend OTP", b"resend_otp")],
                [Button.inline("Cancel", b"cancel_addaccount")]
            ],
            parse_mode='Markdown'
        )
        safe_print(f"Invalid OTP received: {code}")
        return

    client = user_states[user_id]['client']
    phone = user_states[user_id]['phone']
    session_file = user_states[user_id]['session_file']
    session_string = user_states[user_id]['session_string']
    phone_code_hash = user_states[user_id]['phone_code_hash']
    
    try:
        safe_print(f"Attempting sign-in for {phone} with OTP {code}")
        await client.sign_in(phone, code, phone_code_hash=phone_code_hash)
        # Save the session string to file
        with open(session_file, 'w') as f:
            f.write(session_string)
        user_clients[phone] = (client, session_string)
        await event.reply(
            f"✅ Account {phone} added successfully!",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        safe_print(f"Successfully added account {phone} and saved session to {session_file}")
        user_states.pop(user_id, None)
    except SessionPasswordNeededError:
        safe_print(f"Account {phone} requires 2FA")
        user_states[user_id]['state'] = 'password'
        user_states[user_id]['timestamp'] = asyncio.get_event_loop().time()
        await event.reply(
            "🔒 This account has 2FA enabled. Please send your password:",
            buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
            parse_mode='Markdown'
        )
    except Exception as e:
        safe_print(f"❌ Error signing in {phone}: {e}")
        await event.reply(
            f"❌ Invalid OTP or error: {str(e)}. Try again or press Cancel.",
            buttons=[
                [Button.inline("Resend OTP", b"resend_otp")],
                [Button.inline("Cancel", b"cancel_addaccount")]
            ],
            parse_mode='Markdown'
        )

async def receive_password(event):
    user_id = event.sender_id
    if user_id not in user_states or user_states[user_id]['state'] != 'password':
        return
    password = event.message.text.strip()
    client = user_states[user_id]['client']
    phone = user_states[user_id]['phone']
    session_file = user_states[user_id]['session_file']
    session_string = user_states[user_id]['session_string']
    
    try:
        safe_print(f"Attempting 2FA sign-in for {phone}")
        await client.sign_in(password=password)
        # Save the session string to file
        with open(session_file, 'w') as f:
            f.write(session_string)
        user_clients[phone] = (client, session_string)
        await event.reply(
            f"✅ Account {phone} added with 2FA! Session saved to {session_file}",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
        safe_print(f"Successfully added account {phone} with 2FA and saved session to {session_file}")
        user_states.pop(user_id, None)
    except Exception as e:
        safe_print(f"Error with 2FA for {phone}: {e}")
        await event.reply(
            f"❌ Invalid password or error: {str(e)}. Try again or press Cancel.",
            buttons=[[Button.inline("Cancel", b"cancel_addaccount")]],
            parse_mode='Markdown'
        )

async def cancel_addaccount_callback(event):
    user_id = event.sender_id
    if user_id in user_states:
        phone = user_states[user_id].get('phone')
        client = user_states[user_id].get('client')
        if client:
            try:
                await client.disconnect()
                safe_print(f"Disconnected client for {phone} during cancellation")
            except Exception as e:
                safe_print(f"Error disconnecting client {phone}: {e}")
        user_states.pop(user_id, None)
        await event.reply(
            "✅ Add account canceled successfully.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )
    else:
        await event.reply(
            "❌ No active /addaccount session to cancel.",
            buttons=[[Button.inline("🔚 Back", b"main_menu")]],
            parse_mode='Markdown'
        )

@access_control(allow_admin=True)
async def listaccounts(event):
    if not user_clients:
        await event.reply("📭 No accounts added.", buttons=[[Button.inline("🔚 Back", b"main_menu")]], parse_mode='Markdown')
        return
    
    accounts = "\n".join(f"{i+1}. {phone}" for i, phone in enumerate(user_clients.keys()))
    await event.reply(
        f"📋 Active accounts ({len(user_clients)}):\n{accounts}",
        buttons=[[Button.inline("🔚 Back", b"main_menu")]],
        parse_mode='Markdown'
    )

@access_control(allow_admin=True)
async def info(event):
    if not user_clients:
        await event.reply("📭 No accounts added.", buttons=[[Button.inline("🔚 Back", b"main_menu")]], parse_mode='Markdown')
        return
    
    info_lines = []
    for i, (phone, (client, _)) in enumerate(user_clients.items(), 1):
        try:
            if not await ensure_connected(client, phone):
                info_lines.append(f"{i}. {phone} ❌ - Not connected")
                continue
            me = await client.get_me()
            status = "✅" if client.is_connected() else "❌"
            identifier = f"@{me.username}" if me.username else f"ID: {me.id}"
            info_lines.append(f"{i}. {phone} {status} - {identifier}")
        except Exception as e:
            info_lines.append(f"{i}. {phone} ❌ - Error: {str(e)}")
    
    await event.reply(
        "📋 Active accounts info:\n\n" + "\n".join(info_lines),
        buttons=[[Button.inline("🔚 Back", b"main_menu")]],
        parse_mode='Markdown'
    )

@access_control(allow_admin=True)
async def stop(event):
    owner = event.sender_id
    event_obj = cancellation_events.get(owner)
    if event_obj:
        event_obj.set()
        await event.reply("✅ Operation canceled.", buttons=[[Button.inline("🔚 Back", b"main_menu")]], parse_mode='Markdown')
    else:
        await event.reply("❌ No ongoing operation to cancel.", buttons=[[Button.inline("🔚 Back", b"main_menu")]], parse_mode='Markdown')
    user_states.pop(event.sender_id, None)

async def cleanup_stale_states():
    while True:
        current_time = asyncio.get_event_loop().time()
        for user_id, state in list(user_states.items()):
            if current_time - state['timestamp'] > STATE_TIMEOUT:
                safe_print(f"Cleaning up stale state for user {user_id}")
                if state.get('client'):
                    try:
                        await state['client'].disconnect()
                        safe_print(f"Disconnected stale client for {state.get('phone', 'unknown')}")
                    except Exception as e:
                        safe_print(f"Error disconnecting stale client: {e}")
                user_states.pop(user_id, None)
        await asyncio.sleep(60)

async def load_sessions(client):
    safe_print("Loading existing sessions...")
    corrupted_files = []
    
    for session_file in SESSIONS_DIR.glob("*.session"):
        phone = session_file.stem
        try:
            with open(session_file, 'r') as f:
                session_string = f.read().strip()
            if not session_string:
                safe_print(f"Invalid session file {session_file}: empty or corrupted")
                corrupted_files.append(session_file)
                continue
            user_client = TelegramClient(StringSession(session_string), API_ID, API_HASH)
            await user_client.connect()
            
            if await user_client.is_user_authorized():
                user_clients[phone] = (user_client, session_string)
                safe_print(f"✅ Loaded session for {phone}")
            else:
                await user_client.disconnect()
                safe_print(f"❌ Session {phone} is not authorized")
                corrupted_files.append(session_file)
        except Exception as e:
            safe_print(f"❌ Failed to load session {session_file}: {e}")
            corrupted_files.append(session_file)
    
    for session_file in corrupted_files:
        try:
            session_file.unlink()
            safe_print(f"Removed corrupted session file: {session_file}")
        except Exception as e:
            safe_print(f"Failed to delete {session_file}: {e}")

    if corrupted_files:
        for owner_id in OWNER_IDS:
            try:
                await client.send_message(
                    owner_id,
                    f"⚠️ Removed {len(corrupted_files)} corrupted session files. Please re-add affected accounts.",
                    parse_mode='Markdown'
                )
            except Exception as e:
                safe_print(f"Failed to notify owner {owner_id}: {e}")

async def join_channel(client, invite_link, phone):
    try:
        if not await ensure_connected(client, phone):
            safe_print(f"❌ Failed to connect client {phone} for joining {invite_link}")
            return False
        await respect_flood_limit(phone)

        if invite_link.startswith('https://t.me/+'):
            # Private channel logic remains the same
            invite_hash = invite_link.split('+')[1]
            if not invite_hash:
                safe_print(f"❌ Invalid private invite link {invite_link}: No hash found")
                return False
            try:
                await client(ImportChatInviteRequest(invite_hash))
                safe_print(f"✅ Account {phone} joined private channel via invite link: {invite_link}")
                await respect_flood_limit(phone, JOIN_DELAY)
                return True
            except (InviteHashExpiredError, InviteHashInvalidError, ChannelsTooMuchError) as e:
                safe_print(f"❌ Error joining private channel {invite_link} with {phone}: {str(e)}")
                return False
            except UserAlreadyParticipantError:
                safe_print(f"⚠️ Account {phone} is already a participant in {invite_link}")
                return True
        else:
            # Modified public channel logic
            try:
                channel_username = invite_link.split('/')[-1]
                if not channel_username:
                    safe_print(f"❌ Invalid public channel link: {invite_link}")
                    return False
                
                try:
                    channel = await client.get_entity(channel_username)
                except ValueError:
                    channel = await client.get_entity(f"@{channel_username}")
                
                await client(JoinChannelRequest(channel))
                safe_print(f"✅ Account {phone} joined public channel: {invite_link}")
                await respect_flood_limit(phone, JOIN_DELAY)
                return True
            except UserAlreadyParticipantError:
                safe_print(f"⚠️ Account {phone} is already a participant in {invite_link}")
                return True
            except Exception as e:
                safe_print(f"❌ Error joining public channel {invite_link} with {phone}: {str(e)}")
                return False

    except FloodWaitError as e:
        flood_wait_until[phone] = asyncio.get_event_loop().time() + e.seconds
        safe_print(f"❌ Flood wait for {phone}: {e.seconds}s")
        return False
    except Exception as e:
        safe_print(f"❌ Join failed for {invite_link} by account {phone}: {e}")
        return False

async def send_reaction(client, message_link: str, phone, emoji=None, retries=3):
    chat_identifier, message_id = extract_message_info(message_link)
    if not chat_identifier or not message_id:
        safe_print(f"❌ Invalid message link: {message_link}")
        return None

    try:
        if not await ensure_connected(client, phone):
            safe_print(f"❌ Client for {phone} not connected for reaction to {message_link}")
            return None
        await respect_flood_limit(phone)

        try:
            # Modified entity resolution
            if chat_identifier.startswith('-100'):
                chat_id = int(chat_identifier)
                entity = await client.get_entity(chat_id)
            else:
                try:
                    entity = await client.get_entity(chat_identifier)
                except ValueError:
                    entity = await client.get_entity(f"@{chat_identifier}")
            
            chat = InputPeerChannel(channel_id=entity.id, access_hash=entity.access_hash)
        except Exception as e:
            safe_print(f"❌ Error resolving entity {chat_identifier} for {phone}: {e}")
            return None

        for attempt in range(retries):
            try:
                await client(SendReactionRequest(
                    peer=chat,
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=emoji)]
                ))
                safe_print(f"✅ Account {phone} reacted with {emoji} to {message_link}")
                return emoji
            except UserNotParticipantError:
                safe_print(f"❌ Account {phone} not in chat {chat_identifier}—joining first")
                try:
                    await join_channel(client, f"https://t.me/{chat_identifier}", phone)
                    continue
                except Exception as e:
                    safe_print(f"❌ Failed to join channel for reaction: {e}")
                    return None
            except (ReactionInvalidError, FloodWaitError) as e:
                if isinstance(e, FloodWaitError):
                    wait_time = e.seconds + random.uniform(0, 2)
                    flood_wait_until[phone] = asyncio.get_event_loop().time() + wait_time
                    await asyncio.sleep(wait_time)
                if attempt == retries - 1:
                    return None
            except Exception as e:
                safe_print(f"❌ Attempt {attempt + 1}/{retries} failed for {phone}: {e}")
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(2)
        return None
    except Exception as e:
        safe_print(f"❌ Unexpected error sending reaction: {str(e)}")
        return None

        for attempt in range(retries):
            try:
                await client(SendReactionRequest(
                    peer=chat,
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=emoji)]
                ))
                safe_print(f"✅ Account {phone} reacted with {emoji} to {message_link}")
                return emoji
            except UserNotParticipantError:
                safe_print(f"❌ Account {phone} not in chat {chat_identifier}—use /join first")
                return None
            except ReactionInvalidError:
                safe_print(f"❌ Reaction {emoji} invalid for {message_link} by {phone}")
                return None
            except FloodWaitError as e:
                wait_time = e.seconds + random.uniform(0, 2)
                flood_wait_until[phone] = asyncio.get_event_loop().time() + wait_time
                safe_print(f"❌ Flood wait for {wait_time}s on attempt {attempt + 1}/{retries} for {phone}")
                await asyncio.sleep(wait_time)
                if attempt == retries - 1:
                    safe_print(f"❌ Max retries reached for {phone} due to flood wait")
                    return None
            except Exception as e:
                safe_print(f"❌ Attempt {attempt + 1}/{retries} failed for {phone} on {message_link}: {e}")
                if attempt == retries - 1:
                    return None
                await asyncio.sleep(2)  # Small delay between retries
        safe_print(f"❌ Account {phone} failed to send reaction {emoji} to {message_link} after {retries} attempts")
        return None
    except Exception as e:
        safe_print(f"❌ Unexpected error sending reaction to {message_link} for {phone}: {str(e)}")
        return None

async def send_view(client, message_link: str, phone):
    chat_identifier, message_id = extract_message_info(message_link)
    if not chat_identifier or not message_id:
        safe_print(f"❌ Invalid message link: {message_link}")
        return False

    try:
        if not await ensure_connected(client, phone):
            return False
        await respect_flood_limit(phone)

        try:
            if chat_identifier.startswith('-100'):
                chat_id = int(chat_identifier)
                chat = await client.get_entity(chat_id)
            else:
                chat = await client.get_entity(f"@{chat_identifier}" if not chat_identifier.startswith('@') else chat_identifier)
            chat = InputPeerChannel(channel_id=chat.id, access_hash=chat.access_hash)
        except ValueError as e:
            safe_print(f"❌ Failed to resolve chat {chat_identifier}: {e}")
            return False

        await client(GetMessagesViewsRequest(
            peer=chat,
            id=[message_id],
            increment=True
        ))
        safe_print(f"✅ Account {phone} sent view to {message_link}")
        return True
    except FloodWaitError as e:
        flood_wait_until[phone] = asyncio.get_event_loop().time() + e.seconds
        safe_print(f"❌ Flood wait for {phone}: {e.seconds} seconds")
        return False
    except Exception as e:
        safe_print(f"❌ View failed for {phone}: {str(e)}")
        return False

async def main():
    safe_print("🚖 Starting bot...")
    
    bot_client = TelegramClient(StringSession(), API_ID, API_HASH)
    try:
        await bot_client.start(bot_token=BOT_TOKEN)
        safe_print("✅ Bot client started successfully")
    except Exception as e:
        safe_print(f"❌ Failed to start bot client: {e}")
        for owner_id in OWNER_IDS:
            try:
                await bot_client.send_message(
                    owner_id,
                    f"❌ Bot failed to start: {str(e)}.",
                    parse_mode='Markdown'
                )
            except Exception as notify_e:
                safe_print(f"❌ Failed to notify owner {owner_id} of startup failure: {notify_e}")
        return

    load_sudo_users()
    load_known_users()
    await load_sessions(bot_client)

    # Send pop-up startup message with Start button to all known users
    popup_message = "🌟 **Telegram Meta Bot is now running!**\nClick below to start."
    for user_id in known_users:
        try:
            await bot_client.send_message(
                user_id,
                popup_message,
                buttons=[[Button.inline("🚀 Start", b"main_menu")]],
                parse_mode='Markdown'
            )
        except Exception as e:
            safe_print(f"❌ Failed to send pop-up startup message to user {user_id}: {e}")

    bot_client.add_event_handler(start_count_link_command, events.NewMessage(pattern='/join'))
    bot_client.add_event_handler(start_count_link_command, events.NewMessage(pattern='/leave'))
    bot_client.add_event_handler(start_count_link_command, events.NewMessage(pattern='/react'))
    bot_client.add_event_handler(start_count_link_command, events.NewMessage(pattern='/view'))
    bot_client.add_event_handler(start_link_only_command, events.NewMessage(pattern='/mega_join'))
    bot_client.add_event_handler(start_link_only_command, events.NewMessage(pattern='/mega_leave'))
    bot_client.add_event_handler(start_link_only_command, events.NewMessage(pattern='/mega_react'))
    bot_client.add_event_handler(start_link_only_command, events.NewMessage(pattern='/mega_view'))
    bot_client.add_event_handler(stop, events.NewMessage(pattern='/cancel'))
    bot_client.add_event_handler(show_main_menu, events.NewMessage(pattern='/start'))
    bot_client.add_event_handler(show_main_menu, events.NewMessage(pattern='/help'))
    bot_client.add_event_handler(show_sudo_list, events.NewMessage(pattern='/listsudo'))
    bot_client.add_event_handler(addaccount, events.NewMessage(pattern='/addaccount'))
    bot_client.add_event_handler(listaccounts, events.NewMessage(pattern='/listaccounts'))
    bot_client.add_event_handler(info, events.NewMessage(pattern='/info'))

    @bot_client.on(events.CallbackQuery)
    async def callback_handler(event):
        user_id = event.sender_id
        data = event.data
        # Add user to known_users
        known_users.add(user_id)
        save_known_users()

        # Restrict access for non-sudo users and admins
        role = user_roles.get(user_id, {}).get('role') if user_id not in OWNER_IDS else 'owner'
        if not has_sudo_access(user_id):
            if data not in [b"addaccount", b"cancel_addaccount", b"resend_otp", b"main_menu"]:
                await event.reply("❌ You are not authorized to access this section.")
                return
        if role == 'admin':
            # Admins can only access Add Account, Reaction Management, View Management, and Help
            allowed_data = [b"addaccount", b"cancel_addaccount", b"resend_otp", b"main_menu",
                           b"reaction_management", b"view_management", b"help",
                           b"mega_react", b"react", b"mega_view", b"view"]
            if data not in allowed_data:
                await event.reply("❌ Admins are not authorized to access this section.")
                return

        if data == b"main_menu":
            await show_main_menu(event)
        elif data == b"account_management":
            await show_account_management(event)
        elif data == b"member_management":
            await show_member_management(event)
        elif data == b"reaction_management":
            await show_reaction_management(event)
        elif data == b"view_management":
            await show_view_management(event)
        elif data == b"help":
            await show_help_message(event)
        elif data == b"addaccount":
            await addaccount(event)
        elif data == b"listaccounts":
            await listaccounts(event)
        elif data == b"info":
            await info(event)
        elif data == b"removeaccount":
            if user_id not in OWNER_IDS:
                await event.reply("❌ Only owners can remove accounts.")
                return
            await start_remove_phone_command(event)
        elif data in [b"join", b"leave", b"react", b"view"]:
            await start_count_link_command(event, data.decode())
        elif data in [b"mega_join", b"mega_leave", b"mega_react", b"mega_view"]:
            await start_link_only_command(event, data.decode())
        elif data == b"resend_otp":
            await resend_otp_callback(event)
        elif data == b"cancel_addaccount":
            await cancel_addaccount_callback(event)
        elif data == b"sudo_access":
            role = user_roles.get(user_id, {}).get('role')
            if user_id not in OWNER_IDS and role != 'co-owner':
                await event.reply("❌ Only Owners and Co-Owners can manage sudo access.")
                return
            await event.reply(
                "🔑 **Sudo Access Management**\n\nChoose an action:",
                buttons=[
                    [Button.inline("➕ Add Sudo User", b"add_sudo_user")],
                    [Button.inline("➖ Remove Sudo User", b"remove_sudo_user")],
                    [Button.inline("📝 List Sudo Users", b"list_sudo_users")],
                    [Button.inline("🔚 Back", b"main_menu")]
                ],
                parse_mode='Markdown'
            )
        elif data == b"add_sudo_user":
            role = user_roles.get(user_id, {}).get('role')
            if user_id not in OWNER_IDS and role != 'co-owner':
                await event.reply("❌ Only Owners and Co-Owners can add sudo users.")
                return
            user_states[user_id] = {
                'command': 'add_sudo_user',
                'state': 'sudo_user_id',
                'timestamp': asyncio.get_event_loop().time()
            }
            await event.reply(
                "🔑 Enter the Telegram user ID to grant sudo access to (e.g., 123456789):",
                parse_mode='Markdown'
            )
        elif data == b"remove_sudo_user":
            role = user_roles.get(user_id, {}).get('role')
            if user_id not in OWNER_IDS and role != 'co-owner':
                await event.reply("❌ Only Owners and Co-Owners can remove sudo users.")
                return
            user_states[user_id] = {
                'command': 'remove_sudo_user',
                'state': 'remove_sudo_user_id',
                'timestamp': asyncio.get_event_loop().time()
            }
            await event.reply(
                "🔑 Enter the Telegram user ID to remove sudo access from (e.g., 123456789):",
                parse_mode='Markdown'
            )
        elif data == b"list_sudo_users":
            await show_sudo_list(event)
        elif data == b"set_co_owner":
            if user_id not in OWNER_IDS:
                await event.reply("❌ Only owners can promote co-owners.")
                return
            await set_sudo_role(event, 'co-owner')
        elif data == b"set_admin":
            await set_sudo_role(event, 'admin')
        elif data == b"confirm_remove_sudo":
            await confirm_remove_sudo(event)
        elif data == b"cancel_sudo":
            await cancel_sudo_callback(event)

    @bot_client.on(events.NewMessage)
    async def message_handler(event):
        user_id = event.sender_id
        # Add user to known_users
        known_users.add(user_id)
        save_known_users()

        if user_id not in user_states:
            return

        if re.match(r'http://example\.com/', event.message.text):
            return
            
        state = user_states[user_id].get('state')
        if state == 'phone':
            await receive_phone(event)
        elif state == 'otp':
            await receive_code(event)
        elif state == 'password':
            await receive_password(event)
        elif state == 'counting':
            await receive_number(event)
        elif state == 'invite_link':
            await receive_invite_link(event)
        elif state == 'message_link':
            await receive_msg_link(event)
        elif state == 'remove_phone':
            await receive_remove_phone(event)
        elif state == 'sudo_user_id':
            await receive_sudo_user_id(event)
        elif state == 'remove_sudo_user_id':
            await receive_remove_sudo_user_id(event)

    try:
        await bot_client.run_until_disconnected()
    except Exception as e:
        safe_print(f"❌ Bot error: {e}")
        for owner_id in OWNER_IDS:
            try:
                await bot_client.send_message(
                    owner_id,
                    f"❌ Bot encountered an error: {e}",
                    parse_mode='Markdown'
                )
            except Exception as notify_e:
                safe_print(f"❌ Failed to notify owner {owner_id} of error: {notify_e}")
    finally:
        await bot_client.disconnect()
        for phone, (client, _) in list(user_clients.items()):
            try:
                await client.disconnect()
                safe_print(f"Disconnected client {phone}")
            except Exception as e:
                safe_print(f"Error disconnecting client {phone}: {e}")

if __name__ == "__main__":
    asyncio.run(main())  
