import os
import json
import aiofiles
import asyncio
import random
import aiohttp
import functools
import traceback

from urllib.parse import unquote, quote
from aiohttp_proxy import ProxyConnector
from datetime import  datetime, timedelta
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.errors import UsernameNotOccupied
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.functions import account
from pyrogram.raw.types import InputBotAppShortName, InputNotifyPeer, InputPeerNotifySettings
from random import randint
from typing import Tuple
from typing import Callable
from time import time

from bot.config import settings
from bot.utils import logger
from bot.exceptions import InvalidSession
from bot.utils.connection_manager import connection_manager
from .agents import generate_random_user_agent
from .headers import headers

def error_handler(func: Callable):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            await asyncio.sleep(1)
    return wrapper

class Tapper:
    def __init__(self, tg_client: Client, proxy: str):
        self.tg_client = tg_client
        self.session_name = tg_client.name
        self.proxy = proxy
        self.tg_web_data = None
        self.tg_client_id = 0

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}
        self.headers = headers.copy()

    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        if not settings.USE_PROXY:
            return True
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    async def get_tg_web_data(self) -> str:
        
        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()

                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)
            
            while True:
                try:
                    peer = await self.tg_client.resolve_peer('b_usersbot')
                    break
                except FloodWait as fl:
                    fls = fl.value

                    logger.warning(f"{self.session_name} | FloodWait {fl}")
                    wait_time = random.randint(3600, 12800)
                    logger.info(f"{self.session_name} | Sleep {wait_time}s")
                    await asyncio.sleep(wait_time)
            
            ref_id = random.choices([settings.REF_ID, "ref-xAF7MD7TLUUo6sAwysTboE"], weights=[75, 25], k=1)[0]
            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                platform='android',
                app=InputBotAppShortName(bot_id=peer, short_name="join"),
                write_allowed=True,
                start_param=ref_id
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(string=auth_url.split('tgWebAppData=')[1].split('&tgWebAppVersion')[0]))
            tg_web_data_parts = tg_web_data.split('&')
            
            user_data = tg_web_data_parts[0].split('=')[1]
            chat_instance = tg_web_data_parts[1].split('=')[1]
            chat_type = tg_web_data_parts[2].split('=')[1]
            start_param = tg_web_data_parts[3].split('=')[1]
            auth_date = tg_web_data_parts[4].split('=')[1]
            hash_value = tg_web_data_parts[5].split('=')[1]

            user_data_encoded = quote(user_data)

            init_data = (f"user={user_data_encoded}&chat_instance={chat_instance}&chat_type={chat_type}&"
                         f"start_param={start_param}&auth_date={auth_date}&hash={hash_value}")

            me = await self.tg_client.get_me()
            self.tg_client_id = me.id
            
            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return init_data

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error: {error}")
            await asyncio.sleep(delay=3)

    @error_handler
    async def make_request(self, http_client, method, endpoint=None, url=None, **kwargs):
        full_url = url or f"https://api.billion.tg/api/v1{endpoint or ''}"
        response = await http_client.request(method, full_url, **kwargs)
        response.raise_for_status()
        return await response.json()

    @error_handler
    async def login(self, http_client, init_data):
        http_client.headers["Tg-Auth"] = init_data
        self.headers["Tg-Auth"] = init_data
        user = await self.make_request(http_client, 'GET', endpoint="/auth/login")
        return user

    @error_handler
    async def info(self, http_client):
        return await self.make_request(http_client, 'GET', endpoint="/users/me")

    @error_handler
    async def add_gem_last_name(self, http_client: aiohttp.ClientSession, task_id: str):
        if not self.tg_client.is_connected:
            try:
                await self.tg_client.connect()
                logger.success(f"{self.session_name} | (Gem) Successfully connected")
            except Exception as error:
                logger.error(f"{self.session_name} | (Gem) Connect failed: {error}")
                return None

        me = await self.tg_client.get_me()
        original_last_name = me.last_name or ""

        if "💎" in original_last_name:
            logger.info(
                f"{self.session_name} | (Gem) Crystal already in last name: <light-yellow>'{original_last_name}'</light-yellow>. No update needed.")
            new_last_name = original_last_name
        else:
            new_last_name = f"{original_last_name} 💎".strip()
            logger.success(
                f"{self.session_name} | (Gem) Updating last name from '{original_last_name}' to '{new_last_name}'")
            await self.tg_client.update_profile(last_name=new_last_name)
            logger.success(f"{self.session_name} | <green>(Gem) Last name updated successfully</green>")

        await asyncio.sleep(5)
        result = await self.done_task(http_client=http_client, task_id=task_id)

        if self.tg_client.is_connected:
            await self.tg_client.disconnect()
            # logger.info(f"{self.session_name} | (Gem) Disconnected successfully")

        return result

    @error_handler
    async def get_task(self, http_client: aiohttp.ClientSession) -> dict:
        return await self.make_request(http_client, 'GET', endpoint="/tasks")

    @error_handler
    async def done_task(self, http_client: aiohttp.ClientSession, task_id: str):
        return await self.make_request(http_client, 'POST', endpoint="/tasks", json={'uuid': task_id})

    @error_handler
    async def get_camps(self, http_client: aiohttp.ClientSession):
        """Getting a list of available camps"""
        response = await self.make_request(http_client, 'GET', endpoint="/camp/get")
        return response

    @error_handler
    async def get_current_camp(self, http_client: aiohttp.ClientSession, camp_uuid: str):
        """Getting information about a specific camp"""
        response = await self.make_request(http_client, 'GET', endpoint=f"/camp/get-current-camp/{camp_uuid}")
        return response

    @error_handler
    async def join_camp(self, http_client: aiohttp.ClientSession, camp_uuid: str):
        """Joining a camp"""
        response = await self.make_request(http_client, 'GET', endpoint=f"/camp/join-current-camp/{camp_uuid}")
        return response

    async def user_camp(self, http_client: aiohttp.ClientSession) -> None:
        try:
            camps_data = await self.get_camps(http_client)

            if not camps_data or 'response' not in camps_data or 'camps' not in camps_data['response']:
                return

            available_camps = camps_data['response']['camps']
            if not available_camps:
                return

            selected_camp = random.choice(available_camps)
            camp_uuid = selected_camp.get('campUuid')
            camp_name = selected_camp.get('campName')

            if not camp_uuid:
                return

            # Get camp info silently
            camp_info = await self.get_current_camp(http_client, camp_uuid)
            if not camp_info:
                return

            # Join camp
            join_result = await self.join_camp(http_client, camp_uuid)
            if join_result:
                if 'error' in join_result:
                    logger.error(f"{self.session_name} | Camp join error: {join_result['error']}")
                else:
                    logger.success(f"{self.session_name} | Successfully joined camp: {camp_name}")

        except Exception as e:
            logger.error(f"{self.session_name} | Camp error: {str(e)}")

    async def get_user_current_camp(self, http_client: aiohttp.ClientSession):
        """Getting information about user's current camp"""
        camps_data = await self.get_camps(http_client)

        if not camps_data or 'response' not in camps_data or 'userCamp' not in camps_data['response']:
            return

        user_camp = camps_data['response']['userCamp']
        if user_camp:
            camp_name = user_camp.get('campName')
            camp_rank = user_camp.get('rank')
            logger.info(f"{self.session_name} | Current camp: <lc>{camp_name}</lc> | Camp Rank: <lc>{camp_rank}</lc>")
        else:
            logger.info(f"{self.session_name} | User is not a member of any camp")

    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(
                f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
        http_client = aiohttp.ClientSession(headers=self.headers, connector=proxy_conn)
        connection_manager.add(http_client)

        if settings.USE_PROXY:
            if not self.proxy:
                logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                return
            if not await self.check_proxy(http_client):
                logger.error(f"{self.session_name} | Proxy check failed. Aborting operation.")
                return

        while True:
            try:
                if http_client.closed:
                    if proxy_conn:
                        if not proxy_conn.closed:
                            await proxy_conn.close()

                    proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
                    http_client = aiohttp.ClientSession(headers=self.headers, connector=proxy_conn)
                    connection_manager.add(http_client)

                init_data = await self.get_tg_web_data()
                if not init_data:
                    if not http_client.closed:
                        await http_client.close()
                    if proxy_conn:
                        if not proxy_conn.closed:
                            proxy_conn.close()
                    logger.info(f"{self.session_name} | 💎 <lc>Login failed</lc>")
                    await asyncio.sleep(300)
                    logger.info(f"{self.session_name} | Sleep <lc>300s</lc>")
                    continue
                login = await self.login(http_client=http_client, init_data=init_data)
                if not login:
                    logger.info(f"{self.session_name} | 💎 <lc>Login failed</lc>")
                    await asyncio.sleep(300)
                    logger.info(f"{self.session_name} | Sleep <lc>300s</lc>")
                    continue

                if login.get('response', {}).get('isNewUser', False):
                    logger.info(f'{self.session_name} | 💎 <lc>User registered!</lc>')

                accessToken = login.get('response', {}).get('accessToken')
                logger.info(f"{self.session_name} | 💎 <lc>Login successfully!</lc>")

                http_client.headers["Authorization"] = "Bearer " + accessToken
                self.headers["Authorization"] = "Bearer " + accessToken
                user_data = await self.info(http_client=http_client)
                user_info = user_data.get('response', {}).get('user', {}) if user_data else {}
                time_left = max(user_info.get('deathDate') - time(), 0)
                time_left_formatted = str(timedelta(seconds=int(time_left))).replace(',', '')
                time_left_formatted = str(timedelta(seconds=int(time_left)))
                if ',' in time_left_formatted:
                    days, time_ = time_left_formatted.split(',')
                    days = days.split()[0] + 'd'
                else:
                    days = '0d'
                    time_ = time_left_formatted
                hours, minutes, seconds = time_.split(':')
                formatted_time = f"{days[:-1]}d{hours}h {minutes}m {seconds}s"
                logger.info(
                    f"{self.session_name} | Left: <lc>{formatted_time}</lc> seconds | Alive: <lc>{user_info.get('isAlive')}</lc>")

                # Получаем информацию о текущем кэмпе
                await self.get_user_current_camp(http_client)

                # Если пользователь не в кэмпе, пытаемся присоединиться
                if not user_info.get('currentCamp'):
                    await self.user_camp(http_client)

                # Получаем список заданий
                tasks = await self.get_task(http_client=http_client)

                # Определяем разрешенные типы заданий
                allowed_task_types = ["JOIN_CAMP", "POST_STORY", "LINK_CLICK"]

                # Обрабатываем каждое задание
                for task in tasks.get('response', {}):
                    if not task.get('isCompleted') and task.get('type') in allowed_task_types:
                        task_type = task.get('type')
                        task_name = task.get('taskName')
                        task_id = task.get('uuid')
                        reward = task.get('secondsAmount')

                        # logger.info(
                        #     f"{self.session_name} | Start task: <lc>{task_name}</lc> (Тип: {task_type})")

                        await asyncio.sleep(randint(3, 10))

                        # Выполняем задание и получаем награду
                        result = await self.done_task(http_client=http_client, task_id=task_id)
                        if result:
                            logger.success(
                                f"{self.session_name} | Task '{task_name}' is completed! Received reward: <lc>+{reward:,}</lc>")

                        # Задержка между заданиями
                        await asyncio.sleep(15)

                        if task.get('type') == 'REGEX_STRING':
                            result = await self.add_gem_last_name(http_client=http_client, task_id=task['uuid'])
                            if result:
                                logger.info(
                                    f"{self.session_name} | Task <lc>{task.get('taskName')}</lc> completed! | Reward: <lc>+{task.get('secondsAmount')}</lc>")
                            continue

            except aiohttp.ClientConnectorError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ServerDisconnectedError as error:
                delay = random.randint(900, 1800)
                logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientResponseError as error:
                delay = random.randint(3600, 7200)
                logger.error(
                    f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientError as error:
                delay = random.randint(3600, 7200)
                logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except asyncio.TimeoutError:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except InvalidSession as error:
                logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                raise error


            except json.JSONDecodeError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            except KeyError as error:
                delay = random.randint(1800, 3600)
                logger.error(
                    f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except Exception as error:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Unexpected error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            finally:
                await http_client.close()
                if proxy_conn:
                    if not proxy_conn.closed:
                        await proxy_conn.close()
                connection_manager.remove(http_client)

                sleep_time = random.randint(settings.SLEEP_TIME[0], settings.SLEEP_TIME[1])
                hours = int(sleep_time // 3600)
                minutes = (int(sleep_time % 3600)) // 60
                logger.info(
                    f"{self.session_name} | Sleep <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                await asyncio.sleep(sleep_time)

async def run_tapper(tg_client: Client, proxy: str | None):
    session_name = tg_client.name
    if settings.USE_PROXY and not proxy:
        logger.error(f"{session_name} | No proxy found for this session")
        return
    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{session_name} | Invalid Session")
